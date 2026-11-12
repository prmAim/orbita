"""Read-only T-Invest REST gateway and execution matching for issue #6."""

import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

import httpx

BASE = "https://invest-public-api.tbank.ru/rest/tinkoff.public.invest.api.contract.v1."
ACCOUNTS = BASE + "UsersService/GetAccounts"
STREAM = BASE + "OrdersStreamService/TradesStream"
HISTORY = BASE + "OperationsService/GetOperationsByCursor"


class Incomplete(Exception):
    """Only local, non-personal diagnostic messages may be used here."""


def identifier(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise Incomplete("missing_identifier")
    return value


def instant(value: str) -> str:
    # Preserve all nine fractional digits. datetime alone truncates to microseconds.
    match = re.fullmatch(
        r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?(Z|[+-]\d\d:\d\d)",
        value,
    )
    if not match:
        raise Incomplete("invalid_execution_time")
    seconds, fraction, offset = match.groups()
    utc = datetime.fromisoformat(seconds + offset).astimezone(UTC)
    return utc.strftime("%Y-%m-%dT%H:%M:%S.") + (fraction or "").ljust(9, "0") + "Z"


def integer(value: object) -> int:
    if not re.fullmatch(r"-?\d+", str(value)):
        raise Incomplete("invalid_integer")
    return int(str(value))


@dataclass(frozen=True)
class Execution:
    instrument: str
    side: str
    time: str
    pieces: int
    price: Decimal

    @classmethod
    def parse(cls, instrument, side, trade, time_field):
        price = trade["price"]
        if not isinstance(price, dict) or not ({"units", "nano"} & price.keys()):
            raise Incomplete("missing_price")
        units = integer(price.get("units", 0))
        nano = integer(price.get("nano", 0))
        quantity = integer(trade["quantity"])
        if abs(nano) >= 10**9 or units * nano < 0 or quantity <= 0:
            raise Incomplete("invalid_price_or_quantity")
        return cls(
            identifier(instrument),
            side,
            instant(trade[time_field]),
            quantity,
            Decimal(units) + Decimal(nano).scaleb(-9),
        )


class Broker:
    def __init__(self, client: httpx.Client, account: str, log: Callable):
        self.client = client
        self.account = account
        self.log = log

    def verify_account(self):
        self.log("GetAccounts")
        data = self.client.post(ACCOUNTS, json={}).raise_for_status().json()
        accounts = [a for a in data["accounts"] if a["id"] == self.account]
        if len(accounts) != 1 or accounts[0]["status"] != "ACCOUNT_STATUS_OPEN":
            raise Incomplete("source_account_unavailable")
        if accounts[0]["accessLevel"] != "ACCOUNT_ACCESS_LEVEL_READ_ONLY":
            raise Incomplete("read_only_access_unconfirmed")
        self.log("source_account_verified", access="read_only", status="open")

    def observe(self, *, reconnect=False) -> Execution | None:
        """Close this response before opening a new subscription."""
        self.log("TradesStream", reconnect=reconnect)
        body = {"accounts": [self.account], "pingDelayMs": 10000}
        with self.client.stream("POST", STREAM, json=body) as response:
            response.raise_for_status()
            subscribed = False
            for line in response.iter_lines():
                if not line.strip():
                    continue
                frame = json.loads(line)
                if "error" in frame:
                    raise Incomplete("stream_error_frame")
                event = frame.get("result", frame)
                if "subscription" in event:
                    ack = event["subscription"]
                    if ack.get("status") != "RESULT_SUBSCRIPTION_STATUS_OK" or ack.get(
                        "accounts"
                    ) != [self.account]:
                        raise Incomplete("source_subscription_rejected")
                    subscribed = True
                    self.log("subscription_confirmed", reconnect=reconnect)
                    if reconnect:
                        return None
                elif "orderTrades" in event:
                    order = event["orderTrades"]
                    if not subscribed or order["accountId"] != self.account:
                        raise Incomplete("unexpected_stream_source")
                    side = {
                        "ORDER_DIRECTION_BUY": "BUY",
                        "ORDER_DIRECTION_SELL": "SELL",
                    }[order["direction"]]
                    trades = order["trades"]
                    if trades:
                        fills = [
                            Execution.parse(order["instrumentUid"], side, t, "dateTime")
                            for t in trades
                        ]
                        if fills.count(fills[0]) != 1:
                            raise Incomplete("indistinguishable_stream_fills")
                        self.log("execution_received", fills_in_message=len(fills))
                        return fills[0]
                elif "ping" not in event:
                    raise Incomplete("unexpected_stream_frame")
        raise Incomplete("stream_ended_without_evidence")

    def history(self, start: str, end: str, *, instrument="", ticker=""):
        """Read individual executions from every page of a fixed interval."""
        query = {
            "accountId": self.account,
            "from": start,
            "to": end,
            "limit": 100,
            "withoutTrades": False,
        }
        if instrument:
            query["instrumentId"] = instrument
        cursors = set()
        seen = {}
        while True:
            self.log("GetOperationsByCursor", page=len(cursors) + 1)
            page = self.client.post(HISTORY, json=query).raise_for_status().json()
            for op in page.get("items", []):
                if op.get("brokerAccountId") != self.account:
                    raise Incomplete("unexpected_history_source")
                if instrument and op.get("instrumentUid") != instrument:
                    continue
                symbol = op.get("ticker", "")
                if ticker and ticker != symbol:
                    continue
                side = re.fullmatch(
                    r"OPERATION_TYPE_(BUY|SELL)(?:_MARGIN|_CARD)?", op["type"]
                )
                if not side:
                    continue
                for trade in op.get("tradesInfo", {}).get("trades", []):
                    fill = Execution.parse(op["instrumentUid"], side[1], trade, "date")
                    key = (identifier(op["id"]), identifier(trade["num"]))
                    if key in seen:
                        if seen[key] != (fill, symbol):
                            raise Incomplete("conflicting_history_records")
                        continue
                    seen[key] = (fill, symbol)
            has_next = page.get("hasNext", False)
            if not isinstance(has_next, bool):
                raise Incomplete("invalid_pagination")
            if not has_next:
                return list(seen.values())
            cursor = identifier(page["nextCursor"])
            if cursor in cursors:
                raise Incomplete("repeated_history_cursor")
            cursors.add(cursor)
            query["cursor"] = cursor
