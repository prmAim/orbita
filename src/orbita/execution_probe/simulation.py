"""In-memory HTTP responses used by the standalone offline check."""

import json
from datetime import UTC, datetime, timedelta

import httpx

from .broker import ACCOUNTS, HISTORY, STREAM, Execution, instant

ACCOUNT = "fixture-source"


class Simulation:
    def __init__(self):
        now = datetime.now(UTC)
        self.time = now.isoformat()
        self.known_time = (now - timedelta(days=1)).isoformat()
        self.price = {"units": "101", "nano": 500000000}
        self.connections = 0

    def confirm(self, fill: Execution, ticker: str) -> bool:
        reference = {
            "EARLIER": ("earlier-instrument", instant(self.known_time)),
            "DEMO": ("fixture-instrument", instant(self.time)),
        }
        return (
            reference.get(ticker) == (fill.instrument, fill.time)
            and fill.side == "BUY"
            and fill.pieces == 3
            and fill.price == 101.5
        )

    def __call__(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        if request.method != "POST":
            return httpx.Response(405)
        if str(request.url) == ACCOUNTS:
            account = {
                "id": ACCOUNT,
                "status": "ACCOUNT_STATUS_OPEN",
                "accessLevel": "ACCOUNT_ACCESS_LEVEL_READ_ONLY",
            }
            return httpx.Response(200, json={"accounts": [account]})
        if str(request.url) == STREAM and body["accounts"] == [ACCOUNT]:
            self.connections += 1
            frames = [
                {
                    "subscription": {
                        "status": "RESULT_SUBSCRIPTION_STATUS_OK",
                        "accounts": [ACCOUNT],
                    }
                },
                {"ping": {}},
            ]
            if self.connections == 1:
                frames.append(
                    {
                        "orderTrades": {
                            "accountId": ACCOUNT,
                            "instrumentUid": "fixture-instrument",
                            "direction": "ORDER_DIRECTION_BUY",
                            "trades": [
                                {
                                    "dateTime": self.time,
                                    "price": self.price,
                                    "quantity": "3",
                                    "tradeId": "stream-42",
                                }
                            ],
                        }
                    }
                )
            return httpx.Response(
                200, text="\n".join(json.dumps({"result": f}) for f in frames)
            )
        if str(request.url) == HISTORY and body["accountId"] == ACCOUNT:
            if "cursor" not in body:
                return httpx.Response(
                    200, json={"items": [], "hasNext": True, "nextCursor": "p2"}
                )
            if body["cursor"] == "p2":
                item = {
                    "id": "fixture-op",
                    "brokerAccountId": ACCOUNT,
                    "type": "OPERATION_TYPE_BUY",
                    "instrumentUid": "fixture-instrument",
                    "ticker": "DEMO",
                    "tradesInfo": {
                        "trades": [
                            {
                                "num": "history-17",
                                "date": self.time,
                                "quantity": "3",
                                "price": self.price,
                            }
                        ]
                    },
                }
                if "instrumentId" not in body:
                    item.update(ticker="EARLIER", instrumentUid="earlier-instrument")
                    item["tradesInfo"]["trades"][0]["date"] = self.known_time
                return httpx.Response(200, json={"items": [item], "hasNext": False})
        return httpx.Response(400)
