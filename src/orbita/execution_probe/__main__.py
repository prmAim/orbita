"""Проверка чтения исполнений счёта автоследования по задаче №6."""

import argparse
import json
import sys
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from getpass import getpass
from pathlib import Path
from uuid import uuid4

import httpx

from .broker import Broker, Execution, Incomplete
from .simulation import ACCOUNT, Simulation


def check(broker: Broker, confirm: Callable, attempts: int, delay: float, known=None):
    broker.verify_account()
    if known:
        day, symbol = known
        rows = broker.history(
            day.isoformat(), (day + timedelta(days=1)).isoformat(), ticker=symbol
        )
        if not any(
            confirm(fill, ticker)
            for fill, ticker in rows
            if fill.time[:10] == day.date().isoformat()
        ):
            raise Incomplete("known_execution_unconfirmed")
        broker.log("known_execution_matched")
    execution = broker.observe()
    assert execution is not None
    broker.observe(reconnect=True)
    # Anchor the overlapping interval to the fill, not its arrival at the client.
    start = datetime.fromisoformat(execution.time) - timedelta(minutes=5)
    end = datetime.now(UTC)
    if start + timedelta(minutes=5) > end:
        raise Incomplete("execution_ahead_of_local_clock")
    for scan in (1, 2):
        for attempt in range(1, attempts + 1):
            broker.log("history_scan", scan=scan, attempt=attempt)
            rows = broker.history(
                start.isoformat(), end.isoformat(), instrument=execution.instrument
            )
            symbols = [symbol for fill, symbol in rows if fill == execution]
            if len(symbols) > 1:
                raise Incomplete("ambiguous_history_match")
            if symbols:
                broker.log("unique_match", scan=scan)
                break
            if attempt < attempts:
                time.sleep(delay)
        else:
            raise Incomplete("execution_missing_in_history")
    if not confirm(execution, symbols[0]):
        raise Incomplete("broker_display_unconfirmed")
    broker.log("broker_display_matched")


def confirm_live(fill: Execution, ticker: str) -> bool:
    print("Сверьте исполнение с приложением Т-Банка на выбранном счёте.")
    print(
        f"{ticker} ({fill.instrument}), {fill.side}, {fill.time}, "
        f"{fill.pieces} шт., цена {fill.price}"
    )
    return (
        input("Все поля совпали? Введите ДА для подтверждения: ").strip().upper()
        == "ДА"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--live-read-only",
        action="store_true",
        help="чтение подключённого счёта вместо имитации",
    )
    parser.add_argument(
        "--known-history",
        action="store_true",
        help="сначала сверить уже известное исполнение по дате UTC и тикеру",
    )
    args = parser.parse_args()
    mode = "live-read-only" if args.live_read_only else "offline"
    directory = Path(".scratch/execution-probe")
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{mode}-{uuid4().hex}.jsonl"
    print(f"Режим {mode}. Протокол: {path}", flush=True)
    with path.open("x", encoding="utf-8") as report:

        def log(event, **fields):
            record = {
                "utc": datetime.now(UTC).isoformat(),
                "mode": mode,
                "event": event,
                **fields,
            }
            report.write(json.dumps(record) + "\n")
            report.flush()
            print(event, *fields.values(), flush=True)

        log("started")
        try:
            known = None
            if args.live_read_only:
                if not sys.stdin.isatty():
                    raise Incomplete("interactive_terminal_required")
                if args.known_history:
                    day = datetime.strptime(
                        input("Дата известной сделки в UTC (ГГГГ-ММ-ДД): ").strip(),
                        "%Y-%m-%d",
                    )
                    ticker = input("Тикер известной сделки: ").strip().upper()
                    if not ticker:
                        raise Incomplete("empty_ticker")
                    known = (day.replace(tzinfo=UTC), ticker)
                account = getpass(
                    "ID подключённого счёта автоследования (скрытый ввод): "
                ).strip()
                token = getpass(
                    "Локальный токен только для чтения (скрытый ввод): "
                ).strip()
                if not account or not token:
                    raise Incomplete("empty_credentials")
                client = httpx.Client(
                    headers={"Authorization": f"Bearer {token}"}, timeout=30
                )
            else:
                account = ACCOUNT
                sim = Simulation()
                if args.known_history:
                    known = (
                        datetime.fromisoformat(sim.known_time).replace(
                            hour=0, minute=0, second=0, microsecond=0
                        ),
                        "EARLIER",
                    )
                client = httpx.Client(
                    transport=httpx.MockTransport(sim), trust_env=False
                )
            with client:
                check(
                    Broker(client, account, log),
                    confirm_live if args.live_read_only else sim.confirm,
                    attempts=12 if args.live_read_only else 1,
                    delay=10,
                    known=known,
                )
        except Incomplete as exc:
            log("incomplete", reason=str(exc))
        except httpx.HTTPStatusError as exc:
            log("incomplete", reason="http_status", status=exc.response.status_code)
        except (KeyboardInterrupt, EOFError):
            log("incomplete", reason="operator_interrupted")
        except (
            httpx.HTTPError,
            ValueError,
            TypeError,
            KeyError,
            AttributeError,
        ) as exc:
            log("incomplete", reason=type(exc).__name__)
        else:
            log(
                "passed",
                scope="observed_execution" if args.live_read_only else "synthetic",
            )
            return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
