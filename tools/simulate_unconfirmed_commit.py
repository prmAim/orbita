"""Wayfinder T06. Offline, single writer, current SQLite database, synthetic broker."""

import json
import sqlite3
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory


class Broker:
    def __init__(self):
        self.orders = {}
        self.calls = []
        self.hidden = set()

    def post(self, key, symbol, lots):
        self.calls.append(("post", key))
        number = len(self.orders) + 1
        self.orders[number] = (key, symbol, lots, "open", 0)
        return number

    def cancel(self, number):
        self.calls.append(("cancel", number))
        key, symbol, lots, _, filled = self.orders[number]
        self.orders[number] = (key, symbol, lots, "cancelled", filled)

    def lookup(self, key):
        return [
            (number, *values[1:])
            for number, values in self.orders.items()
            if values[0] == key and key not in self.hidden
        ]


class CommitUnknown(Exception):
    """Storage adapter cannot attest whether the acknowledgement was committed."""


class Coordinator:
    def __init__(self, db, broker):
        self.db = db
        self.broker = broker
        self.ready = False

    def reconcile(self):
        self.ready = False
        try:
            if self.db.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
                return False
            with self.db:
                for key, symbol, lots, known_id in self.db.execute(
                    "SELECT key, symbol, lots, broker_id FROM intents"
                ).fetchall():
                    matches = self.broker.lookup(key)
                    if len(matches) != 1:
                        raise CommitUnknown
                    number, observed_symbol, observed_lots, status, filled = matches[0]
                    if (observed_symbol, observed_lots) != (
                        symbol,
                        lots,
                    ) or known_id not in (None, number):
                        raise CommitUnknown
                    self.db.execute(
                        "UPDATE intents SET broker_id=?, status=?, filled=? WHERE key=?",
                        (number, status, filled, key),
                    )
                # Verify writes and commit even when there are no intents yet.
                self.db.execute("UPDATE health SET checks=checks+1")
        except (sqlite3.Error, CommitUnknown):
            return False
        self.ready = True
        return True

    def post(self, key, symbol, lots, fault=None):
        if not self.ready:
            return
        try:
            with self.db:
                inserted = self.db.execute(
                    "INSERT INTO intents(key,symbol,lots) VALUES (?,?,?) "
                    "ON CONFLICT(key) DO NOTHING",
                    (key, symbol, lots),
                ).rowcount
            if not inserted:
                return
            number = self.broker.post(key, symbol, lots)
            with self.db:
                self.db.execute(
                    "UPDATE intents SET broker_id=?, status='open' WHERE key=?",
                    (number, key),
                )
                if fault == "full":
                    # Real SQLITE_FULL, confined to this size-limited test database.
                    self.db.execute("INSERT INTO payload VALUES(zeroblob(1048576))")
                if fault == "rolled_back":
                    raise CommitUnknown
            if fault == "committed":
                raise CommitUnknown
        except (sqlite3.Error, CommitUnknown) as error:
            self.ready = False
            return getattr(error, "sqlite_errorname", type(error).__name__)

    def cancel(self, key):
        if not self.ready:
            return
        try:
            row = self.db.execute(
                "SELECT broker_id FROM intents WHERE key=? AND status='open'", (key,)
            ).fetchone()
            if row:
                with self.db:
                    self.db.execute(
                        "UPDATE intents SET status='cancelling' WHERE key=?", (key,)
                    )
                self.broker.cancel(row[0])
        except sqlite3.Error:
            self.ready = False


def check(actual, expected, step):
    if actual != expected:
        raise AssertionError(f"{step}: expected {expected!r}, got {actual!r}")


def probe_block(model, broker, baseline, stage):
    # These are otherwise valid actions on another instrument and a known active order.
    model.post("fresh", "C", 2)
    model.cancel("prior")
    model.post("lost", "B", 3)
    model.cancel("lost")
    check(broker.calls, baseline, stage)


def run_case(path, fault):
    broker = Broker()
    with closing(sqlite3.connect(path)) as db:
        db.executescript("""
            CREATE TABLE intents(key TEXT PRIMARY KEY, symbol TEXT, lots INTEGER,
                                 broker_id INTEGER, status TEXT DEFAULT 'started', filled INTEGER DEFAULT 0);
            CREATE TABLE health(checks INTEGER);
            INSERT INTO health VALUES(0);
            CREATE TABLE payload(data BLOB);
        """)
        model = Coordinator(db, broker)
        check(model.reconcile(), True, "initial readiness")
        model.post("prior", "A", 4)
        pages = db.execute("PRAGMA page_count").fetchone()[0]
        if fault == "full":
            check(
                db.execute(f"PRAGMA max_page_count={pages}").fetchone()[0],
                pages,
                "quota",
            )
        error = model.post("lost", "B", 3, fault)
        check(
            error,
            "SQLITE_FULL" if fault == "full" else "CommitUnknown",
            "fault reached",
        )
        baseline = [("post", "prior"), ("post", "lost")]
        check(broker.calls, baseline, "broker acceptance")
        local_id = db.execute(
            "SELECT broker_id FROM intents WHERE key='lost'"
        ).fetchone()[0]
        check(local_id, 2 if fault == "committed" else None, "actual commit outcome")
        probe_block(model, broker, baseline, "after unconfirmed write")
        # Broker can execute while the client is blocked; an accepted order was not undone.
        broker.orders[2] = ("lost", "B", 3, "filled", 3)
        db.execute("PRAGMA query_only=ON")
        check(model.reconcile(), False, "database still cannot record reconciliation")
        probe_block(model, broker, baseline, "failed storage recovery")

    with closing(sqlite3.connect(path)) as db:
        # Reopen the same current file, not a backup. Writer is healthy again.
        model = Coordinator(db, broker)
        probe_block(
            model, broker, baseline, "healthy database before broker reconciliation"
        )
        broker.hidden.add("lost")
        check(model.reconcile(), False, "incomplete broker evidence")
        probe_block(model, broker, baseline, "incomplete broker reconciliation")
        broker.hidden.clear()
        check(model.reconcile(), True, "complete and durably recorded reconciliation")
        check(
            db.execute(
                "SELECT broker_id,status,filled FROM intents WHERE key='lost'"
            ).fetchone(),
            (2, "filled", 3),
            "original order and its actual fill",
        )
        model.post("lost", "B", 3)
        check(broker.calls, baseline, "no blind repost")
        model.post("fresh", "C", 2)
        model.cancel("prior")
        check(
            broker.calls,
            baseline + [("post", "fresh"), ("cancel", 1)],
            "fresh post and cancel permitted after reconciliation",
        )
    return {
        "fault": fault,
        "local_id_before_recovery": local_id,
        "error": error,
        "calls": broker.calls,
        "reconciled_filled_lots": 3,
    }


if __name__ == "__main__":
    with TemporaryDirectory() as directory:
        results = [
            run_case(Path(directory) / f"{fault}.sqlite", fault)
            for fault in ("full", "rolled_back", "committed")
        ]
    print(json.dumps({"scenario": "T06", "result": "PASS synthetic", "cases": results}))
