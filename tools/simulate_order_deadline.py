"""Wayfinder T09. Offline SQLite journals, separate worker processes, exact model clock."""

import json
import sqlite3
import subprocess
import sys
from contextlib import ExitStack, closing
from pathlib import Path
from tempfile import TemporaryDirectory

KEY = "synthetic-request-1"
WAIT_MS = 60_000


class Broker:
    def __init__(self, db):
        self.db = db

    def post(self, key, now, lost):
        with self.db:
            number = self.db.execute(
                "INSERT INTO orders(request_key, status) VALUES (?, 'active')", (key,)
            ).lastrowid
            self.db.execute("INSERT INTO calls VALUES ('post', ?, ?)", (number, now))
        if lost:
            raise TimeoutError
        return number

    def lookup(self, key):
        return self.db.execute(
            "SELECT id,status FROM orders WHERE request_key=? AND visible=1", (key,)
        ).fetchall()

    def cancel(self, number, now):
        # Intentionally just record the request. Cancellation is not yet confirmed.
        with self.db:
            self.db.execute("INSERT INTO calls VALUES ('cancel', ?, ?)", (number, now))


class Coordinator:
    def __init__(self, db, broker, key=KEY):
        self.db = db
        self.broker = broker
        self.key = key

    def submit(self, now, lost):
        with self.db:
            inserted = self.db.execute(
                "INSERT INTO intents(request_key,status) VALUES (?, 'unknown') "
                "ON CONFLICT(request_key) DO NOTHING",
                (self.key,),
            ).rowcount
        if not inserted:
            return
        try:
            number = self.broker.post(self.key, now, lost)
        except TimeoutError:
            return
        self.confirm_active(number, now)

    def confirm_active(self, number, now):
        with self.db:
            self.db.execute(
                "UPDATE intents SET broker_id=?, status='active', "
                "anchor_ms=COALESCE(anchor_ms, ?) "
                "WHERE request_key=? AND status IN ('unknown','active')",
                (number, now, self.key),
            )

    def reconcile(self, now):
        matches = self.broker.lookup(self.key)
        if len(matches) != 1:
            return
        number, status = matches[0]
        if status == "active":
            self.confirm_active(number, now)
        else:
            with self.db:
                self.db.execute(
                    "UPDATE intents SET broker_id=?, status=? WHERE request_key=?",
                    (number, status, self.key),
                )
        self.tick(now)

    def tick(self, now):
        row = self.db.execute(
            "SELECT broker_id,anchor_ms FROM intents WHERE request_key=? AND status='active'",
            (self.key,),
        ).fetchone()
        if row is None or now < row[1] + WAIT_MS:
            return
        with self.db:
            self.db.execute(
                "UPDATE intents SET status='cancel_requested' WHERE request_key=?",
                (self.key,),
            )
        self.broker.cancel(row[0], now)


def worker(folder, steps):
    with ExitStack() as stack:
        local = stack.enter_context(closing(sqlite3.connect(folder / "client.db")))
        remote = stack.enter_context(closing(sqlite3.connect(folder / "broker.db")))
        coordinator = Coordinator(local, Broker(remote))
        for action, now in steps:
            if action in ("lost", "ack"):
                coordinator.submit(now, lost=action == "lost")
            elif action == "read":
                coordinator.reconcile(now)
            elif action == "tick":
                coordinator.tick(now)
            else:
                raise ValueError(action)


def check(actual, expected, label):
    if actual != expected:
        raise AssertionError(f"{label}: expected {expected!r}, got {actual!r}")


def initialize_journals(client, broker):
    client.execute(
        "CREATE TABLE intents(request_key TEXT PRIMARY KEY, broker_id INTEGER,"
        " status TEXT, anchor_ms INTEGER)"
    )
    broker.executescript(
        "CREATE TABLE orders(id INTEGER PRIMARY KEY, request_key TEXT, status TEXT,"
        " visible INTEGER DEFAULT 0); CREATE TABLE calls(kind TEXT, id INTEGER, ms INTEGER);"
    )


def experiment(folder, case):
    folder.mkdir()
    with ExitStack() as stack:
        client = stack.enter_context(closing(sqlite3.connect(folder / "client.db")))
        broker = stack.enter_context(closing(sqlite3.connect(folder / "broker.db")))
        initialize_journals(client, broker)

        def run(*steps):
            subprocess.run(
                [
                    sys.executable,
                    str(Path(__file__).resolve()),
                    "--worker",
                    str(folder),
                    json.dumps(steps),
                ],
                check=True,
                timeout=30,
            )

        def state():
            return client.execute("SELECT status,anchor_ms FROM intents").fetchall()

        def calls():
            return broker.execute(
                "SELECT kind,id,ms FROM calls ORDER BY rowid"
            ).fetchall()

        known = case == "saved_ack"
        sent = 45_000 if known else 0
        posts = [("post", 1, sent)]
        run(("ack" if known else "lost", sent), ("read", sent + 1))
        check(
            state(),
            [("active", sent)] if known else [("unknown", None)],
            "before visibility",
        )
        check(calls(), posts, "accepted once")
        with broker:
            broker.execute(
                "UPDATE orders SET visible=1, status=?",
                ("filled" if case == "already_filled" else "active",),
            )
        if case == "already_filled":
            run(("read", 200_000), ("tick", 400_000))
            check(state(), [("filled", None)], "no timer for completed order")
            check(calls(), posts, "completed order untouched")
            return {"case": case, "calls": calls()}

        first_read = 95_000 if known else 200_000
        anchor = sent if known else first_read
        # Explicit oracles from A37/A38, independent of WAIT_MS.
        deadline = 105_000 if known else 260_000
        run(("read", first_read), ("read", first_read + 1_000))
        check(state(), [("active", anchor)], "first confirmation survives another poll")
        check(calls(), posts, "no cancel on first confirmation")
        if case == "overdue_restart":
            actual_cancel = 275_000
        else:
            run(("read", 99_000 if known else 220_000), ("tick", deadline - 1))
            check(calls(), posts, "one millisecond before original deadline")
            check(
                state(), [("active", anchor)], "repeat reconciliation preserved anchor"
            )
            actual_cancel = deadline
        run(("read", actual_cancel))
        check(
            calls(),
            posts + [("cancel", 1, actual_cancel)],
            "cancel on original deadline or overdue startup",
        )
        check(state(), [("cancel_requested", anchor)], "request is not confirmation")
        run(("read", actual_cancel + 1), ("tick", actual_cancel + WAIT_MS))
        check(calls(), posts + [("cancel", 1, actual_cancel)], "no extra calls")
        return {
            "case": case,
            "anchor_ms": anchor,
            "deadline_ms": deadline,
            "calls": calls(),
        }


if __name__ == "__main__":
    if sys.argv[1:2] == ["--worker"]:
        worker(Path(sys.argv[2]), json.loads(sys.argv[3]))
    else:
        with TemporaryDirectory() as directory:
            results = [
                experiment(Path(directory) / case, case)
                for case in (
                    "lost_answer",
                    "overdue_restart",
                    "saved_ack",
                    "already_filled",
                )
            ]
        print(
            json.dumps(
                {"scenario": "T09", "result": "PASS synthetic", "cases": results}
            )
        )
