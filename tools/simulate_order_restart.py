"""Offline Wayfinder T04 experiment. Run with Python 3.13, no dependencies."""

import json
import os
import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

SOURCE = "synthetic-source-fill"
CRASH_EXIT = 73


class SimulatedBroker:
    """Independent durable state. Every submission creates a distinct order."""

    def __init__(self, db):
        self.db = db

    def post(self, request_id, lots):
        with self.db:
            cursor = self.db.execute(
                "INSERT INTO submissions(request_id, lots) VALUES (?, ?)",
                (request_id, lots),
            )
        return cursor.lastrowid

    def lookup(self, request_id, lots):
        with self.db:
            self.db.execute("INSERT INTO lookups VALUES (?)", (request_id,))
        return self.db.execute(
            "SELECT rowid FROM submissions WHERE request_id = ? AND lots = ? "
            "AND (SELECT visible FROM visibility)",
            (request_id, lots),
        ).fetchall()


def handle_fill(local, broker, crash_after_accept=False):
    # Every worker sees the same source execution, including after a restart.
    with local:
        intent = local.execute(
            "SELECT request_id, lots, state FROM intents WHERE source = ?", (SOURCE,)
        ).fetchone()
        if intent is None:
            request_id, lots = str(uuid4()), 3
            local.execute(
                "INSERT INTO intents VALUES (?, ?, ?, 'attempt_started', NULL)",
                (SOURCE, request_id, lots),
            )
    if intent is None:
        order_id = broker.post(request_id, lots)
        if crash_after_accept:
            os._exit(CRASH_EXIT)  # Broker committed; client has not recorded the reply.
    else:
        request_id, lots, state = intent
        if state == "accepted":
            return
        matches = broker.lookup(request_id, lots)
        if len(matches) != 1:
            return  # Attempt remains unresolved. No speculative submission.
        order_id = matches[0][0]

    with local:
        local.execute(
            "UPDATE intents SET state = 'accepted', order_id = ? WHERE source = ?",
            (order_id, SOURCE),
        )


def worker(folder, crash):
    with (
        closing(sqlite3.connect(folder / "client.db")) as local,
        closing(sqlite3.connect(folder / "broker.db")) as remote,
    ):
        handle_fill(local, SimulatedBroker(remote), crash)


def check(actual, expected, step):
    if actual != expected:
        raise AssertionError(f"{step}: expected {expected!r}, got {actual!r}")


def experiment(folder):
    def run_worker(*, crash=False):
        command = [
            sys.executable,
            str(Path(__file__).resolve()),
            "--worker",
            str(folder),
        ]
        if crash:
            command.append("--crash")
        result = subprocess.run(command, check=False, timeout=30)
        check(result.returncode, CRASH_EXIT if crash else 0, "worker exit")

    with (
        closing(sqlite3.connect(folder / "client.db")) as local,
        closing(sqlite3.connect(folder / "broker.db")) as remote,
    ):
        local.execute(
            "CREATE TABLE intents(source TEXT PRIMARY KEY, request_id TEXT UNIQUE,"
            " lots INTEGER, state TEXT, order_id INTEGER)"
        )
        remote.executescript(
            "CREATE TABLE submissions(request_id TEXT, lots INTEGER);"
            "CREATE TABLE lookups(request_id TEXT);"
            "CREATE TABLE visibility(visible INTEGER); INSERT INTO visibility VALUES (0);"
        )

        def posts():
            return remote.execute("SELECT COUNT(*) FROM submissions").fetchone()[0]

        def state():
            return local.execute(
                "SELECT source, request_id, lots, state, order_id FROM intents"
            ).fetchall()

        run_worker(crash=True)
        check(posts(), 1, "broker accepted before crash")
        order_id, request_id, lots = remote.execute(
            "SELECT rowid, request_id, lots FROM submissions"
        ).fetchone()
        pending = [(SOURCE, request_id, 3, "attempt_started", None)]
        check(lots, 3, "accepted volume")
        check(state(), pending, "lost local acknowledgment")
        post_counts = [posts()]

        run_worker()
        check(state(), pending, "lookup temporarily empty")
        post_counts.append(posts())
        with remote:
            remote.execute("UPDATE visibility SET visible = 1")

        run_worker()
        confirmed = [(SOURCE, request_id, 3, "accepted", order_id)]
        check(state(), confirmed, "reconciled original order")
        post_counts.append(posts())
        run_worker()
        check(state(), confirmed, "redelivered fill after reconciliation")
        post_counts.append(posts())
        check(post_counts, [1, 1, 1, 1], "all broker submission calls")
        reads = remote.execute(
            "SELECT request_id FROM lookups ORDER BY rowid"
        ).fetchall()
        check(reads, [(request_id,), (request_id,)], "lookups by original request ID")
        return {
            "scenario": "T04",
            "result": "PASS synthetic",
            "crash_exit": CRASH_EXIT,
            "post_counts": post_counts,
            "lookups": len(reads),
            "broker_orders": posts(),
        }


if __name__ == "__main__":
    if sys.argv[1:2] == ["--worker"]:
        worker(Path(sys.argv[2]), sys.argv[3:] == ["--crash"])
    else:
        with TemporaryDirectory() as directory:
            print(json.dumps(experiment(Path(directory))))
