"""Offline G9 experiment: crash a real SQLite writer around a scheduled recalculation."""

import json
import os
import sqlite3
import subprocess
import sys
from contextlib import closing
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

PREVIOUS = "2026-11-12T02:00:00+03:00"
DUE = "2026-11-13T02:00:00+03:00"
PAUSED = "paused_for_recalculation"
RUNNING = "running"
STOPPED = "manually_stopped"
CRASH = 73
POINTS = (
    "after_quotes",
    "after_coefficient",
    "after_mode",
    "after_deadline",
    "after_commit",
)
START = ("5", PAUSED, PREVIOUS)
COMMITTED = ("6", RUNNING, DUE)
RECOVERED = ("7", RUNNING, DUE)


def check(actual, expected, label):
    if actual != expected:
        raise AssertionError(f"{label}: expected {expected!r}, got {actual!r}")


def connect(path):
    db = sqlite3.connect(path, isolation_level=None)
    check(db.execute("PRAGMA journal_mode=WAL").fetchone()[0], "wal", "journal mode")
    db.execute("PRAGMA synchronous=FULL")
    check(db.execute("PRAGMA synchronous").fetchone()[0], 2, "synchronous")
    return db


def initialize(path):
    with closing(connect(path)) as db:
        db.execute(
            "CREATE TABLE state(id INTEGER PRIMARY KEY CHECK(id=1),"
            " coefficient TEXT NOT NULL, mode TEXT NOT NULL, completed TEXT NOT NULL)"
        )
        db.execute("INSERT INTO state VALUES(1, ?, ?, ?)", START)


def snapshot(db):
    return db.execute(
        "SELECT coefficient, mode, completed FROM state WHERE id=1"
    ).fetchone()


def recalculate(db, copy_capital, point=None, *, stop_during_quotes=False):
    # Synthetic clock: today's deadline is already due. Fresh, valid RUB pairs.
    _, mode, completed = snapshot(db)
    if mode == STOPPED or completed >= DUE:
        return False
    coefficient = str(Decimal(copy_capital) / Decimal(100_000))

    def interrupt(here):
        if point == here:
            os._exit(CRASH)  # No cleanup, implicit commit, or graceful shutdown.

    interrupt("after_quotes")
    if stop_during_quotes:
        # Owner's stop is committed before the outstanding valuation can be saved.
        db.execute("UPDATE state SET mode=? WHERE id=1", (STOPPED,))
    db.execute("BEGIN IMMEDIATE")
    _, mode, completed = snapshot(db)
    if mode == STOPPED or completed >= DUE:
        db.rollback()
        return False
    db.execute("UPDATE state SET coefficient=? WHERE id=1", (coefficient,))
    interrupt("after_coefficient")
    db.execute("UPDATE state SET mode=? WHERE id=1", (RUNNING,))
    interrupt("after_mode")
    db.execute("UPDATE state SET completed=? WHERE id=1", (DUE,))
    interrupt("after_deadline")
    db.commit()
    interrupt("after_commit")
    return True


def run_child(path, action, *args, expected_exit=0):
    result = subprocess.run(
        [sys.executable, str(Path(__file__).resolve()), action, str(path), *args],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if result.returncode != expected_exit:
        raise RuntimeError(f"{action}: exit {result.returncode}: {result.stderr}")
    return json.loads(result.stdout) if expected_exit == 0 else None


def recovery_worker(path):
    with closing(connect(path)) as db:
        check(db.execute("PRAGMA integrity_check").fetchall(), [("ok",)], "integrity")
        before = snapshot(db)
        saved = recalculate(db, 700_000)
        print(json.dumps({"before": before, "saved": saved, "after": snapshot(db)}))


def experiment(folder):
    results = []
    for point in POINTS:
        path = folder / f"{point}.db"
        initialize(path)
        run_child(path, "crash", point, expected_exit=CRASH)
        first = run_child(path, "recover")
        committed = point == "after_commit"
        check(tuple(first["before"]), COMMITTED if committed else START, point)
        check(first["saved"], not committed, "run only the outstanding deadline")
        final = COMMITTED if committed else RECOVERED
        check(tuple(first["after"]), final, "coherent recovery")
        second = run_child(path, "recover")
        check(
            second,
            {"before": list(final), "saved": False, "after": list(final)},
            "same deadline after another restart",
        )
        results.append({"point": point, **first})

    path = folder / "manual_stop.db"
    initialize(path)
    with closing(connect(path)) as db:
        check(recalculate(db, 600_000, stop_during_quotes=True), False, "late result")
    stopped = ["5", STOPPED, PREVIOUS]
    check(
        run_child(path, "recover"),
        {"before": stopped, "saved": False, "after": stopped},
        "manual stop survives restart",
    )
    return {
        "result": "PASS synthetic",
        "sqlite": sqlite3.sqlite_version,
        "crashes": results,
        "manual_stop": stopped,
    }


if __name__ == "__main__":
    if sys.argv[1:2] == ["crash"]:
        with closing(connect(Path(sys.argv[2]))) as db:
            recalculate(db, 600_000, sys.argv[3])
    elif sys.argv[1:2] == ["recover"]:
        recovery_worker(Path(sys.argv[2]))
    else:
        with TemporaryDirectory() as directory:
            print(json.dumps(experiment(Path(directory))))
