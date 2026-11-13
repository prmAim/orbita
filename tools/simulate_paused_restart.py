"""Wayfinder T10. Restore a deadline and an independent instrument pause together."""

import json
import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory

from simulate_order_deadline import Broker, Coordinator, check, initialize_journals

A_ORDER = "A/order-1"
B_ORDER = "B/order-2"


def on_signal(client, broker, instrument, key, now):
    if client.execute(
        "SELECT 1 FROM pauses WHERE instrument=?", (instrument,)
    ).fetchone():
        return "paused"
    Coordinator(client, broker, key).submit(now, lost=False)
    return "submitted"


def calls(remote):
    return remote.execute(
        "SELECT kind,request_key,ms FROM calls JOIN orders USING(id) ORDER BY calls.rowid"
    ).fetchall()


def worker(folder, stage, with_pause):
    with (
        closing(sqlite3.connect(folder / "client.db")) as client,
        closing(sqlite3.connect(folder / "broker.db")) as remote,
    ):
        broker = Broker(remote)
        if stage == "before":
            initialize_journals(client, remote)
            client.execute("CREATE TABLE pauses(instrument TEXT PRIMARY KEY)")
            on_signal(client, broker, "A", A_ORDER, 0)
            if with_pause:
                with client:
                    client.execute("INSERT INTO pauses VALUES ('B')")
            with remote:
                remote.execute(
                    "UPDATE orders SET visible=1 WHERE request_key=?", (A_ORDER,)
                )
            return

        order_a = Coordinator(client, broker, A_ORDER)
        check(broker.lookup(A_ORDER), [(1, "active")], "startup broker snapshot")
        order_a.reconcile(40_000)
        paused = client.execute("SELECT instrument FROM pauses").fetchall()
        check(paused, [("B",)] if with_pause else [], "pause after reconciliation")
        check(
            client.execute(
                "SELECT broker_id,status,anchor_ms FROM intents WHERE request_key=?",
                (A_ORDER,),
            ).fetchone(),
            (1, "active", 0),
            "original confirmation after restart",
        )
        signal = on_signal(client, broker, "B", B_ORDER, 41_000)
        expected = [("post", A_ORDER, 0)]
        if not with_pause:
            expected.append(("post", B_ORDER, 41_000))
        order_a.tick(59_999)
        check(calls(remote), expected, "calls before deadline")
        check(signal, "paused" if with_pause else "submitted", "B signal at 41s")
        order_a.tick(60_000)
        expected.append(("cancel", A_ORDER, 60_000))
        check(calls(remote), expected, "calls at deadline")
        order_a.reconcile(60_001)
        order_a.tick(100_000)
        check(calls(remote), expected, "calls after repeated reconciliation and tick")
        return {"paused": paused, "signal_B": signal, "calls": calls(remote)}


def experiment(folder, with_pause):
    folder.mkdir()
    for stage in ("before", "after"):
        completed = subprocess.run(
            [
                sys.executable,
                str(Path(__file__).resolve()),
                "--worker",
                str(folder),
                stage,
                "paused" if with_pause else "open",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if completed.returncode:
            raise RuntimeError(completed.stderr)
    return json.loads(completed.stdout)


if __name__ == "__main__":
    if sys.argv[1:2] == ["--worker"]:
        print(
            json.dumps(worker(Path(sys.argv[2]), sys.argv[3], sys.argv[4] == "paused"))
        )
    else:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            result = experiment(root / "paused", True)
            control = experiment(root / "open", False)
        print(
            json.dumps({"T10": "PASS synthetic", "result": result, "control": control})
        )
