"""Offline Wayfinder experiment for the bounded R70, R73 and R85–R86 cases."""

import json
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory


@dataclass(frozen=True)
class Fill:
    id: str
    side: str
    lots: int
    time: int  # Broker seconds since 12:00:00, not delivery time.


class Broker:
    def __init__(self):
        self.history = []
        self.history_complete = False
        self.calls = []
        self.copy_lots = 0

    def post(self, source_id, side, lots):
        # Never deduplicate: an erroneous second submission must be visible.
        self.calls.append((source_id, side, lots))


class Recovery:
    """No open orders; historical fills can be missing from stream delivery."""

    def __init__(self, db, broker):
        self.db = db
        self.broker = broker
        db.execute(
            "CREATE TABLE IF NOT EXISTS fills(id TEXT PRIMARY KEY, side TEXT, lots INT,"
            " time INT, k INT, decision TEXT, posted INT DEFAULT 0, skipped INT DEFAULT 0)"
        )

    def receive(self, fill, coefficient, now):
        with self.db:
            self.db.execute(
                "INSERT INTO fills(id, side, lots, time, k, decision) VALUES (?,?,?,?,?,?)"
                " ON CONFLICT(id) DO NOTHING",
                (fill.id, fill.side, fill.lots, fill.time, coefficient, "pending"),
            )
            if fill.side == "sell" and self.broker.copy_lots == 0:
                # Sale processed on an empty copy account before the missing buy.
                self.db.execute(
                    "UPDATE fills SET decision='no_stock', skipped=lots*k"
                    " WHERE id=? AND decision='pending'",
                    (fill.id,),
                )
        return self.reconcile(now)

    def reconcile(self, now):
        rows = self.db.execute("SELECT id, side, lots, time FROM fills").fetchall()
        delivered = {Fill(*row) for row in rows}
        history = sorted(self.broker.history, key=lambda fill: fill.time)
        if not self.broker.history_complete or set(history) != delivered:
            return ()
        if len(history) == 3:
            return self.reconcile_mixed(history, now)
        if len(history) != 2 or self.broker.copy_lots != 0:
            return ()
        buy, sale = history
        if (
            buy.time == sale.time
            or (buy.side, sale.side) != ("buy", "sell")
            or not 0 < sale.lots <= buy.lots
        ):
            return ()
        order = (buy.id, sale.id)
        coefficient, decision = self.db.execute(
            "SELECT k, decision FROM fills WHERE id=?", (buy.id,)
        ).fetchone()
        if decision != "pending":
            return order

        target = (buy.lots - sale.lots) * coefficient
        if target == 0:
            decision = "closed"
        elif not 0 <= now - buy.time <= 60:
            decision, target = "expired", 0
        else:
            # Price, funds (including fees), clock and permissions valid in this fixture.
            self.broker.post(buy.id, "buy", target)
            decision = "submitted"
        with self.db:
            self.db.execute(
                "UPDATE fills SET decision=?, posted=?, skipped=? WHERE id=?",
                (decision, target, buy.lots * coefficient - target, buy.id),
            )
        return order

    def reconcile_mixed(self, history, now):
        # R85–R86 fixture: one confirmed buy, a missing buy, then a partial sale.
        first, late, sale = history
        if [event.side for event in history] != [
            "buy",
            "buy",
            "sell",
        ] or not first.time < late.time < sale.time:
            return ()
        rows = dict(
            (key, (k, decision, posted))
            for key, k, decision, posted in self.db.execute(
                "SELECT id, k, decision, posted FROM fills"
            )
        )
        first_k, first_state, first_lots = rows[first.id]
        late_k, late_state, _ = rows[late.id]
        order = tuple(event.id for event in history)
        if rows[sale.id][1] == "submitted":
            return order
        if (first_state, late_state) != ("filled", "pending"):
            return ()
        total = first.lots + late.lots
        remaining = total - sale.lots
        if not 0 < remaining < total or not 0 <= now - sale.time <= 60:
            return ()
        # This model covers exact whole-lot proportions only.
        targets = [
            divmod(f.lots * remaining * k, total)
            for f, k in [(first, first_k), (late, late_k)]
        ]
        if any(remainder for _, remainder in targets):
            return ()
        actual = self.broker.copy_lots
        excess = actual - sum(lots for lots, _ in targets)
        if actual != first_lots or excess <= 0:
            return ()
        self.broker.post(sale.id, "sell", excess)
        with self.db:
            self.db.execute(
                "UPDATE fills SET decision='covered' WHERE id=?", (late.id,)
            )
            self.db.execute(
                "UPDATE fills SET decision='submitted', posted=? WHERE id=?",
                (excess, sale.id),
            )
        return order


def check(actual, expected, checkpoint):
    if actual != expected:
        raise AssertionError(f"{checkpoint}: expected {expected!r}, got {actual!r}")


def run_case(folder, name, sold, expected_calls, expected_buy, *, age=20, tied=False):
    buy = Fill("B1", "buy", 10, 0)
    sale = Fill("S1", "sell", sold, 0 if tied else 10)
    broker = Broker()
    path = folder / f"{name}.sqlite"
    with closing(sqlite3.connect(path)) as db:
        model = Recovery(db, broker)
        broker.history = [sale]
        model.receive(sale, coefficient=4, now=11)
        check(broker.calls, [], "sale received first")
        model.receive(buy, coefficient=5, now=age)
        check(broker.calls, [], "missing historical purchase")
        broker.history = [sale, buy]  # API page order is deliberately newest first.
        check(model.reconcile(age), (), "history completeness not confirmed")
        check(broker.calls, [], "unconfirmed history")
        broker.history_complete = True
        restored = model.reconcile(age)
        check(restored, () if tied else ("B1", "S1"), "broker execution order")
        check(broker.calls, expected_calls, "submissions after reconciliation")
        model.receive(buy, coefficient=9, now=age)
        model.receive(sale, coefficient=9, now=age)
        check(broker.calls, expected_calls, "duplicate deliveries")

    with closing(sqlite3.connect(path)) as db:
        model = Recovery(db, broker)
        model.receive(sale, coefficient=9, now=age)
        model.receive(buy, coefficient=9, now=age)
        check(broker.calls, expected_calls, "replay after reopening journal")
        states = db.execute(
            "SELECT id, k, decision, posted, skipped FROM fills ORDER BY id"
        ).fetchall()
        check(
            states,
            [("B1", 5, *expected_buy), ("S1", 4, "no_stock", 0, sold * 4)],
            "decisions and original coefficients",
        )
    return {
        "case": name,
        "reconstructed": restored,
        "submissions": broker.calls,
        "buy_decision": states[0][2:],
    }


def run_mixed_case(folder):
    first, late = Fill("B1", "buy", 10, 0), Fill("B2", "buy", 10, 5)
    sale = Fill("S1", "sell", 10, 10)
    broker = Broker()
    path = folder / "mixed.sqlite"
    with closing(sqlite3.connect(path)) as db:
        model = Recovery(db, broker)
        # Known opening state of A107: B1 was already copied and filled.
        broker.post(first.id, "buy", 50)
        broker.copy_lots = 50
        with db:
            db.execute(
                "INSERT INTO fills VALUES (?,?,?,?,?,'filled',?,0)",
                (first.id, first.side, first.lots, first.time, 5, 50),
            )
        original = list(broker.calls)
        broker.history = [sale, first]
        model.receive(sale, 4, 11)
        model.receive(late, 3, 20)
        check(broker.calls, original, "mixed: incomplete history")
        broker.history = [sale, late, first]
        broker.history_complete = True
        order = model.reconcile(20)
        check(order, ("B1", "B2", "S1"), "mixed: recovered execution order")
        expected = [("B1", "buy", 50), ("S1", "sell", 10)]
        check(broker.calls, expected, "mixed: single correction, no second purchase")
    with closing(sqlite3.connect(path)) as db:
        model = Recovery(db, broker)
        for event in [sale, late, first]:
            model.receive(event, 9, 21)
        check(broker.calls, expected, "mixed: replay after reopening journal")
    return {
        "case": "mixed",
        "reconstructed": order,
        "initial_calls": original,
        "new_submissions": broker.calls[len(original) :],
    }


def main():
    with TemporaryDirectory() as directory:
        folder = Path(directory)
        results = [
            run_case(folder, "full_close", 10, [], ("closed", 0, 50)),
            run_case(
                folder, "partial_close", 4, [("B1", "buy", 30)], ("submitted", 30, 20)
            ),
            run_case(folder, "expired_buy", 4, [], ("expired", 0, 50), age=61),
            run_case(folder, "ambiguous_time", 4, [], ("pending", 0, 0), tied=True),
            run_mixed_case(folder),
        ]
    print(json.dumps({"result": "PASS synthetic", "cases": results}))


if __name__ == "__main__":
    main()
