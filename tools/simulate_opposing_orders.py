"""A40/T22 offline model: two open copy buys, followed by one source sale."""

import json
from dataclasses import dataclass


@dataclass(frozen=True)
class Fill:
    order: str
    execution: str
    lots: int


class Broker:
    """Synthetic broker. Its order log records every call without deduplication."""

    def __init__(self):
        self.buys = {"B1": 6, "B2": 7}
        self.executions = {order: [] for order in self.buys}
        self.cancel_calls = []
        self.final_results = {}
        self.sales = []

    def cancel(self, order):
        self.cancel_calls.append(order)

    def execute_buy(self, order, execution, lots):
        fill = Fill(order, execution, lots)
        self.executions[order].append(fill)
        return fill

    def confirm_cancel(self, order):
        if order not in self.cancel_calls:
            raise AssertionError("unexpected cancellation")
        filled = sum(fill.lots for fill in self.executions[order])
        self.final_results[order] = (filled, self.buys[order] - filled)

    def post_sale(self, source, lots):
        self.sales.append((source, lots))


class Coordinator:
    """One instrument; initially no holdings, no sales or other reservations."""

    def __init__(self, broker):
        self.broker = broker
        self.opposing = {}
        self.fills = {}
        self.source = None
        self.target = 0
        self.sent = False
        self.skipped_buys = {}

    def source_sale(self, source, target):
        if self.source is None:
            self.source, self.target = source, target
            self.opposing = self.broker.buys.copy()
            for order in self.opposing:
                self.broker.cancel(order)
        elif (source, target) != (self.source, self.target):
            raise ValueError("fixture accepts one source sale")
        self.reconcile()

    def receive(self, fill):
        key = (fill.order, fill.execution)
        previous = self.fills.setdefault(key, fill.lots)
        if previous != fill.lots:
            raise ValueError("conflicting execution")
        self.reconcile()

    def reconcile(self):
        if self.source is None or self.sent:
            return
        for order, requested in self.opposing.items():
            final = self.broker.final_results.get(order)
            if final is None:
                return
            executed, cancelled = final
            observed = sum(
                lots for (parent, _), lots in self.fills.items() if parent == order
            )
            if executed + cancelled != requested or observed != executed:
                return
            self.skipped_buys[order] = cancelled
        # All other trading conditions hold throughout the short test interval.
        available = sum(self.fills.values())
        lots = min(self.target, available)
        if lots:
            self.broker.post_sale(self.source, lots)
            self.sent = True


def check(actual, expected, step):
    if actual != expected:
        raise AssertionError(f"{step}: expected {expected!r}, got {actual!r}")


def exercise(first, second, *, delayed_fill):
    broker = Broker()
    model = Coordinator(broker)
    initial = broker.execute_buy(first, "F1", 2)
    model.receive(initial)
    model.source_sale("S1", 8)
    check(sorted(broker.cancel_calls), ["B1", "B2"], "cancel both buys")
    check(broker.sales, [], "cancel requests are not confirmations")

    broker.confirm_cancel(first)
    model.reconcile()
    check(broker.sales, [], "one final result missing")
    late = broker.execute_buy(second, "F2", 3)
    if delayed_fill:
        broker.confirm_cancel(second)
        model.reconcile()
        check(broker.sales, [], "terminal totals but missing execution")
        model.receive(late)
    else:
        model.receive(late)
        model.receive(late)
        check(
            sum(model.fills.values()), 5, "replayed three-lot fill before confirmation"
        )
        model.receive(initial)
        model.source_sale("S1", 8)
        check(broker.sales, [], "all five lots visible but cancellation still unknown")
        broker.confirm_cancel(second)
        model.reconcile()

    expected = [("S1", 5)]
    check(broker.sales, expected, "first complete reconciliation")
    for fill in (initial, late, late):
        model.receive(fill)
    model.source_sale("S1", 8)
    model.reconcile()
    check(broker.sales, expected, "replayed fills, sale and reconciliation")
    check(sorted(broker.cancel_calls), ["B1", "B2"], "no repeat cancels")
    check(sum(model.fills.values()), 5, "unique filled lots")
    check(
        model.skipped_buys,
        {first: broker.buys[first] - 2, second: broker.buys[second] - 3},
        "cancelled remainders",
    )
    return {
        "first_confirmed": first,
        "final_before_late_fill": delayed_fill,
        "cancel_calls": broker.cancel_calls,
        "sale_calls": broker.sales,
        "unique_filled_lots": sum(model.fills.values()),
    }


if __name__ == "__main__":
    cases = [
        exercise(first, second, delayed_fill=delayed)
        for first, second in [("B1", "B2"), ("B2", "B1")]
        for delayed in (False, True)
    ]
    print(
        json.dumps({"scenario": "A40/T22", "result": "PASS synthetic", "cases": cases})
    )
