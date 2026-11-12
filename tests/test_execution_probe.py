import json
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

import httpx

from orbita.execution_probe.__main__ import check
from orbita.execution_probe.broker import ACCOUNTS, HISTORY, STREAM, Broker, Incomplete
from orbita.execution_probe.simulation import ACCOUNT, Simulation


class ExecutionProbeTests(unittest.TestCase):
    def setUp(self):
        self.sim = Simulation()
        self.events = []
        self.requests = []

    def run_probe(
        self, change=None, *, confirm=lambda *_: True, attempts=1, known=None
    ):
        def handler(request):
            self.requests.append((str(request.url), json.loads(request.content)))
            response = self.sim(request)
            return change(request, response) if change else response

        def log(event, **fields):
            self.events.append((event, fields))

        with httpx.Client(transport=httpx.MockTransport(handler)) as client:
            check(Broker(client, ACCOUNT, log), confirm, attempts, 0, known)

    def test_known_execution_stream_and_overlapping_history(self):
        day = datetime.fromisoformat(self.sim.known_time).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        checked = []

        def confirm(fill, ticker):
            checked.append((ticker, fill.pieces, float(fill.price), fill.side))
            return True

        with patch("socket.socket.connect", side_effect=AssertionError("network")):
            self.run_probe(known=(day, "EARLIER"), confirm=confirm)
        self.assertEqual(
            checked, [("EARLIER", 3, 101.5, "BUY"), ("DEMO", 3, 101.5, "BUY")]
        )
        self.assertEqual(
            [url for url, _ in self.requests],
            [
                ACCOUNTS,
                HISTORY,
                HISTORY,
                STREAM,
                STREAM,
                HISTORY,
                HISTORY,
                HISTORY,
                HISTORY,
            ],
        )
        first, second = self.requests[-4:-2], self.requests[-2:]
        self.assertEqual(first, second)
        self.assertNotIn("cursor", first[0][1])
        self.assertEqual(first[1][1]["cursor"], "p2")
        self.assertEqual(self.sim.connections, 2)

    def test_ack_without_execution(self):
        def ack_only(req, resp):
            if str(req.url) == STREAM:
                return httpx.Response(200, text=resp.text.splitlines()[0])
            return resp

        with self.assertRaisesRegex(Incomplete, "stream_ended"):
            self.run_probe(ack_only)
        self.assertNotIn(HISTORY, [url for url, _ in self.requests])

    def test_wrong_account_or_access(self):
        for name, value in [
            ("id", "other"),
            ("accessLevel", "ACCOUNT_ACCESS_LEVEL_FULL_ACCESS"),
        ]:

            def change(req, resp, name=name, value=value):
                data = resp.json()
                data["accounts"][0][name] = value
                return httpx.Response(200, json=data)

            with self.subTest(name=name), self.assertRaises(Incomplete):
                self.run_probe(change)
        self.assertTrue(all(url == ACCOUNTS for url, _ in self.requests))

    def test_rejected_second_subscription(self):
        def reject(req, resp):
            if str(req.url) == STREAM and self.sim.connections == 2:
                return httpx.Response(
                    200, text=resp.text.replace("STATUS_OK", "STATUS_ERROR")
                )
            return resp

        with self.assertRaisesRegex(Incomplete, "subscription_rejected"):
            self.run_probe(reject)

    def test_history_must_be_unique_and_match_exact_fill(self):
        for fault, reason in [
            ("ambiguous", "ambiguous_history"),
            ("side", "missing_in_history"),
            ("nano", "missing_in_history"),
            ("account", "unexpected_history_source"),
            ("quantity", "missing_in_history"),
            ("time", "missing_in_history"),
            ("cursor", "repeated_history_cursor"),
        ]:

            def change(req, resp, fault=fault):
                if str(req.url) != HISTORY or "cursor" not in json.loads(req.content):
                    return resp
                page = resp.json()
                op = page["items"][0]
                trade = op["tradesInfo"]["trades"][0]
                if fault == "ambiguous":
                    op["tradesInfo"]["trades"].append({**trade, "num": "another-fill"})
                elif fault == "side":
                    op["type"] = "OPERATION_TYPE_SELL"
                elif fault == "nano":
                    trade["price"]["nano"] += 1
                elif fault == "account":
                    op["brokerAccountId"] = "other"
                elif fault == "quantity":
                    trade["quantity"] = "4"
                elif fault == "time":
                    trade["date"] = (
                        datetime.fromisoformat(trade["date"]) - timedelta(seconds=1)
                    ).isoformat()
                else:
                    page.update(hasNext=True, nextCursor="p2")
                return httpx.Response(200, json=page)

            self.sim = Simulation()
            with self.subTest(fault=fault), self.assertRaisesRegex(Incomplete, reason):
                self.run_probe(change)

    def test_delayed_visibility_then_repeat_with_changed_operation_id(self):
        scans = 0

        def change(req, resp):
            nonlocal scans
            if str(req.url) == HISTORY and "cursor" in json.loads(req.content):
                scans += 1
                if scans == 1:
                    return httpx.Response(200, json={"items": []})
                page = resp.json()
                op = page["items"][0]
                op["id"] = f"scan-{scans}"
                page["items"].append(op.copy())
                return httpx.Response(200, json=page)
            return resp

        self.run_probe(change, attempts=2)
        self.assertEqual(scans, 3)

    def test_independent_broker_confirmation_required(self):
        with self.assertRaisesRegex(Incomplete, "broker_display_unconfirmed"):
            self.run_probe(confirm=lambda *_: False)
