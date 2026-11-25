import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools.codex_langfuse_hook import ROOT, handle


class CodexTraceTest(unittest.TestCase):
    def test_direct_initial_and_followup_turns(self):
        with tempfile.TemporaryDirectory() as tmp, patch("httpx.Client") as factory:
            client = factory.return_value.__enter__.return_value
            client.get.return_value.json.return_value = {"data": [{"name": "orbita"}]}
            path = Path(tmp) / "rollout-session.jsonl"
            path.touch()
            traces = set()
            for turn in ("direct", "create_thread", "send_message_to_thread"):
                with self.subTest(turn=turn):
                    client.post.reset_mock()
                    event = dict(
                        cwd=str(ROOT),
                        turn_id=turn,
                        session_id="session",
                        transcript_path=str(path),
                    )
                    prompt = f"Spring Boot <health> & {turn}"
                    if turn == "direct":
                        handle(
                            dict(
                                event, hook_event_name="UserPromptSubmit", prompt=prompt
                            )
                        )
                    else:
                        item = dict(
                            type="FunctionCallOutput",
                            namespace="codex_app",
                            name=turn,
                            output=f"<codex_delegation>\n  <source_thread_id>origin</source_thread_id>\n  <input>{prompt}</input>\n</codex_delegation>",
                        )
                        payload = dict(
                            type="item_completed",
                            thread_id="session",
                            turn_id=turn,
                            item=item,
                            completed_at_ms=1000,
                        )
                        with path.open("a", encoding="utf-8") as stream:
                            print(
                                json.dumps(dict(type="event_msg", payload=payload)),
                                file=stream,
                            )
                    names = [
                        "mcp__context7__resolve_library_id",
                        "mcp__context7__query_docs",
                    ]
                    for name in names:
                        handle(
                            dict(
                                event,
                                hook_event_name="PostToolUse",
                                tool_name=name,
                                tool_use_id=name,
                                tool_input={},
                                tool_response="docs",
                            )
                        )
                    handle(
                        dict(event, hook_event_name="Stop", last_assistant_message="UP")
                    )
                    spans = [
                        span
                        for call in client.post.call_args_list
                        for span in call.kwargs["json"]["resourceSpans"][0][
                            "scopeSpans"
                        ][0]["spans"]
                    ]
                    self.assertCountEqual(
                        [s["name"] for s in spans],
                        ["codex.user", *names, "codex.assistant"],
                    )
                    root = next(s for s in spans if s["name"] == "codex.user")
                    self.assertIn(
                        dict(
                            key="langfuse.observation.input",
                            value=dict(stringValue=prompt),
                        ),
                        root["attributes"],
                    )
                    self.assertTrue(all(s["traceId"] == root["traceId"] for s in spans))
                    self.assertNotIn(root["traceId"], traces)
                    traces.add(root["traceId"])
