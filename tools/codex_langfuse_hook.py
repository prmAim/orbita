"""Export Orbita project chat events to its local Langfuse project."""

import hashlib
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TOOLS = {"mcp__context7__resolve_library_id", "mcp__context7__query_docs"}
PROMPT_DELIVERIES = {"create_thread", "send_message_to_thread"}


def forwarded_prompt(event):
    """Read only the delivery envelope for this receiving chat and turn."""
    source = event.get("transcript_path")
    if not source:
        return None
    path = Path(source)
    if not path.name.endswith(event["session_id"] + ".jsonl") or not path.is_file():
        return None
    with path.open("rb") as stream:
        for line in stream:
            if (
                b'"FunctionCallOutput"' not in line
                or event["turn_id"].encode() not in line
            ):
                continue
            record = json.loads(line)
            payload = record.get("payload", {})
            item = payload.get("item", {})
            if not (
                record.get("type") == "event_msg"
                and payload.get("type") == "item_completed"
                and payload.get("thread_id") == event["session_id"]
                and payload.get("turn_id") == event["turn_id"]
                and item.get("type") == "FunctionCallOutput"
                and item.get("namespace") == "codex_app"
                and item.get("name") in PROMPT_DELIVERIES
            ):
                continue
            envelope = item.get("output", "")
            # Codex inserts the message verbatim, including literal XML characters.
            prefix = "</source_thread_id>\n  <input>"
            suffix = "</input>\n</codex_delegation>"
            header, sep, body = envelope.partition(prefix)
            if (
                sep
                and header.startswith("<codex_delegation>\n  <source_thread_id>")
                and body.endswith(suffix)
            ):
                return body[: -len(suffix)], str(payload["completed_at_ms"] * 1_000_000)
    return None


def handle(event):
    session = event.get("session_id")
    cwd = event.get("cwd")
    if (
        not session
        or not cwd
        or not Path(cwd).resolve().is_relative_to(ROOT)
        or event.get("agent_id")
    ):
        return
    kind = event["hook_event_name"]
    turn = event.get("turn_id")
    if not turn:
        return
    if kind == "UserPromptSubmit":
        name, data = "codex.user", {"input": event["prompt"]}
    elif kind == "Stop" and event.get("last_assistant_message"):
        name, data = "codex.assistant", {"output": event["last_assistant_message"]}
    elif kind == "PostToolUse" and event.get("tool_name") in TOOLS:
        name = event["tool_name"]
        data = {"input": event["tool_input"], "output": event["tool_response"]}
    else:
        return

    import httpx

    from orbita.config import Settings

    settings = Settings(_env_file=ROOT / ".env")
    auth = (
        settings.langfuse_public_key.get_secret_value(),
        settings.langfuse_secret_key.get_secret_value(),
    )
    with httpx.Client(
        base_url=settings.langfuse_base_url, auth=auth, trust_env=False, timeout=3
    ) as client:
        projects = client.get("/api/public/projects")
        projects.raise_for_status()
        if [p["name"] for p in projects.json()["data"]] != ["orbita"]:
            raise RuntimeError("Unexpected tracing destination")
        trace = hashlib.sha256((session + turn).encode()).hexdigest()[:32]

        def span_id(label):
            return hashlib.sha256((trace + label).encode()).hexdigest()[:16]

        attrs = {
            "session.id": session,
            "langfuse.trace.name": "Codex chat",
            "langfuse.observation.type": "tool" if name in TOOLS else "span",
        }
        for key, value in data.items():
            attrs["langfuse.observation." + key] = (
                value
                if isinstance(value, str)
                else json.dumps(value, ensure_ascii=False)
            )
        now = str(time.time_ns())
        span = dict(
            traceId=trace,
            spanId=span_id(event.get("tool_use_id") or name),
            name=name,
            kind=1,
            startTimeUnixNano=now,
            endTimeUnixNano=now,
            attributes=[
                dict(key=k, value=dict(stringValue=v)) for k, v in attrs.items()
            ],
        )
        if name != "codex.user":
            span["parentSpanId"] = span_id("codex.user")
        spans = [span]
        if kind == "Stop":
            delivery = forwarded_prompt(event)
            if delivery:
                prompt, delivered_at = delivery
                root = dict(
                    span,
                    name="codex.user",
                    spanId=span_id("codex.user"),
                    startTimeUnixNano=delivered_at,
                    endTimeUnixNano=delivered_at,
                )
                del root["parentSpanId"]
                root["attributes"] = [
                    a
                    for a in span["attributes"]
                    if a["key"] != "langfuse.observation.output"
                ]
                root["attributes"].append(
                    dict(
                        key="langfuse.observation.input", value=dict(stringValue=prompt)
                    )
                )
                spans.insert(0, root)
        result = client.post(
            "/api/public/otel/v1/traces",
            headers={"x-langfuse-ingestion-version": "4"},
            json={
                "resourceSpans": [
                    {"scopeSpans": [{"scope": {"name": "codex.hooks"}, "spans": spans}]}
                ]
            },
        )
        result.raise_for_status()


if __name__ == "__main__":
    handle(json.load(sys.stdin))
    print("{}")
