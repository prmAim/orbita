"""Local tracing with project identity and persisted-result verification."""

import json
import time

import httpx
from langfuse import Langfuse

from orbita.config import Settings


class CheckFailure(RuntimeError):
    """Safe diagnostic authored by the application, without provider payloads."""


def create_client(settings: Settings, *, httpx_client: httpx.Client) -> Langfuse:
    return Langfuse(
        public_key=settings.langfuse_public_key.get_secret_value(),
        secret_key=settings.langfuse_secret_key.get_secret_value(),
        base_url=settings.langfuse_base_url,
        environment=settings.app_env,
        httpx_client=httpx_client,
        timeout=10,
        flush_at=1,
    )


def require_project(client: Langfuse, name: str) -> None:
    projects = client.api.projects.get().data
    if len(projects) != 1 or projects[0].name != name:
        raise CheckFailure("Langfuse keys do not belong to the expected project")


def wait_for_result(
    client: Langfuse, trace_id: str, environment: str, result: dict
) -> None:
    client.flush()
    deadline = time.monotonic() + 45
    while True:
        rows = client.api.observations.get_many(
            trace_id=trace_id,
            environment=environment,
            fields="core,basic,io,model",
            limit=100,
        ).data
        matched = any(
            row.is_root_observation and row.output and json.loads(row.output) == result
            for row in rows
        )
        if matched and any(row.type == "GENERATION" for row in rows):
            return
        if time.monotonic() >= deadline:
            raise CheckFailure(
                f"Trace {trace_id} was not fully persisted within 45 seconds"
            )
        time.sleep(2)
