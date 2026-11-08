"""Installation acceptance: offline by default, explicit local or live modes."""

import argparse
import asyncio
import logging
from contextlib import ExitStack
from pathlib import Path

import httpx
from langchain_core.language_models.fake_chat_models import FakeListChatModel
from langfuse.langchain import CallbackHandler
from langsmith import tracing_context

from orbita.config import ConfigurationError, Settings
from orbita.models import create_model
from orbita.observability import (
    CheckFailure,
    create_client,
    require_project,
    wait_for_result,
)
from orbita.workflow import build_graph


def check(settings: Settings, mode: str) -> None:
    if mode != "offline":
        settings.require_services(live=mode == "live")
    model = None
    client = None
    resources = ExitStack()
    try:
        if mode != "offline":
            # Only local Langfuse traffic bypasses system/environment proxies.
            transport = resources.enter_context(
                httpx.Client(trust_env=False, timeout=10)
            )
            client = create_client(settings, httpx_client=transport)
            resources.callback(client.shutdown)
            # Validate trace destination before spending any model tokens.
            require_project(client, settings.langfuse_project_name)
        model = (
            create_model(settings)
            if mode == "live"
            else FakeListChatModel(responses=["ORBITA_OK"])
        )
        trace_id = client.create_trace_id() if client else None
        callbacks = (
            [
                CallbackHandler(
                    public_key=settings.langfuse_public_key.get_secret_value(),
                    trace_context={"trace_id": trace_id},
                )
            ]
            if client
            else []
        )
        with tracing_context(enabled=False):
            result = build_graph(model).invoke(
                {"prompt": "Reply with exactly ORBITA_OK.", "answer": ""},
                config={
                    "callbacks": callbacks,
                    "run_name": f"orbita-{mode}-check",
                    "metadata": {"app_name": "orbita", "mode": mode},
                },
            )
        if client:
            wait_for_result(client, trace_id, settings.app_env, result)
            print(f"Trace: {client.get_trace_url(trace_id=trace_id)}")
        print(
            f"PASS: {mode}; graph and validated result"
            + ("; trace persisted" if client else "")
        )
    finally:
        try:
            resources.close()
        finally:
            if mode == "live" and model is not None:
                model.root_client.close()
                asyncio.run(model.root_async_client.close())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode", choices=("offline", "local", "live"), default="offline"
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=Path(".env"),
        help="Configuration file relative to working directory",
    )
    args = parser.parse_args()
    logging.disable(logging.CRITICAL)
    try:
        settings = Settings(_env_file=args.env_file, _env_file_encoding="utf-8")
        check(settings, args.mode)
    except (ConfigurationError, CheckFailure) as error:
        raise SystemExit(f"FAIL: {error}") from None
    except Exception as error:
        status = getattr(error, "status_code", None)
        raise SystemExit(
            f"FAIL: {type(error).__name__}"
            + (f" (HTTP {status})" if status else "")
            + "; check service availability, keys and model access."
        ) from None


if __name__ == "__main__":
    main()
