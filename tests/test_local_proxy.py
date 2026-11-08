"""Exercise the CLI's real Langfuse API transport against a local proxy trap."""

import json
import os
import threading
import unittest
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import Mock, patch
from uuid import uuid4

from orbita.cli import check
from orbita.config import Settings


@contextmanager
def server(status, body):
    paths = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            paths.append(self.path)
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(body).encode())

        def log_message(self, *_args):
            pass

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    worker = threading.Thread(target=httpd.serve_forever, daemon=True)
    worker.start()
    try:
        yield f"http://127.0.0.1:{httpd.server_port}", paths
    finally:
        httpd.shutdown()
        httpd.server_close()
        worker.join(timeout=5)


class LocalProxyTests(unittest.TestCase):
    def test_local_api_bypasses_system_proxy_without_changing_environment(self):
        projects = {
            "data": [
                {
                    "id": "orbita",
                    "name": "orbita",
                    "organization": {"id": "test", "name": "test"},
                    "metadata": {},
                }
            ]
        }
        with server(200, projects) as (origin, origin_paths):
            with server(503, {}) as (proxy, proxy_paths):
                with patch.dict(os.environ, {"OTEL_SDK_DISABLED": "true"}):
                    # A random loopback port keeps the test independent of real Langfuse.
                    settings = Settings(
                        _env_file=None,
                        langfuse_public_key=f"test-{uuid4()}",
                        langfuse_secret_key="test-only",
                    ).model_copy(update={"langfuse_base_url": origin})
                    original_environment = dict(os.environ)
                    with (
                        patch("httpx._utils.getproxies", return_value={"http": proxy}),
                        patch(
                            "orbita.cli.build_graph",
                            return_value=Mock(
                                invoke=Mock(return_value={"answer": "ORBITA_OK"})
                            ),
                        ),
                        patch("orbita.cli.CallbackHandler"),
                        patch("orbita.cli.wait_for_result"),
                    ):
                        check(settings, "local")
                    self.assertEqual(dict(os.environ), original_environment)
                self.assertTrue(origin_paths)
                self.assertEqual(set(origin_paths), {"/api/public/projects"})
                self.assertEqual(proxy_paths, [])
