import os
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from pydantic import ValidationError

from orbita.cli import check
from orbita.config import ConfigurationError, Settings
from orbita.models import create_model
from orbita.observability import CheckFailure, wait_for_result


class ConfigurationTests(unittest.TestCase):
    def settings(self, **changes):
        with patch.dict(os.environ, {}, clear=True):
            return Settings(_env_file=None, **changes)

    def test_offline_needs_no_keys_or_clients(self):
        with (
            patch("orbita.cli.create_client") as client,
            patch("orbita.cli.create_model") as model,
        ):
            check(self.settings(), "offline")
        client.assert_not_called()
        model.assert_not_called()

    def test_live_missing_key_fails_before_network(self):
        settings = self.settings(
            langfuse_public_key="public-test", langfuse_secret_key="secret-test"
        )
        with patch("orbita.cli.create_client") as client:
            with self.assertRaisesRegex(ConfigurationError, "OPENAI_API_KEY"):
                check(settings, "live")
        client.assert_not_called()

    def test_wrong_project_rejected_before_model_creation(self):
        settings = self.settings(
            openai_api_key="model-test",
            langfuse_public_key="public-test",
            langfuse_secret_key="secret-test",
        )
        client = Mock()
        client.api.projects.get.return_value.data = [
            SimpleNamespace(name="aim_setupaiv2")
        ]
        with (
            patch("orbita.cli.create_client", return_value=client),
            patch("orbita.cli.create_model") as model,
        ):
            with self.assertRaises(CheckFailure):
                check(settings, "live")
        model.assert_not_called()
        client.shutdown.assert_called_once()

    def test_cloud_endpoint_is_rejected_without_secret_in_error(self):
        with self.assertRaises(ValidationError) as error:
            self.settings(
                langfuse_base_url="https://cloud.langfuse.com",
                openai_api_key="private-test",
            )
        self.assertNotIn("private-test", str(error.exception))

    def test_model_uses_explicit_responses_and_bounded_call(self):
        settings = self.settings(openai_api_key="model-test")
        with patch("orbita.models.ChatOpenAI") as constructor:
            create_model(settings)
        args = constructor.call_args.kwargs
        self.assertEqual(args["base_url"], "https://api.openai.com/v1")
        self.assertTrue(args["use_responses_api"])
        self.assertFalse(args["store"])
        self.assertEqual(args["max_retries"], 0)
        self.assertEqual(args["max_tokens"], 256)

    def test_trace_waits_for_result_and_generation(self):
        client = Mock()
        root = SimpleNamespace(
            is_root_observation=True, output='{"answer":"ok"}', type="CHAIN"
        )
        generation = SimpleNamespace(
            is_root_observation=False, output=None, type="GENERATION"
        )
        client.api.observations.get_many.side_effect = [
            SimpleNamespace(data=[root]),
            SimpleNamespace(data=[root, generation]),
        ]
        with patch("orbita.observability.time.sleep") as sleep:
            wait_for_result(client, "trace-test", "development", {"answer": "ok"})
        sleep.assert_called_once_with(2)

    def test_trace_timeout_is_reported(self):
        client = Mock()
        client.api.observations.get_many.return_value.data = []
        with patch("orbita.observability.time.monotonic", side_effect=[0, 46]):
            with self.assertRaises(CheckFailure):
                wait_for_result(client, "trace-test", "development", {})
