import unittest

from langchain_core.language_models.fake_chat_models import FakeListChatModel
from langsmith import tracing_context
from pydantic import ValidationError

from orbita.workflow import Request, build_graph


class WorkflowTests(unittest.TestCase):
    def test_graph_returns_model_text(self):
        with tracing_context(enabled=False):
            result = build_graph(FakeListChatModel(responses=["answer"])).invoke(
                {"prompt": "question", "answer": ""}
            )
        self.assertEqual(result, {"prompt": "question", "answer": "answer"})

    def test_invalid_request_is_rejected(self):
        for data in (
            {"prompt": " "},
            {"prompt": 42},
            {"prompt": "x", "extra": True},
            {"prompt": "x" * 4001},
        ):
            with self.subTest(data_type=type(data["prompt"]).__name__):
                with self.assertRaises(ValidationError):
                    Request.model_validate(data)

    def test_blank_model_response_is_rejected(self):
        with tracing_context(enabled=False), self.assertRaises(ValidationError):
            build_graph(FakeListChatModel(responses=[" "])).invoke(
                {"prompt": "question", "answer": ""}
            )
