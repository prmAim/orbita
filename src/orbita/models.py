"""OpenAI adapter; provider credentials and address stay together."""

from langchain_openai import ChatOpenAI

from orbita.config import ConfigurationError, Settings


def create_model(settings: Settings) -> ChatOpenAI:
    if not settings.openai_api_key.get_secret_value().strip():
        raise ConfigurationError("Set OPENAI_API_KEY in the local environment file")
    return ChatOpenAI(
        model=settings.llm_model,
        api_key=settings.openai_api_key,
        base_url="https://api.openai.com/v1",
        use_responses_api=True,
        store=False,
        reasoning_effort="none",
        max_tokens=256,
        timeout=30,
        max_retries=0,
    )
