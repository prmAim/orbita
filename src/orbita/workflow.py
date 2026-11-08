"""Small graph with injected model; no configuration or network setup here."""

from typing import TypedDict

from langchain_core.language_models import BaseChatModel
from langgraph.graph import END, START, StateGraph
from pydantic import BaseModel, ConfigDict, Field


class Request(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", str_strip_whitespace=True)
    prompt: str = Field(min_length=1, max_length=4000)


class Result(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", str_strip_whitespace=True)
    answer: str = Field(min_length=1)


class State(TypedDict):
    prompt: str
    answer: str


def build_graph(model: BaseChatModel):
    def respond(state: State) -> dict[str, str]:
        request = Request.model_validate({"prompt": state["prompt"]})
        response = Result(answer=model.invoke(request.prompt).text)
        return {"answer": response.answer}

    builder = StateGraph(State)
    builder.add_node("respond", respond)
    builder.add_edge(START, "respond")
    builder.add_edge("respond", END)
    return builder.compile()
