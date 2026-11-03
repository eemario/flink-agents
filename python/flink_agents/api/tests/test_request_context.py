################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
#################################################################################
"""Request context admission, copying and event serialization contracts."""

from uuid import uuid4

import pytest

from flink_agents.api.decision_models import DecisionRequest, PredicateQuestion
from flink_agents.api.events.chat_event import ChatRequestEvent, ChatResponseEvent
from flink_agents.api.events.context_retrieval_event import (
    ContextRetrievalRequestEvent,
    ContextRetrievalResponseEvent,
)
from flink_agents.api.events.decision_event import (
    DecisionRequestEvent,
    DecisionResponseEvent,
)
from flink_agents.api.events.event import Event
from flink_agents.api.events.tool_event import ToolRequestEvent, ToolResponseEvent


def _events() -> list:
    return [
        ChatRequestEvent("model", []),
        ChatResponseEvent.failed(uuid4(), "failure"),
        ToolRequestEvent("model", []),
        ToolResponseEvent(uuid4(), {}),
        ContextRetrievalRequestEvent("query", "store"),
        ContextRetrievalResponseEvent(uuid4(), "query", []),
        DecisionRequestEvent(
            "model",
            DecisionRequest(
                input="review",
                questions={"q": PredicateQuestion(instructions="Approve?")},
            ),
        ),
        DecisionResponseEvent.failed(uuid4(), "failure"),
    ]


@pytest.mark.parametrize("event", _events(), ids=lambda event: event.type)
def test_context_is_optional_copied_and_restored(event) -> None:
    assert event.request_context is None
    assert "request_context" not in event.attributes
    context = {"task": "摘要", "nested": {"items": [1, True, None, 0.5]}}
    event.set_request_context(context)
    context["nested"]["items"].append("caller mutation")
    view = event.request_context
    view["nested"]["items"].append("reader mutation")
    assert event.request_context["nested"]["items"] == [1, True, None, 0.5]
    event.set_attachment("unrelated", "payload")
    restored = Event.from_json(event.model_dump_json())
    assert type(restored) is type(event)
    assert restored.id == event.id
    assert restored.request_context == event.request_context
    assert restored.attachments == event.attachments
    restored.set_request_context({})
    assert event.request_context["task"] == "摘要"
    assert Event.from_json(restored.model_dump_json()).request_context == {}
    restored.set_request_context(None)
    assert "request_context" not in restored.attributes


@pytest.mark.parametrize(
    "context",
    [
        [],
        "label",
        {"key": object()},
        {"key": float("nan")},
        {"key": float("inf")},
        {"key": {1: "value"}},
        {"key": b"bytes"},
    ],
)
def test_invalid_context_rejected_by_api_and_reconstruction(context) -> None:
    for event in _events():
        with pytest.raises(ValueError, match="request_context"):
            event.set_request_context(context)
        raw = Event(type=event.type, attributes=event.attributes)
        raw.attributes["request_context"] = context
        with pytest.raises((ValueError, TypeError)):
            type(event).from_event(raw)


def test_constructor_snapshots_context_and_rejects_cycles() -> None:
    context = {"task": "summary"}
    request = ChatRequestEvent("model", [], request_context=context)
    context["task"] = "changed"
    assert request.request_context == {"task": "summary"}
    context["cycle"] = context
    with pytest.raises(ValueError, match="cycles"):
        request.set_request_context(context)
