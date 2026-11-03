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
"""Exercise context through built-in Actions, including interleaved chat rounds."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from flink_agents.api.chat_message import ChatMessage, ToolCallBlock
from flink_agents.api.chat_result import ChatResult
from flink_agents.api.events.chat_event import ChatRequestEvent, ChatResponseEvent
from flink_agents.api.events.context_retrieval_event import ContextRetrievalRequestEvent
from flink_agents.api.events.event import Event
from flink_agents.api.events.tool_event import ToolRequestEvent, ToolResponseEvent
from flink_agents.api.tools.tool_response import ToolResponse
from flink_agents.plan.actions.chat_model_action import (
    process_chat_request_or_tool_response,
)
from flink_agents.plan.actions.context_retrieval_action import (
    process_context_retrieval_request,
)
from flink_agents.plan.actions.decision_model_action import process_decision_request
from flink_agents.plan.actions.tool_call_action import process_tool_request
from flink_agents.plan.configuration import AgentConfiguration
from flink_agents.plan.tests.actions.test_chat_model_action_retry import (
    _create_mock_runner_context,
)
from flink_agents.plan.tests.actions.test_decision_model_action import (
    _context,
    _Model,
    _request,
    _result,
)


@pytest.mark.parametrize("failed", [False, True])
def test_interleaved_chat_rounds_restore_context_and_finish_in_reverse_order(
    failed,
) -> None:
    model = MagicMock()
    tool_result = ChatResult(
        message=ChatMessage.assistant(
            [ToolCallBlock(call_id="call", name="tool", input={})]
        )
    )
    model.chat.side_effect = [
        tool_result,
        tool_result,
        tool_result,
        ChatResult(message=ChatMessage.assistant("B done")),
        RuntimeError("A failed")
        if failed
        else ChatResult(message=ChatMessage.assistant("A done")),
    ]
    ctx, events, _, memory = _create_mock_runner_context(model, max_retries=0)
    requests = [
        ChatRequestEvent(
            "model",
            [ChatMessage.user("same prompt")],
            request_context={"task": task, "nested": {"items": [1, None]}},
        )
        for task in ("A", "B")
    ]
    for request in requests:
        request.set_attachment("not_for_response", "payload")
        asyncio.run(process_chat_request_or_tool_response(request, ctx))
    tools = list(events)
    assert all(isinstance(event, ToolRequestEvent) for event in tools)
    assert all(event.request_context is None for event in tools)
    # Materialize the parked state as JSON, then resume from reconstructed events.
    # This verifies the continuation's persisted data rather than a local variable.
    memory._store = json.loads(json.dumps(memory._store))

    def reply(tool_request) -> None:
        event = ToolResponseEvent(
            tool_request.id,
            {"call": ToolResponse.text("result")},
            request_context={"task": "tool-only"},
        )
        asyncio.run(
            process_chat_request_or_tool_response(
                Event.from_json(event.model_dump_json()), ctx
            )
        )

    reply(tools[1])  # B needs a second tool round.
    reply(events[-1])  # B completes before A.
    reply(tools[0])
    responses = [event for event in events if isinstance(event, ChatResponseEvent)]
    assert [event.request_id for event in responses] == [requests[1].id, requests[0].id]
    assert [event.request_context["task"] for event in responses] == ["B", "A"]
    assert responses[-1].is_failed == failed
    assert all(not event.attachments for event in responses)
    assert memory.get("_USER_REQUEST_CONTEXT") == {}
    for call in model.chat.call_args_list:
        assert "request_context" not in call.kwargs


@pytest.mark.parametrize("failed", [False, True])
def test_decision_context_survives_retry_and_terminal_outcome(failed) -> None:
    outcomes = [RuntimeError("retry"), RuntimeError("failed") if failed else _result()]
    ctx, _ = _context(_Model(outcomes), retries=1, interval=0)
    request = _request()
    request.set_request_context({"task": "decision"})
    request.set_attachment("unrelated", "payload")
    asyncio.run(process_decision_request(request, ctx))
    response = ctx.send_event.call_args.args[0]
    assert response.request_id == request.id
    assert response.request_context == request.request_context
    assert response.is_failed == failed
    assert response.attachments == {}


def test_tool_failure_and_retrieval_preserve_only_explicit_context() -> None:
    ctx = MagicMock()
    ctx.config = AgentConfiguration({"tool-call.async": False, "rag.async": False})
    ctx.get_resource.side_effect = RuntimeError("tool unavailable")
    request = ToolRequestEvent(
        "model",
        [ToolCallBlock(call_id="call", name="missing", input={})],
        request_context={"task": "tool"},
    )
    request.set_attachment("unrelated", "payload")
    asyncio.run(process_tool_request(request, ctx))
    response = ctx.send_event.call_args.args[0]
    assert response.request_id == request.id
    assert response.request_context == request.request_context
    assert not response.success["call"]
    assert response.attachments == {}

    store = MagicMock()
    store.query.return_value = SimpleNamespace(documents=[])
    ctx.get_resource.side_effect = None
    ctx.get_resource.return_value = store
    ctx.durable_execute.side_effect = lambda fn, *args: fn(*args)
    request = ContextRetrievalRequestEvent(
        "query", "store", request_context={"task": "rag"}
    )
    asyncio.run(process_context_retrieval_request(request, ctx))
    response = ctx.send_event.call_args.args[0]
    assert response.request_id == request.id
    assert response.request_context == request.request_context
    assert response.attachments == {}
    assert store.query.call_args.args[0].query_text == "query"
