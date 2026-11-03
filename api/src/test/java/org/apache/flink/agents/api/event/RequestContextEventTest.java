/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.agents.api.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.agents.api.Event;
import org.apache.flink.agents.api.decision.DecisionRequest;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RequestContextEventTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static List<RequestResponseEvent> events() {
        return List.of(
                new ChatRequestEvent("model", List.of()),
                ChatResponseEvent.failed(UUID.randomUUID(), "failure", 0, 0),
                new ToolRequestEvent("model", List.of()),
                new ToolResponseEvent(UUID.randomUUID(), Map.of(), Map.of(), Map.of()),
                new ContextRetrievalRequestEvent("query", "store"),
                new ContextRetrievalResponseEvent(UUID.randomUUID(), "query", List.of()),
                new DecisionRequestEvent(
                        "model",
                        new DecisionRequest(
                                "review",
                                Map.of("q", DecisionRequest.Question.predicate("Approve?")))),
                DecisionResponseEvent.failed(UUID.randomUUID(), "failure", 0, 0));
    }

    @Test
    @SuppressWarnings("unchecked")
    void contextIsOptionalCopiedAndRestored() throws Exception {
        for (RequestResponseEvent event : events()) {
            assertThat(event.getRequestContext()).isNull();
            assertThat(event.getAttributes()).doesNotContainKey("request_context");
            List<Object> items = new ArrayList<>(Arrays.asList(1, true, null, 0.5));
            Map<String, Object> context = new HashMap<>(Map.of("task", "摘要", "items", items));
            event.setRequestContext(context);
            items.add("caller mutation");
            ((List<Object>) event.getRequestContext().get("items")).add("reader mutation");
            assertThat((List<?>) event.getRequestContext().get("items")).hasSize(4);
            event.setAttachment("unrelated", "payload");
            RequestResponseEvent restored =
                    (RequestResponseEvent) Event.fromJson(MAPPER.writeValueAsString(event));
            assertThat(restored).isExactlyInstanceOf(event.getClass());
            assertThat(restored.getId()).isEqualTo(event.getId());
            assertThat(restored.getRequestContext()).isEqualTo(event.getRequestContext());
            assertThat(restored.getAttachments()).isEqualTo(event.getAttachments());
            restored.setRequestContext(Map.of());
            assertThat(event.getRequestContext()).containsEntry("task", "摘要");
            restored.setRequestContext(null);
            assertThat(restored.getAttributes()).doesNotContainKey("request_context");
        }
    }

    @Test
    void rejectsInvalidJsonAndCycles() {
        for (RequestResponseEvent event : events()) {
            for (Object invalid :
                    List.of(
                            new Object(),
                            Double.NaN,
                            Double.POSITIVE_INFINITY,
                            Map.of(1, "value"),
                            new byte[] {1})) {
                assertThatThrownBy(() -> event.setRequestContext(Map.of("key", invalid)))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("request_context");
            }
            Map<String, Object> cycle = new HashMap<>();
            cycle.put("cycle", cycle);
            assertThatThrownBy(() -> event.setRequestContext(cycle))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("cycles");
        }
    }
}
