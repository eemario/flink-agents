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
package org.apache.flink.agents.plan.actions;

import org.apache.flink.agents.api.agents.AgentExecutionOptions;
import org.apache.flink.agents.api.context.DurableCallable;
import org.apache.flink.agents.api.context.RunnerContext;
import org.apache.flink.agents.api.event.ContextRetrievalRequestEvent;
import org.apache.flink.agents.api.event.ContextRetrievalResponseEvent;
import org.apache.flink.agents.api.resource.ResourceType;
import org.apache.flink.agents.api.vectorstores.BaseVectorStore;
import org.apache.flink.agents.api.vectorstores.VectorStoreQuery;
import org.apache.flink.agents.api.vectorstores.VectorStoreQueryResult;
import org.apache.flink.agents.plan.AgentConfiguration;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ContextRetrievalActionTest {
    @Test
    void returnsOnlyExplicitRequestContext() throws Exception {
        RunnerContext ctx = mock(RunnerContext.class);
        BaseVectorStore store = mock(BaseVectorStore.class);
        AgentConfiguration config = new AgentConfiguration();
        config.set(AgentExecutionOptions.RAG_ASYNC, false);
        when(ctx.getConfig()).thenReturn(config);
        when(ctx.getResource("store", ResourceType.VECTOR_STORE)).thenReturn(store);
        when(store.query(any())).thenReturn(new VectorStoreQueryResult(List.of()));
        when(ctx.durableExecute(any()))
                .thenAnswer(inv -> inv.<DurableCallable<?>>getArgument(0).call());
        ContextRetrievalRequestEvent request = new ContextRetrievalRequestEvent("query", "store");
        request.setRequestContext(Map.of("task", "retrieval"));
        request.setAttachment("unrelated", "payload");
        ContextRetrievalAction.processContextRetrievalRequest(request, ctx);
        ArgumentCaptor<ContextRetrievalResponseEvent> response =
                ArgumentCaptor.forClass(ContextRetrievalResponseEvent.class);
        verify(ctx).sendEvent(response.capture());
        assertThat(response.getValue().getRequestId()).isEqualTo(request.getId());
        assertThat(response.getValue().getRequestContext()).isEqualTo(request.getRequestContext());
        assertThat(response.getValue().getAttachments()).isEmpty();
        ArgumentCaptor<VectorStoreQuery> query = ArgumentCaptor.forClass(VectorStoreQuery.class);
        verify(store).query(query.capture());
        assertThat(query.getValue().getQueryText()).isEqualTo("query");
    }
}
