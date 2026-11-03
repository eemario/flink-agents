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

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.agents.api.Event;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * An event in a request/response protocol carrying optional opaque application context.
 *
 * <p>Context is a JSON object stored inline in attributes. Setters and getters copy nested values;
 * attachments and model/tool inputs are independent of this context.
 */
public abstract class RequestResponseEvent extends Event {
    public static final String REQUEST_CONTEXT = "request_context";

    protected RequestResponseEvent(String type) {
        super(type);
    }

    protected RequestResponseEvent(UUID id, String type, Map<String, Object> attributes) {
        super(id, type, attributes);
        setRequestContextValue(getAttr(REQUEST_CONTEXT));
    }

    /** Returns a defensive copy of the caller's context, or null when absent. */
    @JsonIgnore
    @Nullable
    @SuppressWarnings("unchecked")
    public Map<String, Object> getRequestContext() {
        return (Map<String, Object>) copyContext(getAttr(REQUEST_CONTEXT));
    }

    /** Snapshots JSON context for this request or response; null removes it. */
    @JsonIgnore
    public void setRequestContext(@Nullable Map<String, Object> context) {
        setRequestContextValue(context);
    }

    private void setRequestContextValue(@Nullable Object context) {
        Object copied = copyContext(context);
        if (copied == null) {
            getAttributes().remove(REQUEST_CONTEXT);
        } else {
            setAttr(REQUEST_CONTEXT, copied);
        }
    }

    private static Object copyContext(@Nullable Object context) {
        if (context != null && !(context instanceof Map)) {
            throw new IllegalArgumentException("request_context must be a JSON object");
        }
        return copyJson(context, new IdentityHashMap<>());
    }

    private static Object copyJson(Object value, IdentityHashMap<Object, Boolean> visiting) {
        if (value == null
                || value instanceof String
                || value instanceof Boolean
                || value instanceof Byte
                || value instanceof Short
                || value instanceof Integer
                || value instanceof Long
                || value instanceof BigInteger
                || value instanceof BigDecimal
                || (value instanceof Double && Double.isFinite((Double) value))
                || (value instanceof Float && Float.isFinite((Float) value))) {
            return value;
        }
        if (value instanceof Map || value instanceof List) {
            if (visiting.put(value, Boolean.TRUE) != null) {
                throw new IllegalArgumentException("request_context must not contain cycles");
            }
            try {
                if (value instanceof Map) {
                    Map<String, Object> copy = new LinkedHashMap<>();
                    for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                        if (!(entry.getKey() instanceof String)) {
                            throw new IllegalArgumentException(
                                    "request_context requires string keys");
                        }
                        copy.put((String) entry.getKey(), copyJson(entry.getValue(), visiting));
                    }
                    return copy;
                }
                List<Object> copy = new ArrayList<>();
                for (Object item : (List<?>) value) {
                    copy.add(copyJson(item, visiting));
                }
                return copy;
            } finally {
                visiting.remove(value);
            }
        }
        throw new IllegalArgumentException("request_context must contain only finite JSON values");
    }
}
