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
import math
from typing import Any

from typing_extensions import Self

from flink_agents.api.events.event import Event


def _copy_context(context: Any) -> dict[str, Any] | None:
    if context is not None and type(context) is not dict:
        msg = "request_context must be a JSON object"
        raise ValueError(msg)
    visiting: set[int] = set()

    def copy(value: Any) -> Any:
        if value is None or type(value) in (str, bool, int):
            return value
        if type(value) is float and math.isfinite(value):
            return value
        if type(value) in (dict, list):
            identity = id(value)
            if identity in visiting:
                msg = "request_context must not contain cycles"
                raise ValueError(msg)
            visiting.add(identity)
            try:
                if type(value) is dict:
                    if any(type(key) is not str for key in value):
                        msg = "request_context requires string keys"
                        raise ValueError(msg)
                    return {key: copy(item) for key, item in value.items()}
                return [copy(item) for item in value]
            finally:
                visiting.remove(identity)
        msg = "request_context must contain only finite JSON values"
        raise ValueError(msg)

    return copy(context)


class RequestResponseEvent(Event):
    """An event carrying optional opaque application context as a JSON object.

    Context lives inline in attributes and is independent of attachments and
    model/tool inputs. Setting and reading context copies all nested values.
    """

    def __init__(
        self, *, request_context: dict[str, Any] | None = None, **data: Any
    ) -> None:
        """Snapshot context when constructing a request or response."""
        super().__init__(**data)
        self.set_request_context(
            request_context
            if request_context is not None
            else self.attributes.get("request_context")
        )

    @property
    def request_context(self) -> dict[str, Any] | None:
        """Return a defensive copy of the caller's context, or None."""
        return _copy_context(self.attributes.get("request_context"))

    def set_request_context(self, context: dict[str, Any] | None) -> None:
        """Snapshot JSON context; None removes it."""
        copied = _copy_context(context)
        if copied is None:
            self.attributes.pop("request_context", None)
        else:
            self.attributes["request_context"] = copied

    def reconstruct_from(self, source: Event) -> Self:
        """Preserve context when reconstructing the same event occurrence."""
        result = super().reconstruct_from(source)
        result.attributes = dict(result.attributes)
        result.set_request_context(source.attributes.get("request_context"))
        return result
