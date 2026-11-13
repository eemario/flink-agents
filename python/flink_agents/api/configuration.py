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
import re
from abc import ABC, abstractmethod
from copy import deepcopy
from datetime import timedelta
from typing import Any, Type


class ConfigOption:
    """A configuration option defines a configuration key with its type and default
    value.

    Args:
        key: The configuration key name
        config_type: The expected type of the configuration value (int, float, str,
        or bool)
        default: The default value for this configuration option
    """

    def __init__(
        self, key: str, config_type: Type[Any], default: Any | None = None
    ) -> None:
        """Initialize a configuration option."""
        self._key = key
        self._type = config_type
        self._default_value = default

    def get_key(self) -> str:
        """Gets the configuration key."""
        return self._key

    def get_type(self) -> Type[Any]:
        """Returns the type of the configuration value."""
        return self._type

    def get_default_value(self) -> Any:
        """Return the default value without sharing mutable state between callers."""
        return deepcopy(self._default_value)


class WritableConfiguration(ABC):
    """Abstract base class providing write access to a configuration object.

    This class enables modification of configuration settings.
    """

    @abstractmethod
    def set_str(self, key: str, value: str) -> None:
        """Set the string configuration value using the key.

        Args:
            key: The configuration key to set
            value: The string value to set for the key
        """

    @abstractmethod
    def set_int(self, key: str, value: int) -> None:
        """Set the int configuration value using the key.

        Args:
            key: The configuration key to set
            value: The integer value to set for the key
        """

    @abstractmethod
    def set_float(self, key: str, value: float) -> None:
        """Set the float configuration value using the key.

        Args:
            key: The configuration key to set
            value: The float value to set for the key
        """

    @abstractmethod
    def set_bool(self, key: str, value: bool) -> None:  # noqa: FBT001
        """Set the boolean configuration value using the key.

        Args:
            key: The configuration key to set
            value: The boolean value to set for the key
        """

    @abstractmethod
    def set(self, option: ConfigOption, value: Any) -> None:
        """Set the configuration value using the ConfigOption.

        Args:
            option: The config option to set
            value: The value to set for the key
        """


class ReadableConfiguration(ABC):
    """Abstract base class providing read access to a configuration object.

    This class enables retrieval of configuration settings.
    """

    @abstractmethod
    def get_int(self, key: str, default: int | None = None) -> int:
        """Get the int configuration value by key.

        Args:
            key: The configuration key to retrieve
            default: The default value to return if key is not found

        Returns:
            The integer value associated with the key or the default value
        """

    @abstractmethod
    def get_float(self, key: str, default: float | None = None) -> float:
        """Get the float configuration value by key.

        Args:
            key: The configuration key to retrieve
            default: The default value to return if key is not found

        Returns:
            The float value associated with the key or the default value
        """

    @abstractmethod
    def get_bool(self, key: str, default: bool | None = None) -> bool:
        """Get the boolean configuration value by key.

        Args:
            key: The configuration key to retrieve
            default: The default value to return if key is not found

        Returns:
            The boolean value associated with the key or the default value
        """

    @abstractmethod
    def get_str(self, key: str, default: str | None = None) -> str:
        """Get the string configuration value by key.

        Args:
            key: The configuration key to retrieve
            default: The default value to return if key is not found

        Returns:
            The string value associated with the key or the default value
        """

    @abstractmethod
    def get(self, option: ConfigOption) -> Any:
        """Get the configuration value by ConfigOption.

        Args:
            option: The metadata of the option to read

        Returns:
            The value of the given option
        """


class Configuration(WritableConfiguration, ReadableConfiguration, ABC):
    """A configuration object that provides both read and write access to a
    configuration object.
    """


def parse_duration(value: object) -> timedelta:
    """Parse a duration without imposing a configuration-specific limit.

    Unitless strings use milliseconds. Sub-microsecond values are rejected
    because timedelta cannot represent them without losing precision.
    """
    if not isinstance(value, str | timedelta):
        msg = f"Expected a timedelta or duration string, got {type(value).__name__}"
        raise TypeError(msg)
    if isinstance(value, str):
        match = re.fullmatch(r"\s*([+-]?\d+)\s*([a-zA-Zµ]*)\s*", value)
        units = {
            "": 1000,
            "micro": 1,
            "µs": 1,
            "microsecond": 1,
            "ms": 1000,
            "millisecond": 1000,
            "milli": 1000,
            "s": 10**6,
            "sec": 10**6,
            "second": 10**6,
            "m": 60 * 10**6,
            "min": 60 * 10**6,
            "minute": 60 * 10**6,
            "h": 3600 * 10**6,
            "hour": 3600 * 10**6,
            "d": 86400 * 10**6,
            "day": 86400 * 10**6,
        }
        if match:
            name = match[2].lower()
            unit = units.get(name, units.get(name.removesuffix("s")))
            if (
                name in {"ns", "nano", "nanos", "nanosecond", "nanoseconds"}
                and int(match[1]) % 1000 == 0
            ):
                value = timedelta(microseconds=int(match[1]) // 1000)
            elif unit:
                value = timedelta(microseconds=int(match[1]) * unit)
    if isinstance(value, timedelta):
        return value
    msg = f"Invalid duration: {value!r}; expected a timedelta or duration string"
    raise ValueError(msg)
