import typing
from typing import Any
from typing import Optional

import json
from datetime import datetime
from datetime import timezone

from sqlalchemy import types
from sqlalchemy.engine.interfaces import Dialect

from ..utils import default_utc


class UTCfyDateTime(types.TypeDecorator[datetime]):
    impl = types.DATETIME

    cache_ok = True

    def process_bind_param(
        self, value: Optional[datetime], dialect: Dialect
    ) -> Optional[Any]:
        if value is not None:
            if value.tzinfo is None:
                value = default_utc(value)
            value = value.astimezone(timezone.utc)
        return value

    def process_result_value(
        self, value: Optional[Any], dialect: Dialect
    ) -> Optional[datetime]:
        if value is not None and value.tzinfo is None:
            value = default_utc(value)
        return value


UTCDateTime = types.DateTime(timezone=True).with_variant(UTCfyDateTime, "sqlite")


class StringyJSON(types.TypeDecorator[dict[str, Any]]):
    """Stores and retrieves JSON as TEXT."""

    impl = types.TEXT

    def process_bind_param(
        self, value: Optional[dict[str, Any]], dialect: Dialect
    ) -> Optional[typing.Text]:
        if value is not None:
            return json.dumps(value)
        return None

    def process_result_value(
        self, value: Optional[Any], dialect: Dialect
    ) -> Optional[dict[str, Any]]:
        if value is not None:
            value = json.loads(value)
        return value


JSON = types.JSON().with_variant(StringyJSON, "sqlite")
