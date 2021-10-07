from datetime import datetime
from datetime import timezone
from typing import TYPE_CHECKING
from typing import Any
from typing import Optional

from sqlalchemy import types
from sqlalchemy.engine.interfaces import Dialect


def default_utc(date: datetime) -> datetime:
    if date.tzinfo is None:
        return date.replace(tzinfo=timezone.utc)
    return date


if TYPE_CHECKING:
    UTCfyDateTimeBase = types.TypeDecorator[datetime]
else:
    UTCfyDateTimeBase = types.TypeDecorator


class UTCfyDateTime(UTCfyDateTimeBase):
    impl = types.DATETIME

    def process_bind_param(
        self,
        value: Optional[datetime],
        dialect: Dialect,
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


UTCDateTime = types.DateTime(timezone=True).with_variant(
    UTCfyDateTime(), "sqlite"  # type: ignore
)
