from typing import Any

import dataclasses
import uuid
import datetime

from saturn_engine.utils import utcnow




@dataclasses.dataclass
class TopicMessage:
    args: dict[str, Any]
    #enqueued_at: datetime.datetime = dataclasses.field(default_factory=lambda: utcnow())
    id: str = dataclasses.field(default_factory=lambda: str(uuid.uuid4()))

    def extend(self, args: dict[str, object]) -> "TopicMessage":
        return self.__class__(id=self.id, args=args | self.args)
