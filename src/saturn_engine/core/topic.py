import dataclasses
import uuid


@dataclasses.dataclass
class TopicMessage:
    args: dict[str, object]
    id: str = dataclasses.field(default_factory=lambda: str(uuid.uuid4()))

    def extend(self, args: dict[str, object]) -> "TopicMessage":
        return self.__class__(id=self.id, args=args | self.args)
