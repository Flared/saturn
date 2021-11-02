import dataclasses
from typing import ClassVar
from typing import Optional


@dataclasses.dataclass(eq=False)
class Resource:
    id: str
    typename: ClassVar[Optional[str]] = None

    @classmethod
    def _typename(cls) -> str:
        return cls.typename or cls.__name__
