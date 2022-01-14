from typing import ClassVar
from typing import Optional

import dataclasses


@dataclasses.dataclass(eq=False)
class Resource:
    name: str
    typename: ClassVar[Optional[str]] = None

    @classmethod
    def _typename(cls) -> str:
        return cls.typename or cls.__name__
