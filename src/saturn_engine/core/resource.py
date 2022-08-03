from typing import ClassVar
from typing import Optional

import dataclasses

from saturn_engine.utils import inspect


@dataclasses.dataclass(eq=False)
class Resource:
    name: str
    typename: ClassVar[Optional[str]] = None

    @classmethod
    def _typename(cls) -> str:
        return cls.typename or inspect.get_import_name(cls)
