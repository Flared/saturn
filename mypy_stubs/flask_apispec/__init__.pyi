from typing import Any
from typing import Callable
from typing import TypeVar

F = TypeVar("F", bound=Callable)

def use_kwargs(*args: Any, **kwargs: Any) -> Callable[[F], F]: ...
def marshal_with(*args: Any, **kwargs: Any) -> Callable[[F], F]: ...
