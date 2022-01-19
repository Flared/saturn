from typing import Any

import shutil
from pprint import pformat

from . import lazy


@lazy()
def terminal_size() -> tuple[int, int]:
    return shutil.get_terminal_size()


def human_encode(data: Any, compact: bool = False) -> str:
    width, _ = terminal_size()
    return pformat(data, compact=compact, width=width)
