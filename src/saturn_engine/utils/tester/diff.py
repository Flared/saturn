import shutil
from typing import Any


def print_diff(*, expected: Any, got: Any) -> None:
    """
    Inspired from pyest-icdiff.
    Licensed under the public domain.
    """
    try:
        import icdiff  # type: ignore
        from pprintpp import pformat  # type: ignore
    except ImportError:
        import pprint

        print("expected:", pprint.pformat(expected))
        print("got:", pprint.pformat(got))
        return

    COLS: int = shutil.get_terminal_size().columns
    MARGIN_L: int = 10
    GUTTER: int = 2
    MARGINS: int = MARGIN_L + GUTTER + 1

    half_cols: float = COLS / 2 - MARGINS

    pretty_left = pformat(expected, indent=2, width=half_cols).splitlines()
    pretty_right = pformat(got, indent=2, width=half_cols).splitlines()
    diff_cols = COLS - MARGINS

    if len(pretty_left) < 3 or len(pretty_right) < 3:
        # avoid small diffs far apart by smooshing them up to the left
        smallest_left = pformat(expected, indent=2, width=1).splitlines()
        smallest_right = pformat(got, indent=2, width=1).splitlines()
        max_side = max(len(line) + 1 for line in smallest_left + smallest_right)
        if (max_side * 2 + MARGINS) < COLS:
            diff_cols = max_side * 2 + GUTTER
            pretty_left = pformat(expected, indent=2, width=max_side).splitlines()
            pretty_right = pformat(got, indent=2, width=max_side).splitlines()

    differ = icdiff.ConsoleDiff(cols=diff_cols, tabsize=2)
    color_off = icdiff.color_codes["none"]

    icdiff_lines = list(differ.make_table(pretty_left, pretty_right, context=True))

    print("Expected" + (" " * int(half_cols)) + "Got")

    for line in [color_off + line for line in icdiff_lines]:
        print(line)
