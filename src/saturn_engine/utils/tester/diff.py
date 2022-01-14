from typing import Any

import shutil


def get_diff(*, expected: Any, got: Any) -> str:
    """
    Inspired from pytest-icdiff.
    Licensed under the public domain.
    """
    try:
        import icdiff  # type: ignore
        from pprintpp import pformat  # type: ignore
    except ImportError:
        import pprint

        return f"expected: {pprint.pformat(expected)}\ngot: {pprint.pformat(got)}"

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

    header: str = "Expected" + (" " * int(half_cols)) + "Got"

    icdiff_lines: list[str] = [header]
    icdiff_lines.extend(
        list(differ.make_table(pretty_left, pretty_right, context=True))
    )

    return "\n".join([color_off + line for line in icdiff_lines])
