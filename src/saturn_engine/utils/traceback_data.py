import typing as t

import collections
import dataclasses
import itertools
import linecache
import sys
import traceback
from collections.abc import Collection
from collections.abc import Mapping
from contextlib import suppress
from types import FrameType
from types import TracebackType

_RECURSIVE_CUTOFF = 3  # Also hardcoded in traceback.c.

_cause_message = (
    "\nThe above exception was the direct cause " "of the following exception:\n\n"
)

_context_message = (
    "\nDuring handling of the above exception, " "another exception occurred:\n\n"
)


def format_local(anyval: object, maxlen: int = 80) -> str:
    val: str = "<???>"
    with suppress(Exception):
        if isinstance(anyval, (str, bytes)):
            val = repr(anyval[:maxlen])
        elif isinstance(anyval, (int, float)):
            val = repr(anyval)
        elif isinstance(anyval, Mapping):
            val = ""
            if not isinstance(anyval, dict):
                val = f"{type(anyval)}"
            val += "{"
            vals = []
            valslen = len(val)
            for k, v in anyval.items():
                ks = format_local(k, maxlen=maxlen - valslen)
                valslen += len(ks)
                vs = format_local(v, maxlen=maxlen - valslen)
                valslen += len(vs) + 4
                vals.append(f"{ks}: {vs}")
                if valslen > maxlen:
                    return val + ", ".join(vals)
            return val + ", ".join(vals) + "}"
        elif isinstance(anyval, Collection):
            val = ""
            if not isinstance(anyval, list):
                val = f"{type(anyval)}"
            val += "["
            vals = []
            valslen = len(val)
            for v in anyval:
                vs = format_local(v, maxlen=maxlen - valslen)
                valslen += len(vs) + 2
                vals.append(vs)
                if valslen > maxlen:
                    return val + ", ".join(vals)
            return val + ", ".join(vals) + "]"
        else:
            val = str(type(anyval))

    if len(val) >= maxlen:
        if val[-1] == "'":
            val = val[:-1] + "<...>"
        else:
            val += "<...>"
    return val


@dataclasses.dataclass
class TracebackData:
    """Very alike traceback.TracebackException, with a few tweak to ensure
    100% serializability and some life-improvers:
     * `exc_type` is not a Type, but a descriptive str.
     * Add `exc_args`, a serializable tuple of `exc.args`.
     * Add `lines_[before|after]` to get context around frames lines.
    """

    exc_type: str
    exc_module: str
    exc_str: str
    stack: list["FrameData"]
    truncated: bool = False
    __cause__: t.Optional["TracebackData"] = None
    __context__: t.Optional["TracebackData"] = None
    __suppress_context__: bool = False
    __notes__: t.Optional[list[str]] = None

    @classmethod
    def from_exception(
        cls, exc: BaseException, *, _seen: t.Optional[set[int]] = None
    ) -> "TracebackData":
        return cls.from_exc_info(type(exc), exc, exc.__traceback__)

    @classmethod
    def from_exc_info(
        cls,
        exc_type: t.Type[BaseException],
        exc_value: BaseException,
        exc_traceback: t.Optional[TracebackType],
        *,
        _seen: t.Optional[set[int]] = None,
    ) -> "TracebackData":
        if _seen is None:
            _seen = set()
        _seen.add(id(exc_value))

        truncated = False
        cause = None
        context = None
        try:
            if exc_value.__cause__ and id(exc_value.__cause__) not in _seen:
                cause = cls.from_exception(exc_value.__cause__)
            if exc_value.__context__ and id(exc_value.__context__) not in _seen:
                context = cls.from_exception(exc_value.__context__)
        except RecursionError:
            # The recursive call to the constructors above
            # may result in a stack overflow for long exception chains,
            # so we must truncate.
            truncated = True

        stack = cls.extract_stack(traceback.walk_tb(exc_traceback))
        return cls(
            exc_type=exc_type.__qualname__,
            exc_module=exc_type.__module__,
            exc_str=_some_str(exc_value),
            stack=stack,
            truncated=truncated,
            __cause__=cause,
            __context__=context,
            __suppress_context__=exc_value.__suppress_context__,
            __notes__=exc_value.__notes__ if hasattr(exc_value, "__notes__") else None,
        )

    def __str__(self) -> str:
        return self.exc_str

    @staticmethod
    def extract_stack(
        frame_gen: t.Iterator[tuple[FrameType, int]], *, line_context: int = 2
    ) -> list["FrameData"]:
        limit = getattr(sys, "tracebacklimit", None)
        if limit is not None and limit < 0:
            limit = 0
        if limit is not None:
            if limit >= 0:
                frame_gen = itertools.islice(frame_gen, limit)
            else:
                frame_gen = iter(collections.deque(frame_gen, maxlen=-limit))

        result = []
        fnames = set()

        for f, lineno in frame_gen:
            co = f.f_code
            filename = co.co_filename
            name = co.co_name
            module = f.f_globals.get("__name__", "")

            fnames.add(filename)
            linecache.lazycache(filename, f.f_globals)
            f_locals = f.f_locals
            _locals: dict[str, str] = (
                {k: format_local(v) for k, v in f_locals.items()} if f_locals else {}
            )

            firstlineno = co.co_firstlineno
            lastlineno = firstlineno + sum(x for x in co.co_lnotab[1::2])
            lines_before = []
            lines_after = []
            line = linecache.getline(filename, lineno).rstrip()
            for i in range(
                max(co.co_firstlineno, lineno - line_context, line_context), lineno
            ):
                lines_before.append(linecache.getline(filename, i).rstrip())
            for i in range(lineno + 1, min(lastlineno, lineno + line_context) + 1):
                lines_after.append(linecache.getline(filename, i).rstrip())

            # Must defer line lookups until we have called checkcache.
            result.append(
                FrameData(
                    filename=filename,
                    lineno=lineno,
                    module=module,
                    name=name,
                    lines_before=lines_before,
                    line=line,
                    lines_after=lines_after,
                    locals=_locals,
                )
            )
        return result

    def format_exception_only(self) -> str:
        """Format the exception part of the traceback.

        The return value is a generator of strings, each ending in a newline.

        Normally, the generator emits a single string; however, for
        SyntaxError exceptions, it emits several lines that (when
        printed) display detailed information about where the syntax
        error occurred.

        The message indicating which exception occurred is always the last
        string in the output.
        """
        stype = self.exc_type
        smod = self.exc_module
        if smod not in ("__main__", "builtins"):
            stype = smod + "." + stype

        return "\n".join(
            itertools.chain(
                [_format_final_exc_line(stype, self.exc_str)],
                self.__notes__ or [],
            )
        )

    def format(
        self,
        *,
        chain: bool = True,
        include_line: bool = True,
        include_locals: bool = False,
    ) -> t.Iterator[str]:
        """Format the exception.

        If chain is not *True*, *__cause__* and *__context__* will not be formatted.

        The return value is a generator of strings, each ending in a newline and
        some containing internal newlines. `print_exception` is a wrapper around
        this method which just prints the lines to a file.

        The message indicating which exception occurred is always the last
        string in the output.
        """
        if chain:
            if self.__cause__ is not None:
                yield from self.__cause__.format(chain=chain)
                yield _cause_message
            elif self.__context__ is not None and not self.__suppress_context__:
                yield from self.__context__.format(chain=chain)
                yield _context_message
            if self.truncated:
                yield (
                    "Chained exceptions have been truncated to avoid "
                    "stack overflow in traceback formatting:\n"
                )
        if self.stack:
            yield "Traceback (most recent call last):\n"
            yield from self.format_stack()
        yield self.format_exception_only()

    def format_stack(
        self, *, include_line: bool = True, include_locals: bool = False
    ) -> t.Iterator[str]:
        """Format the stack ready for printing.

        Returns a list of strings ready for printing.  Each string in the
        resulting list corresponds to a single frame from the stack.
        Each string ends in a newline; the strings may contain internal
        newlines as well, for those items with source text lines.

        For long sequences of the same frame and line, the first few
        repetitions are shown, followed by a summary line stating the exact
        number of further repetitions.
        """
        last_file = None
        last_line = None
        last_name = None
        count = 0
        for frame in self.stack:
            if (
                last_file is None
                or last_file != frame.filename
                or last_line is None
                or last_line != frame.lineno
                or last_name is None
                or last_name != frame.name
            ):
                if count > _RECURSIVE_CUTOFF:
                    count -= _RECURSIVE_CUTOFF
                    yield (
                        f"  [Previous line repeated {count} more "
                        f'time{"s" if count > 1 else ""}]\n'
                    )
                last_file = frame.filename
                last_line = frame.lineno
                last_name = frame.name
                count = 0
            count += 1
            if count > _RECURSIVE_CUTOFF:
                continue
            yield '  File "{}", line {}, in {}\n'.format(
                frame.filename, frame.lineno, frame.name
            )
            if include_line and frame.line:
                yield "    {}\n".format(frame.line.strip())
            if include_locals and frame.locals:
                for name, value in sorted(frame.locals.items()):
                    yield "    {name} = {value}\n".format(name=name, value=value)
        if count > _RECURSIVE_CUTOFF:
            count -= _RECURSIVE_CUTOFF
            yield (
                f"  [Previous line repeated {count} more "
                f'time{"s" if count > 1 else ""}]\n'
            )


@dataclasses.dataclass
class FrameData:
    filename: str
    lineno: int
    module: str
    name: str
    locals: dict[str, str]
    lines_before: list[str]
    line: str
    lines_after: list[str]


def _format_final_exc_line(etype: str, value: str) -> str:
    if not value:
        line = f"{etype}\n"
    else:
        line = f"{etype}: {value}\n"
    return line


def _some_str(value: object) -> str:
    try:
        return str(value)
    except Exception:
        return f"<unprintable {type(value).__name__} object>"
