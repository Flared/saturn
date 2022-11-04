import typing as t

from opentelemetry import trace

from .config import TracerConfig
from .tracer import Tracer
from .tracer import on_executor_initialized


class TraceContext(t.TypedDict):
    trace_id: str
    is_remote: bool
    is_sampled: bool


def get_trace_context() -> t.Optional[TraceContext]:
    span = trace.get_current_span()
    span_context = span.get_span_context()
    if span_context == trace.INVALID_SPAN_CONTEXT:
        return None
    return {
        "trace_id": trace.format_trace_id(span_context.trace_id),
        "is_remote": span_context.is_remote,
        "is_sampled": span_context.trace_flags.sampled,
    }


__all__ = ("TracerConfig", "Tracer", "on_executor_initialized", "get_trace_id")
