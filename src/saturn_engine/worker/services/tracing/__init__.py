import typing as t

from opentelemetry import trace

from .config import TracerConfig
from .tracer import Tracer
from .tracer import on_executor_initialized


def get_trace_id() -> t.Optional[str]:
    span = trace.get_current_span()
    span_context = span.get_span_context()
    if span_context == trace.INVALID_SPAN_CONTEXT:
        return None
    return trace.format_trace_id(span_context.trace_id)


__all__ = ("TracerConfig", "Tracer", "on_executor_initialized", "get_trace_id")
