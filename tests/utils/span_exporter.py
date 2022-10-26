import typing as t

import dataclasses

from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace import Span as SdkSpan
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter as _InMemorySpanExporter,
)


@dataclasses.dataclass
class Span:
    otel_span: ReadableSpan
    children: list["Span"]


class InMemorySpanExporter(_InMemorySpanExporter):
    def __init__(self) -> None:
        super().__init__()  # type: ignore[no-untyped-call]

    def clear(self) -> None:
        super().clear()  # type: ignore[no-untyped-call]

    def get_finished_traces(self) -> list[Span]:
        span_by_id: dict[int, Span] = {}
        root_spans = []

        span_by_id = {
            span.context.span_id: Span(otel_span=span, children=[])
            for span in self.get_finished_spans()  # type: ignore[no-untyped-call]
        }

        for span in span_by_id.values():
            parent_id = self._span_parent_id(span.otel_span)
            if parent_id is None:
                root_spans.append(span)
            else:
                span_by_id[parent_id].children.append(span)

        return root_spans

    @staticmethod
    def _span_parent_id(span: ReadableSpan) -> t.Optional[int]:
        if span.parent is None:
            return None
        if isinstance(span.parent, SdkSpan):
            return span.parent.context.span_id
        return span.parent.span_id
