# pylint: disable=unused-import
import typing as t

from collections.abc import Sequence

from opentelemetry.context import Context
from opentelemetry.sdk.trace import sampling
from opentelemetry.trace import Link
from opentelemetry.trace import SpanKind
from opentelemetry.trace import get_current_span
from opentelemetry.trace.span import TraceState
from opentelemetry.util.types import Attributes


class SaturnSampler(sampling.Sampler):
    def should_sample(
        self,
        parent_context: t.Optional[Context],
        trace_id: int,
        name: str,
        kind: SpanKind = None,
        attributes: Attributes = None,
        links: Sequence[Link] = None,
        trace_state: TraceState = None,
    ) -> sampling.SamplingResult:
        if not attributes:
            return _drop(parent_context)

        sampling_rate = attributes.get("saturn.sampling.rate")
        if not isinstance(sampling_rate, float):
            return _drop(parent_context)

        rate_sampler = sampling.TraceIdRatioBased(rate=sampling_rate)
        return rate_sampler.should_sample(
            parent_context=parent_context,
            trace_id=trace_id,
            name=name,
            kind=kind,
            attributes=attributes,
            links=links,
            trace_state=trace_state,
        )

    def get_description(self) -> str:
        return "SaturnSampler"


class ParentBasedSaturnSampler(sampling.ParentBased):
    """
    Sampler that respects its parent span's sampling decision, but otherwise
    samples probabalistically based on `rate`.
    """

    def __init__(self) -> None:
        root = SaturnSampler()
        super().__init__(root=root)


def _drop(parent_context: t.Optional[Context]) -> sampling.SamplingResult:
    return sampling.SamplingResult(
        sampling.Decision.DROP,
        {},
        _get_parent_trace_state(parent_context),
    )


def _get_parent_trace_state(
    parent_context: t.Optional[Context],
) -> t.Optional[TraceState]:
    parent_span_context = get_current_span(parent_context).get_span_context()
    if parent_span_context is None or not parent_span_context.is_valid:
        return None
    return parent_span_context.trace_state
