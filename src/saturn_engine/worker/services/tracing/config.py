import typing

import functools
import logging

from opentelemetry import trace

from saturn_engine.utils.inspect import import_name
from saturn_engine.worker.executors.bootstrap import PipelineBootstrap

from .. import BaseServices
from .. import Service

if typing.TYPE_CHECKING:
    from opentelemetry.sdk.trace import sampling


class TracerConfig(Service[BaseServices, "TracerConfig.Options"]):
    name = "trace_config"

    class Options:
        exporters: list[str] = ["opentelemetry.sdk.trace.export.ConsoleSpanExporter"]

    async def open(self) -> None:
        from .sampler import ParentBasedSaturnSampler

        self.services.hooks.executor_initialized.register(
            functools.partial(
                self.on_executor_initialized, exporter_names=self.options.exporters
            )
        )
        self.setup_tracer(
            exporter_names=self.options.exporters,
            sampler=ParentBasedSaturnSampler(),
            service_name="saturn_worker",
        )

    @classmethod
    def on_executor_initialized(
        cls, pipeline_bootstrap: PipelineBootstrap, *, exporter_names: list[str]
    ) -> None:
        from opentelemetry.sdk.trace import sampling

        cls.setup_tracer(
            exporter_names=exporter_names,
            sampler=sampling.ParentBased(root=sampling.DEFAULT_OFF),
            service_name="saturn_executor",
        )

    @staticmethod
    def setup_tracer(
        *, exporter_names: list[str], sampler: "sampling.Sampler", service_name: str
    ) -> None:
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import SpanProcessor
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.sdk.trace.export import ConsoleSpanExporter
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor

        logger = logging.getLogger(__name__)

        resource = Resource.create(
            {
                "service.name": service_name,
                "service.namespace": "saturn",
            }
        )

        provider = TracerProvider(
            sampler=sampler,
            resource=resource,
        )

        for exporter_name in exporter_names:
            try:
                exporter_cls = import_name(exporter_name)
                exporter = exporter_cls()
                processor: SpanProcessor
                if isinstance(exporter, ConsoleSpanExporter):
                    processor = SimpleSpanProcessor(exporter)
                else:
                    processor = BatchSpanProcessor(exporter)
                provider.add_span_processor(processor)
            except Exception:
                logger.exception("Failed to load exporter: %s", exporter_name)

        trace.set_tracer_provider(provider)
