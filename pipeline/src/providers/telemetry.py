"""Telemetry provider — OpenTelemetry setup for the pipeline.

Cross-cutting concern injected at the runtime layer.
"""

from __future__ import annotations

import structlog
from opentelemetry import trace
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

from src.config.settings import Settings

logger = structlog.get_logger(__name__)


class TelemetryProvider:
    """Initialises OpenTelemetry tracing and structured logging."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._tracer_provider = self._setup_tracing()
        self._setup_logging()

    def tracer(self, name: str = "naap.pipeline") -> trace.Tracer:
        return self._tracer_provider.get_tracer(name)

    def _setup_tracing(self) -> TracerProvider:
        resource = Resource.create({SERVICE_NAME: "naap-analytics-pipeline"})
        provider = TracerProvider(resource=resource)

        if self._settings.otlp_endpoint:
            # Lazy import — only pulled in when OTLP is configured.
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter  # noqa: PLC0415

            exporter = OTLPSpanExporter(endpoint=self._settings.otlp_endpoint, insecure=True)
            provider.add_span_processor(BatchSpanProcessor(exporter))
            logger.info("otlp_tracing_enabled", endpoint=self._settings.otlp_endpoint)
        elif self._settings.is_development:
            provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))

        trace.set_tracer_provider(provider)
        return provider

    @staticmethod
    def _setup_logging() -> None:
        structlog.configure(
            processors=[
                structlog.contextvars.merge_contextvars,
                structlog.processors.add_log_level,
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.StackInfoRenderer(),
                structlog.dev.ConsoleRenderer(),
            ],
            wrapper_class=structlog.make_filtering_bound_logger(20),  # INFO
            context_class=dict,
            logger_factory=structlog.PrintLoggerFactory(),
        )

    def shutdown(self) -> None:
        self._tracer_provider.shutdown()
