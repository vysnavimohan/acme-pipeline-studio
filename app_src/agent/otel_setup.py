"""One-shot OpenTelemetry setup for the FastAPI app.

Call init_otel(app) once at startup. After that:
  - Every incoming HTTP request becomes a SERVER span (FastAPI instrumentation)
  - Every outgoing requests/httpx call becomes a CLIENT span (GitHub API,
    Databricks SDK transport)
  - Every LangGraph node decorated with @traced_node gets an INTERNAL span,
    automatically a child of the request span

Spans flow to two exporters:
  - DeltaSpanExporter — persists to ${OPS_CONTROL_CATALOG}.${OPS_CONTROL_SCHEMA}.app_spans
                        for the in-app /observability page
  - ConsoleSpanExporter — writes JSON to stdout; captured by the App platform's
                        log pipeline. Acts as a dead-letter for trace data.
"""

from __future__ import annotations

import logging

LOG = logging.getLogger("ops.otel_setup")

_initialized = False


def init_otel(app) -> None:
    """Idempotent OTel initialization.

    Each step is in its own try/except so missing optional packages don't
    prevent the core tracer provider + DeltaSpanExporter from coming up.
    """
    global _initialized
    if _initialized:
        return

    # --- core SDK + DeltaSpanExporter (required) ---
    try:
        from opentelemetry import trace
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
        from agent.otel_exporter import DeltaSpanExporter
    except Exception as e:
        LOG.warning("OTel core SDK not available — skipping init: %s", e)
        return

    try:
        resource = Resource.create({
            "service.name": "databricks-pipeline-studio",
            "service.version": "0.3.0",
        })
        provider = TracerProvider(resource=resource)
        provider.add_span_processor(BatchSpanProcessor(DeltaSpanExporter()))
        provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
        trace.set_tracer_provider(provider)
        LOG.info("OTel core SDK initialized — DeltaSpanExporter + ConsoleSpanExporter active.")
    except Exception as e:
        LOG.warning("OTel TracerProvider setup failed: %s", e)
        return

    # --- optional instrumentors — each independent ---
    try:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        FastAPIInstrumentor.instrument_app(app)
        LOG.info("FastAPIInstrumentor active.")
    except Exception as e:
        LOG.warning("FastAPI instrumentation not active: %s", e)

    try:
        from opentelemetry.instrumentation.requests import RequestsInstrumentor
        RequestsInstrumentor().instrument()
        LOG.info("RequestsInstrumentor active.")
    except Exception as e:
        LOG.warning("requests instrumentation not active: %s", e)

    try:
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
        HTTPXClientInstrumentor().instrument()
        LOG.info("HTTPXClientInstrumentor active.")
    except Exception as e:
        LOG.warning("httpx instrumentation not active: %s", e)

    _initialized = True


def init_otel_for_job() -> None:
    """OTel init for the per-request agent Job (no FastAPI app).

    Sets up the TracerProvider + DeltaSpanExporter so @traced_node spans
    from the agent keep landing in the app_spans table (and the in-app
    /observability page) now that the agent runs outside the App process.
    """
    global _initialized
    if _initialized:
        return
    try:
        from opentelemetry import trace
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
        from agent.otel_exporter import DeltaSpanExporter

        resource = Resource.create({
            "service.name": "databricks-pipeline-studio-agent-job",
            "service.version": "0.4.0",
        })
        provider = TracerProvider(resource=resource)
        provider.add_span_processor(BatchSpanProcessor(DeltaSpanExporter()))
        provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
        trace.set_tracer_provider(provider)
        _initialized = True
        LOG.info("OTel initialized for job context.")
    except Exception as e:
        LOG.warning("OTel job init skipped: %s", e)


def flush() -> None:
    """Force-flush pending spans (call before a job process exits, or the
    BatchSpanProcessor's buffered spans are lost)."""
    try:
        from opentelemetry import trace
        provider = trace.get_tracer_provider()
        if hasattr(provider, "force_flush"):
            provider.force_flush(timeout_millis=10000)
    except Exception as e:
        LOG.warning("OTel flush failed: %s", e)
