"""OpenTelemetry wiring for the orchestrator.

Sends traces, metrics, and logs to Application Insights and turns on the
Microsoft Agent Framework's built-in instrumentation for workflow execution,
agent runs, and tool calls. Sandbox lifecycle spans are added in
``sandbox_manager`` on top of this.

Everything here is a no-op when ``APPLICATIONINSIGHTS_CONNECTION_STRING`` is not
set, so local runs without App Insights keep working.
"""
from __future__ import annotations

import logging
import os

logger = logging.getLogger("orchestrator.observability")

_CONFIGURED = False


def setup_observability(app=None) -> bool:
    """Configure Azure Monitor + Agent Framework instrumentation once.

    Returns True if telemetry was configured, False if skipped (no connection
    string) or already configured.
    """
    global _CONFIGURED
    if _CONFIGURED:
        return True

    conn = os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING")
    if not conn:
        logger.info("APPLICATIONINSIGHTS_CONNECTION_STRING not set; telemetry disabled.")
        return False

    try:
        from azure.monitor.opentelemetry import configure_azure_monitor

        # Sets global tracer/meter/logger providers and auto-instruments
        # requests/urllib3 (so outbound AOAI/Foundry calls are traced too).
        configure_azure_monitor(
            connection_string=conn,
            logger_name="orchestrator",
        )
    except Exception as ex:  # pragma: no cover - defensive
        logger.warning("configure_azure_monitor failed: %s", ex)
        return False

    if app is not None:
        try:
            from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

            FastAPIInstrumentor.instrument_app(app)
        except Exception as ex:  # pragma: no cover - defensive
            logger.warning("FastAPI instrumentation failed: %s", ex)

    try:
        # Attach Agent Framework's GenAI spans (agent runs + tool calls) to the
        # provider Azure Monitor just configured. Honors ENABLE_SENSITIVE_DATA.
        from agent_framework.observability import enable_instrumentation

        enable_instrumentation()
    except Exception as ex:  # pragma: no cover - defensive
        logger.warning("Agent Framework instrumentation failed: %s", ex)

    _CONFIGURED = True
    logger.info("Observability configured: exporting telemetry to Application Insights.")
    return True
