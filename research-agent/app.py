"""
Research Agent — runs inside an ACA Sandbox.

Uses Microsoft Agent Framework with Foundry's hosted web-search tool to research
a question from the RESEARCH_QUESTION env var. Exposes results via Flask API.

Web search runs server-side in the Foundry project, so this sandbox only needs
egress to the Foundry endpoint (no search-engine egress). Falls back to a direct
Azure OpenAI call (model knowledge only) if Foundry is not configured.
"""

import asyncio
import json
import os
import ssl
import threading
import time
import traceback

import requests
from flask import Flask, jsonify

# ADC sandbox egress proxy does TLS interception — disable SSL verification
os.environ.setdefault("CURL_CA_BUNDLE", "")
os.environ.setdefault("REQUESTS_CA_BUNDLE", "")
os.environ.setdefault("SSL_CERT_FILE", "")
# For httpx (used by openai SDK)
os.environ.setdefault("SSL_VERIFY", "0")

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# The sandbox egress proxy does TLS interception. The Azure Monitor exporter
# talks to App Insights through `requests` (via azure-core), which does not honor
# the *_CA_BUNDLE env vars the same way, so force-disable verification for every
# requests Session. This bypasses server-certificate validation; the egress
# allow-list does not replace TLS authentication. Prefer trusting the proxy CA
# when available.
try:
    import requests as _rq

    _orig_merge = _rq.sessions.Session.merge_environment_settings

    def _merge_no_verify(self, url, proxies, stream, verify, cert):
        settings = _orig_merge(self, url, proxies, stream, verify, cert)
        settings["verify"] = False
        return settings

    _rq.sessions.Session.merge_environment_settings = _merge_no_verify
except Exception:
    pass


def _setup_observability():
    """Export OpenTelemetry traces to App Insights and join the orchestrator's
    trace via the forwarded W3C trace context. No-op without a connection string.

    Returns the extracted parent context (or None) so the research span can be
    linked to the orchestrator's ``sandbox.create`` span.
    """
    conn = os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING")
    if not conn:
        return None
    try:
        from azure.monitor.opentelemetry import configure_azure_monitor

        configure_azure_monitor(connection_string=conn)
    except Exception as ex:
        print(f"[observability] configure_azure_monitor failed: {ex}", flush=True)
        return None
    try:
        from agent_framework.observability import enable_instrumentation

        enable_instrumentation()
    except Exception as ex:
        print(f"[observability] enable_instrumentation failed: {ex}", flush=True)
    try:
        from opentelemetry.propagate import extract

        return extract(
            {
                "traceparent": os.environ.get("TRACEPARENT", ""),
                "tracestate": os.environ.get("TRACESTATE", ""),
            }
        )
    except Exception:
        return None


_PARENT_CTX = _setup_observability()

app = Flask(__name__)

# ── shared state ────────────────────────────────────────────────────────
state = {
    "status": "starting",   # starting | working | done | error
    "progress": "Initializing...",
    "question": os.environ.get("RESEARCH_QUESTION", ""),
    "answer": None,
    "sources": [],
    "confidence": 0.0,
    "error": None,
    "simulated": False,
    "hint": None,
    "diagnostics": None,
}
state_lock = threading.Lock()


# ── Agent Framework research (Foundry hosted web search) ───────────────
def _read_forwarded_token(token_name: str):
    """Read a static bearer token with the issuer's Unix expiry, never renew it."""
    from azure.core.credentials import AccessToken

    token = os.environ.get(token_name, "")
    if not token:
        raise RuntimeError(f"{token_name} is missing from the sandbox environment.")
    expiry_name = f"{token_name}_EXPIRES_ON"
    try:
        expires_on = int(os.environ[expiry_name])
    except (KeyError, ValueError):
        raise RuntimeError(
            f"{expiry_name} must be provided as an integer Unix timestamp."
        ) from None
    if expires_on <= time.time():
        raise RuntimeError(
            f"{token_name} has expired; forwarded tokens cannot renew inside the sandbox."
        )
    return AccessToken(token, expires_on)


async def _run_agent_research(question: str) -> dict:
    """Use Microsoft Agent Framework + Foundry's hosted web-search tool.

    The search executes server-side in the Foundry project, so this sandbox
    never calls a search engine directly — it only talks to the Foundry endpoint.
    """
    import httpx
    from agent_framework import Agent
    from agent_framework.foundry import FoundryChatClient
    from azure.ai.projects.aio import AIProjectClient
    from azure.core.credentials import AccessToken
    from azure.core.pipeline.transport import AioHttpTransport

    project_endpoint = os.environ["FOUNDRY_PROJECT_ENDPOINT"]
    access_token = _read_forwarded_token("AZURE_AI_TOKEN")
    model = os.environ.get("AZURE_OPENAI_DEPLOYMENT", "gpt-5-mini")

    # The token is minted by the orchestrator's managed identity for the
    # https://ai.azure.com audience and forwarded into this sandbox. Wrap it in
    # a credential shim so no outbound IMDS/AAD call is made from inside the
    # egress-locked sandbox.
    class _ForwardedTokenCredential:
        async def get_token(self, *scopes: str, **kwargs: object) -> AccessToken:
            if access_token.expires_on <= time.time():
                raise RuntimeError(
                    "AZURE_AI_TOKEN has expired; forwarded tokens cannot renew "
                    "inside the sandbox."
                )
            return access_token

        async def close(self) -> None:
            return None

        async def __aenter__(self) -> "_ForwardedTokenCredential":
            return self

        async def __aexit__(self, *exc: object) -> None:
            return None

    # ADC egress proxy does TLS interception — disable cert verification on both
    # the AIProjectClient transport and the OpenAI (Responses) client it builds.
    project_client = AIProjectClient(
        endpoint=project_endpoint,
        credential=_ForwardedTokenCredential(),
        transport=AioHttpTransport(connection_verify=False),
    )
    _orig_get_openai_client = project_client.get_openai_client

    def _get_openai_client(**kwargs: object):  # type: ignore[no-untyped-def]
        kwargs.setdefault("http_client", httpx.AsyncClient(verify=False))
        return _orig_get_openai_client(**kwargs)

    project_client.get_openai_client = _get_openai_client  # type: ignore[assignment]

    client = FoundryChatClient(project_client=project_client, model=model)

    agent = Agent(
        client=client,
        name="ResearchAgent",
        instructions=(
            "You are a thorough research agent. For the given question:\n"
            "1. Use web search to find current, factual information\n"
            "2. Synthesize findings into a comprehensive answer\n"
            "3. Return your answer as JSON with these fields:\n"
            '   "answer": "<detailed markdown answer with key findings>",\n'
            '   "sources": ["<url1>", "<url2>", ...],\n'
            '   "confidence": <float 0-1>\n'
            "Return ONLY valid JSON, no extra text or code fences."
        ),
        tools=[FoundryChatClient.get_web_search_tool()],
        default_options={"reasoning": {"effort": "low"}},
    )

    response = await agent.run(question)
    raw = str(response).strip()

    # Strip markdown code fences if present
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
        if raw.endswith("```"):
            raw = raw[:-3]
        raw = raw.strip()

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {
            "answer": raw,
            "sources": [],
            "confidence": 0.7,
        }


def _call_openai_direct(question: str) -> dict:
    """Fallback: direct Azure OpenAI call using the model's own knowledge.

    Used when the Foundry project (hosted web search) is unavailable. No web
    search is performed here, keeping the sandbox within its AOAI-only egress.
    """
    import httpx
    from openai import AzureOpenAI

    endpoint = os.environ["AZURE_OPENAI_ENDPOINT"]
    token = _read_forwarded_token("AZURE_OPENAI_TOKEN").token
    deployment = os.environ.get("AZURE_OPENAI_DEPLOYMENT", "gpt-5-mini")

    # ADC egress proxy does TLS interception — use custom httpx client with verify=False.
    # Keyless auth: AZURE_OPENAI_TOKEN is an AAD bearer token forwarded by the orchestrator.
    http_client = httpx.Client(verify=False)
    client = AzureOpenAI(
        azure_endpoint=endpoint,
        azure_ad_token=token,
        api_version="preview",
        http_client=http_client,
    )

    system_prompt = (
        "You are a research assistant. Answer the question thoroughly and "
        "factually using your own knowledge.\n\n"
        "Return your answer as JSON with these fields:\n"
        '  "answer": "<detailed answer in markdown>",\n'
        '  "sources": ["<url1>", "<url2>", ...],\n'
        '  "confidence": <float 0-1>\n'
        "Return ONLY valid JSON, no extra text."
    )

    # gpt-5-mini is a reasoning model: it rejects a non-default `temperature`
    # and uses `max_completion_tokens` (which also covers reasoning tokens),
    # not the deprecated `max_tokens`.
    response = client.chat.completions.create(
        model=deployment,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": question},
        ],
        max_completion_tokens=4000,
        reasoning_effort="low",
    )

    raw = response.choices[0].message.content.strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
        if raw.endswith("```"):
            raw = raw[:-3]
        raw = raw.strip()

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        # Model returned prose or JSON with an invalid escape. Degrade
        # gracefully rather than failing the whole research task.
        return {
            "answer": raw,
            "sources": [],
            "confidence": 0.6,
        }


def _simulate_research(
    question: str,
    reason: str = "AI call unavailable in sandbox",
    diagnostics: str | None = None,
) -> dict:
    """Return a canned result after a short delay (no Azure OpenAI)."""
    time.sleep(8)
    return {
        "answer": (
            f"## Simulated Research Results\n\n"
            f"This is a **simulated** answer for the question:\n\n"
            f"> {question}\n\n"
            f"### Key Findings\n"
            f"1. Finding one — placeholder insight.\n"
            f"2. Finding two — supporting evidence.\n"
            f"3. Finding three — additional context.\n\n"
            f"*Note: Simulated — no Azure OpenAI endpoint configured.*"
        ),
        "sources": ["Simulated Source A", "Simulated Source B"],
        "confidence": 0.65,
        "simulated": True,
        "hint": reason,
        "diagnostics": diagnostics,
    }


# ── network connectivity test ──────────────────────────────────────────
def _test_connectivity(endpoint: str) -> str:
    """Test DNS resolution and TCP connectivity to endpoint."""
    import socket
    from urllib.parse import urlparse
    results = []
    parsed = urlparse(endpoint)
    host = parsed.hostname or endpoint
    port = parsed.port or 443

    # DNS test
    try:
        addrs = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
        ip = addrs[0][4][0] if addrs else "?"
        results.append(f"DNS:{host}->{ip}")
    except Exception as e:
        results.append(f"DNS_FAIL:{host}:{e}")
        return "; ".join(results)

    # TCP test
    try:
        sock = socket.create_connection((host, port), timeout=5)
        sock.close()
        results.append(f"TCP:{host}:{port}->OK")
    except Exception as e:
        results.append(f"TCP_FAIL:{host}:{port}:{e}")

    # HTTPS test (skip SSL verification — ADC proxy uses self-signed certs)
    try:
        resp = requests.get(f"https://{host}/", timeout=5, verify=False)
        results.append(f"HTTPS:{resp.status_code}")
    except Exception as e:
        results.append(f"HTTPS_FAIL:{type(e).__name__}")

    return "; ".join(results)


# ── background research thread ─────────────────────────────────────────
def _research_worker():
    global state
    question = state["question"]

    if not question:
        with state_lock:
            state["status"] = "error"
            state["progress"] = "No research question provided"
            state["error"] = "RESEARCH_QUESTION env var is empty"
        return

    # Wrap the whole run in a span that joins the orchestrator's trace, so the
    # in-sandbox agent run and its tool calls appear under the same end-to-end
    # transaction in Application Insights.
    try:
        from opentelemetry import trace as _trace

        tracer = _trace.get_tracer("research-agent")
        span_cm = tracer.start_as_current_span(
            "research-agent.run", context=_PARENT_CTX
        )
    except Exception:
        import contextlib

        span_cm = contextlib.nullcontext()

    with span_cm as _sp:
        if _sp is not None:
            try:
                _sp.set_attribute("research.question", question[:200])
            except Exception:
                pass
        _do_research(question)


def _do_research(question: str):
    endpoint = os.environ.get("AZURE_OPENAI_ENDPOINT", "")

    with state_lock:
        state["status"] = "working"
        state["progress"] = "Waiting for network egress..."

    # Wait for egress policy to be applied, then test connectivity
    conn_info = "no endpoint"
    if endpoint:
        time.sleep(5)
        conn_info = _test_connectivity(endpoint)
        with state_lock:
            state["progress"] = f"Connectivity: {conn_info}"

    try:
        result = _run_research_with_retries(question, conn_info=conn_info)

        with state_lock:
            state["status"] = "done"
            state["progress"] = "Research complete"
            state["answer"] = result.get("answer", "")
            state["sources"] = result.get("sources", [])
            state["confidence"] = result.get("confidence", 0.0)
            state["simulated"] = bool(result.get("simulated", False))
            state["hint"] = result.get("hint")
            state["diagnostics"] = result.get("diagnostics")

    except Exception as exc:
        with state_lock:
            state["status"] = "error"
            state["progress"] = f"Error: {exc}"
            state["error"] = traceback.format_exc()


def _run_research_with_retries(question: str, max_retries: int = 1, conn_info: str = "") -> dict:
    """Try AI research with retries (egress policy may be applied after startup)."""
    if not os.environ.get("AZURE_OPENAI_ENDPOINT"):
        return _simulate_research(
            question,
            reason="AZURE_OPENAI_ENDPOINT is missing in the sandbox environment",
            diagnostics="Endpoint not configured",
        )

    errors = [f"CONNECTIVITY: {conn_info}"]
    for attempt in range(max_retries):
        with state_lock:
            state["progress"] = f"Research attempt {attempt + 1}/{max_retries}..."

        # Primary: Agent Framework + Foundry hosted web search (if configured)
        if os.environ.get("FOUNDRY_PROJECT_ENDPOINT"):
            try:
                return asyncio.run(_run_agent_research(question))
            except Exception as af_err:
                errors.append(f"Foundry attempt {attempt+1}: {type(af_err).__name__}: {af_err}")

        # Fallback: direct Azure OpenAI call (model knowledge only, no web search)
        try:
            return _call_openai_direct(question)
        except Exception as oai_err:
            errors.append(f"OpenAI attempt {attempt+1}: {type(oai_err).__name__}: {oai_err}")

        # Wait before retry (egress might not be ready yet)
        if attempt < max_retries - 1:
            with state_lock:
                state["progress"] = f"Retrying in 5s (attempt {attempt + 1} failed)..."
            time.sleep(5)

    # All retries exhausted — fall back to simulated
    with state_lock:
        state["progress"] = "Using simulated research (AI unavailable)..."
        state["error"] = "\n".join(errors)
    return _simulate_research(
        question,
        reason=(
            "Azure OpenAI request failed inside sandbox. "
            "Most common causes: missing/expired token, deployment mismatch, or egress/DNS restrictions."
        ),
        diagnostics="\n".join(errors),
    )


# ── Flask routes ────────────────────────────────────────────────────────
@app.route("/health")
def health():
    return jsonify({"status": "healthy"})


@app.route("/debug")
def debug():
    """Debug endpoint showing detailed error info and environment."""
    with state_lock:
        return jsonify({
            "status": state["status"],
            "progress": state["progress"],
            "error": state.get("error"),
            "has_openai_endpoint": bool(os.environ.get("AZURE_OPENAI_ENDPOINT")),
            "has_openai_token": bool(os.environ.get("AZURE_OPENAI_TOKEN")),
            "has_openai_deployment": bool(os.environ.get("AZURE_OPENAI_DEPLOYMENT")),
            "question": state["question"][:50] if state["question"] else None,
            "simulated": state.get("simulated", False),
            "hint": state.get("hint"),
        })


@app.route("/status")
def status():
    with state_lock:
        return jsonify({
            "status": state["status"],
            "progress": state["progress"],
            "error": state.get("error"),
            "has_openai": bool(os.environ.get("AZURE_OPENAI_ENDPOINT")),
            "simulated": state.get("simulated", False),
            "hint": state.get("hint"),
            "diagnostics": state.get("diagnostics"),
        })


@app.route("/result")
def result():
    with state_lock:
        if state["status"] != "done":
            return jsonify({
                "status": state["status"],
                "message": "Research not yet complete",
            }), 202

        return jsonify({
            "question": state["question"],
            "answer": state["answer"],
            "sources": state["sources"],
            "confidence": state["confidence"],
            "simulated": state.get("simulated", False),
            "hint": state.get("hint"),
            "diagnostics": state.get("diagnostics"),
        })


# ── start background thread on module load ──────────────────────────────
_worker_started = False


def _ensure_worker():
    global _worker_started
    if not _worker_started:
        _worker_started = True
        t = threading.Thread(target=_research_worker, daemon=True)
        t.start()


_ensure_worker()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
