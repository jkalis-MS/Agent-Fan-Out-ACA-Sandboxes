"""
Sandbox Manager — Async wrapper around the public ACA Sandboxes SDK
(`azure-containerapps-sandbox`) for managing the research-agent sandbox
group and per-question sandboxes.

Mirrors agents/orchestrator.net/Services/SandboxManager.cs.

The SDK is synchronous (azure-core sync pipeline). All calls are wrapped
with ``asyncio.to_thread()`` so they don't block the FastAPI event loop.

SDK API surface used:
    - SandboxGroupClient(endpoint_for_region(region), credential, ...)
        .list_disk_images() / .create_disk_image() / .get_disk_image() / .delete_disk_image()
        .begin_create_sandbox(...).result()  →  returns SandboxClient
    - SandboxClient (sandbox-scoped instance returned by the LRO poller)
        .get() / .delete() / .exec()
    - Models: DiskImage, Sandbox, EgressPolicy, AddPortRequest, PortAuthConfig,
      endpoint_for_region.

Tested against ``azure-containerapps-sandbox 0.1.0b4``
(local regression and SDK request-compatibility tests; v2 MI disk-image pull
also verified against the live service).
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

import httpx
from azure.identity import DefaultAzureCredential as SyncCredential
from azure.identity.aio import DefaultAzureCredential as AsyncCredential

from azure.containerapps.sandbox import (
    AddPortRequest,
    DiskImage,
    EgressHostRule,
    EgressPolicy,
    PortAuthConfig,
    Sandbox,
    SandboxClient,
    SandboxGroupClient,
    endpoint_for_region,
)

logger = logging.getLogger(__name__)

# OpenTelemetry is optional: present in the container image (pulled by
# azure-monitor-opentelemetry) but harmless if missing locally. All span usage
# goes through `_span`, which no-ops when tracing is unavailable.
try:  # pragma: no cover - import guard
    from opentelemetry import propagate as _otel_propagate
    from opentelemetry import trace as _otel_trace

    _tracer = _otel_trace.get_tracer("orchestrator.sandbox")
except Exception:  # pragma: no cover
    _otel_propagate = None
    _tracer = None


@contextmanager
def _span(name: str, **attributes: Any):
    """Start an OTEL span, or no-op if tracing isn't configured."""
    if _tracer is None:
        yield None
        return
    with _tracer.start_as_current_span(name) as sp:
        for key, value in attributes.items():
            if value is not None:
                sp.set_attribute(key, value)
        yield sp


def _traceparent_env() -> dict[str, str]:
    """W3C trace-context carrier for the current span, to forward into a
    sandbox so its in-VM spans join the same end-to-end trace."""
    if _otel_propagate is None:
        return {}
    carrier: dict[str, str] = {}
    _otel_propagate.inject(carrier)
    env: dict[str, str] = {}
    if carrier.get("traceparent"):
        env["TRACEPARENT"] = carrier["traceparent"]
    if carrier.get("tracestate"):
        env["TRACESTATE"] = carrier["tracestate"]
    return env


# ── Region map (parity with the .NET version) ──────────────────────────────

REGIONS: dict[str, str] = {
    "westus2":        "West US 2",
    "westus3":        "West US 3",
    "westcentralus":  "West Central US",
    "canadacentral":  "Canada Central",
    "swedencentral":  "Sweden Central",
    "northeurope":    "North Europe",
}
DEFAULT_REGION = "westus2"


def validate_region(region: str | None) -> str:
    return region if region in REGIONS else DEFAULT_REGION


def region_to_sandbox_group_name(region: str) -> str:
    return f"sg-demo-swarm-{region}"


# ── Label conventions for managed disk images ──────────────────────────────
LABEL_DEMO         = "demo"
LABEL_DEMO_VALUE   = "agents"
LABEL_IMAGE_REF    = "image-ref"
LABEL_OCI_DIGEST   = "oci-digest"


SANDBOX_PROVISION_TIMEOUT_SECONDS = 300
RESEARCH_TIMEOUT_SECONDS = 360

# Static tokens cannot renew inside a sandbox. Cover provisioning, research,
# and 60s of clock skew and request overhead. Acquire after the create throttle.
FORWARDED_TOKEN_MIN_LIFETIME_SECONDS = (
    SANDBOX_PROVISION_TIMEOUT_SECONDS + RESEARCH_TIMEOUT_SECONDS + 60
)


def _require_token_lifetime(expires_on: int, token_name: str) -> None:
    if expires_on - time.time() <= FORWARDED_TOKEN_MIN_LIFETIME_SECONDS:
        raise RuntimeError(
            f"{token_name} has insufficient remaining lifetime; more than "
            f"{FORWARDED_TOKEN_MIN_LIFETIME_SECONDS}s is required for sandbox "
            "provisioning and research. Retry when the credential can acquire "
            "a fresh token; forwarded tokens cannot renew inside the sandbox."
        )


# ── Models ──────────────────────────────────────────────────────────────────

@dataclass
class SandboxStatus:
    status: str
    progress: str
    error: str | None = None
    has_openai: bool = False


@dataclass
class AgentResult:
    question: str
    answer: str
    sources: list[str] = field(default_factory=list)
    confidence: float = 0.0
    simulated: bool = False
    hint: str | None = None
    diagnostics: str | None = None


# ── SandboxManager ──────────────────────────────────────────────────────────

class SandboxManager:
    """Manages the lifecycle of the sandbox group, disk images, and sandboxes."""

    def __init__(self) -> None:
        # Required
        self.subscription_id = _required("SUBSCRIPTION_ID")
        self.resource_group  = _required("RESOURCE_GROUP")

        # Defaults (env overrideable)
        self.current_region = os.environ.get("DEFAULT_REGION", DEFAULT_REGION)
        self.current_sandbox_group_name = (
            os.environ.get("SANDBOX_GROUP")
            or region_to_sandbox_group_name(self.current_region)
        )
        self.container_image = os.environ.get("DISK_IMAGE_ID", "")

        # The disk-image service expects a client ID, not an ARM resource ID.
        self.sandbox_group_uami_client_id = _required("SANDBOX_GROUP_UAMI_CLIENT_ID")

        # Azure OpenAI passthrough for the research agent.
        self.openai_endpoint   = os.environ.get("AZURE_OPENAI_ENDPOINT")
        self.openai_deployment = os.environ.get("AZURE_OPENAI_DEPLOYMENT", "gpt-5-mini")

        # Foundry project endpoint (for the researcher's hosted web search tool).
        self.foundry_project_endpoint = os.environ.get("FOUNDRY_PROJECT_ENDPOINT")

        # Application Insights connection string. When set, it is forwarded into
        # each sandbox (so the in-VM research agent reports to the same trace)
        # and its ingestion endpoints are added to the sandbox egress allow-list.
        self.appinsights_conn = os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING")

        # Cached AOAI bearer token (lazy)
        self._aoai_token: str | None = None
        self._aoai_token_expires_on: int = 0  # epoch seconds

        # Cached Foundry (ai.azure.com) bearer token (lazy)
        self._foundry_token: str | None = None
        self._foundry_token_expires_on: int = 0  # epoch seconds

        # Concurrency throttle (max 10 concurrent sandbox creates)
        self._throttle = asyncio.Semaphore(10)

        # Caches
        self._provisioned_groups: set[str] = set()
        self._current_disk_image_id: str | None = None
        self._current_disk_image_digest: str | None = None
        self._sandbox_endpoints: dict[str, str] = {}  # logical id -> external URL
        self._sbx_clients: dict[str, SandboxClient] = {}  # logical id -> SDK client
        self._sandbox_started_at: dict[str, float] = {}
        self._total_sandbox_seconds: float = 0.0
        self._total_sandbox_runs: int = 0
        self._prepare_lock = asyncio.Lock()

        # Long-lived credentials + group client (lazy; closed on shutdown).
        # The SDK's SandboxGroupClient is bound to a single (region, group),
        # so we rebuild it when the active region/group changes.
        self._sync_credential: SyncCredential | None = None
        self._async_credential: AsyncCredential | None = None
        self._group_client: SandboxGroupClient | None = None
        self._group_client_key: tuple[str, str] | None = None  # (region, group)

        # HTTP client for polling research-agent /status & /result endpoints
        self._http = httpx.AsyncClient(timeout=httpx.Timeout(120.0))

    async def aclose(self) -> None:
        await self._http.aclose()
        # Close any cached sandbox-scoped clients
        for sbx in list(self._sbx_clients.values()):
            try:
                await asyncio.to_thread(sbx.close)
            except Exception:
                pass
        self._sbx_clients.clear()
        if self._group_client is not None:
            await asyncio.to_thread(self._group_client.close)
            self._group_client = None
        if self._async_credential is not None:
            await self._async_credential.close()
            self._async_credential = None
        if self._sync_credential is not None:
            await asyncio.to_thread(self._sync_credential.close)
            self._sync_credential = None

    # ── Lazy client init ───────────────────────────────────────────────────

    def _get_group_client(self) -> SandboxGroupClient:
        """Return (and lazily build/rebuild) a SandboxGroupClient bound to
        the current region + sandbox group."""
        key = (self.current_region, self.current_sandbox_group_name)
        if self._group_client is not None and self._group_client_key == key:
            return self._group_client

        # Region/group changed — close the previous client.
        if self._group_client is not None:
            try:
                self._group_client.close()
            except Exception:
                pass

        if self._sync_credential is None:
            self._sync_credential = SyncCredential()

        self._group_client = SandboxGroupClient(
            endpoint_for_region(self.current_region),
            self._sync_credential,
            subscription_id=self.subscription_id,
            resource_group=self.resource_group,
            sandbox_group=self.current_sandbox_group_name,
        )
        self._group_client_key = key
        return self._group_client

    # ── AOAI bearer token (keyless) ────────────────────────────────────────

    async def get_aoai_token(self) -> str | None:
        """Acquire a Cognitive Services token valid for provisioning + research.

        Forwarded with its issuer expiry as AZURE_OPENAI_TOKEN_EXPIRES_ON.
        """
        if not self.openai_endpoint:
            return None

        if self._async_credential is None:
            self._async_credential = AsyncCredential()

        if (
            self._aoai_token
            and self._aoai_token_expires_on - time.time()
            > FORWARDED_TOKEN_MIN_LIFETIME_SECONDS
        ):
            return self._aoai_token

        access = await self._async_credential.get_token(
            "https://cognitiveservices.azure.com/.default"
        )
        # The underlying credential may itself return a near-expiry cached token.
        _require_token_lifetime(access.expires_on, "AZURE_OPENAI_TOKEN")
        self._aoai_token = access.token
        self._aoai_token_expires_on = access.expires_on
        return self._aoai_token

    async def get_foundry_token(self) -> str | None:
        """Acquire an AAD bearer token for the Foundry project data plane
        (audience https://ai.azure.com), valid for provisioning + research.
        Forwarded as AZURE_AI_TOKEN with AZURE_AI_TOKEN_EXPIRES_ON so the
        researcher's FoundryChatClient uses the issuer's actual expiry."""
        if not self.foundry_project_endpoint:
            return None

        if self._async_credential is None:
            self._async_credential = AsyncCredential()

        if (
            self._foundry_token
            and self._foundry_token_expires_on - time.time()
            > FORWARDED_TOKEN_MIN_LIFETIME_SECONDS
        ):
            return self._foundry_token

        access = await self._async_credential.get_token(
            "https://ai.azure.com/.default"
        )
        _require_token_lifetime(access.expires_on, "AZURE_AI_TOKEN")
        self._foundry_token = access.token
        self._foundry_token_expires_on = access.expires_on
        return self._foundry_token

    # ── Sandbox group ──────────────────────────────────────────────────────

    async def ensure_sandbox_group(self, region: str) -> None:
        """
        Verify the sandbox group exists in the requested region. The group itself
        is provisioned by the Bicep template (with managed identity + AcrPull),
        so this just validates and caches the result.
        """
        region = validate_region(region)
        group_name = (
            os.environ.get("SANDBOX_GROUP")
            or region_to_sandbox_group_name(region)
        )

        # Update the active (region, group) so _get_group_client picks them up.
        self.current_sandbox_group_name = group_name
        self.current_region = region

        if group_name in self._provisioned_groups:
            return

        client = self._get_group_client()
        try:
            # Iterating ItemPaged forces the first page request, which validates
            # both connectivity and RBAC on the data plane.
            await asyncio.to_thread(lambda: list(client.list_disk_images()))
        except Exception as ex:
            raise RuntimeError(
                f"Cannot reach sandbox group '{group_name}' in {region}. "
                "Ensure it was provisioned via infra/main.bicep and that this "
                "principal has 'Dev Compute SandboxGroup Data Owner' on it. "
                f"Underlying error: {ex}"
            ) from ex

        self._provisioned_groups.add(group_name)
        logger.info("[EnsureSG] Active group: '%s' in %s", group_name, region)

    # ── ACR digest resolution ───────────────────────────────────────────────

    @staticmethod
    def _parse_image_ref(image_ref: str) -> tuple[str, str, str]:
        """Parse `registry/repo:tag` into (registry, repo, tag)."""
        if "/" not in image_ref:
            raise ValueError(f"Image ref missing registry: {image_ref}")
        registry, rest = image_ref.split("/", 1)
        if "@" in rest:
            repo, tag = rest.split("@", 1)
        elif ":" in rest:
            repo, tag = rest.rsplit(":", 1)
        else:
            repo, tag = rest, "latest"
        return registry, repo, tag

    async def _acr_bearer_via_aad(self, registry: str, repo: str) -> str:
        """Exchange the orchestrator's Entra token for an ACR repo:pull token."""
        if self._async_credential is None:
            self._async_credential = AsyncCredential()
        aad = await self._async_credential.get_token(
            "https://management.azure.com/.default"
        )
        r = await self._http.post(
            f"https://{registry}/oauth2/exchange",
            data={
                "grant_type": "access_token",
                "service": registry,
                "access_token": aad.token,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        r.raise_for_status()
        refresh = r.json().get("refresh_token")
        if not isinstance(refresh, str) or not refresh:
            raise RuntimeError("ACR token exchange did not return a refresh token.")
        r = await self._http.post(
            f"https://{registry}/oauth2/token",
            data={
                "grant_type": "refresh_token",
                "service": registry,
                "scope": f"repository:{repo}:pull",
                "refresh_token": refresh,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        r.raise_for_status()
        token = r.json().get("access_token")
        if not isinstance(token, str) or not token:
            raise RuntimeError("ACR token exchange did not return an access token.")
        return token

    async def resolve_oci_digest(self, image_ref: str | None = None) -> str:
        """
        Resolve `registry/repo:tag` to the current `sha256:<hex>` manifest digest.
        Authenticates with the orchestrator's Entra credential.
        """
        ref = image_ref or self.container_image
        registry, repo, tag = self._parse_image_ref(ref)

        if "@sha256:" in ref:
            return ref.split("@", 1)[1]

        token = await self._acr_bearer_via_aad(registry, repo)

        accept = ", ".join([
            "application/vnd.oci.image.manifest.v1+json",
            "application/vnd.docker.distribution.manifest.v2+json",
            "application/vnd.oci.image.index.v1+json",
            "application/vnd.docker.distribution.manifest.list.v2+json",
        ])
        headers = {"Accept": accept, "Authorization": f"Bearer {token}"}

        manifest_url = f"https://{registry}/v2/{repo}/manifests/{tag}"
        r = await self._http.head(manifest_url, headers=headers)
        r.raise_for_status()
        digest = r.headers.get("Docker-Content-Digest")
        if not digest:
            raise RuntimeError(
                f"Registry did not return Docker-Content-Digest for {ref}"
            )
        return digest

    # ── Disk image (digest-cached, persistent across runs) ────────────────────

    def _create_disk_image_with_labels(self, labels: dict[str, str]) -> DiskImage:
        """Create a disk image carrying our full label dict.

        SDK b4's public method targets the legacy endpoint and drops arbitrary
        labels. Use v2's source.managedIdentityClientId for the sandbox group's
        AcrPull identity, retaining our image-ref, oci-digest, and demo labels.
        """
        client = self._get_group_client()
        body: dict[str, Any] = {
            "labels": labels,
            "source": {
                "kind": "registry",
                "imageUrl": self.container_image,
                "managedIdentityClientId": self.sandbox_group_uami_client_id,
            },
        }
        raw = client._dp_put(f"{client._group_path}/diskimages/v2", body)  # type: ignore[attr-defined]
        return DiskImage._from_dict(raw)

    async def _wait_until_ready(
        self,
        image: DiskImage,
        status_cb,
    ) -> DiskImage:
        client = self._get_group_client()
        for i in range(120):  # up to ~4 minutes
            image = await asyncio.to_thread(client.get_disk_image, image.id)
            state = getattr(image.status, "state", None) if image.status else None
            if state == "Ready":
                return image
            if state == "Failed":
                err = (
                    getattr(image.status, "message", None) or "unknown"
                    if image.status else "unknown"
                )
                if status_cb:
                    await status_cb(f"Disk image build failed: {err}", "error")
                raise RuntimeError(f"Disk image build failed: {err}")
            if i and i % 5 == 0 and status_cb:
                await status_cb(f"Disk image building... ({state or 'pending'})")
            await asyncio.sleep(2)
        if status_cb:
            await status_cb("Disk image build timed out", "error")
        raise TimeoutError("Disk image build timed out")

    async def prepare_disk_image(
        self,
        status_cb=None,
        force: bool = False,
    ) -> str:
        """
        Ensure a Ready disk image exists for the current `DISK_IMAGE_ID` and
        matches the current ACR manifest digest. Persistent across runs.
        """
        if not self.container_image:
            raise RuntimeError("DISK_IMAGE_ID env var is required (full image reference).")

        async def _say(msg: str, level: str = "info") -> None:
            if status_cb:
                await status_cb(msg, level)

        async with self._prepare_lock:
            client = self._get_group_client()

            # 1) Resolve current digest (best-effort — fall back to "always rebuild")
            current_digest: str | None = None
            await _say(f"Resolving current digest for {self.container_image}...")
            try:
                current_digest = await self.resolve_oci_digest(self.container_image)
                await _say(f"Current ACR digest: {current_digest[:19]}...")
            except Exception as ex:
                logger.warning("[Digest] resolve failed; will skip cache match: %s", ex)
                await _say(
                    "Could not resolve ACR digest — will rebuild disk image.",
                    "info",
                )

            # 2) Look for an existing matching disk image
            existing_all = await asyncio.to_thread(
                lambda: list(client.list_disk_images())
            )
            # Filter client-side to our image-ref label.
            existing = [
                img for img in existing_all
                if (img.labels or {}).get(LABEL_IMAGE_REF) == self.container_image
            ]
            ready_match: DiskImage | None = None
            stale: list[DiskImage] = []
            for img in existing:
                img_labels = img.labels or {}
                state = getattr(img.status, "state", None) if img.status else None
                if (
                    not force
                    and current_digest is not None
                    and state == "Ready"
                    and img_labels.get(LABEL_OCI_DIGEST) == current_digest
                    and ready_match is None
                ):
                    ready_match = img
                else:
                    stale.append(img)

            if ready_match is not None:
                self._current_disk_image_id = ready_match.id
                self._current_disk_image_digest = current_digest
                await _say(
                    f"Reusing disk image {ready_match.id} — digest matches ACR",
                    "success",
                )
                if stale:
                    asyncio.create_task(self._delete_images_quietly(stale))
                return ready_match.id

            # 3) Build a new one
            if force:
                await _say("Force-rebuilding disk image (Re-create requested)", "info")
            elif existing:
                await _say(
                    f"ACR has changed (or {len(existing)} stale image(s) present). Rebuilding...",
                    "info",
                )
            else:
                await _say("No existing disk image found. Creating one...", "info")

            await _say(f"Pulling container image from ACR: {self.container_image}")
            await _say("Creating disk image in sandbox group...")

            labels = {
                LABEL_DEMO:       LABEL_DEMO_VALUE,
                LABEL_IMAGE_REF:  self.container_image,
            }
            if current_digest:
                labels[LABEL_OCI_DIGEST] = current_digest
            try:
                image = await asyncio.to_thread(
                    self._create_disk_image_with_labels, labels
                )
            except Exception as ex:
                await _say(f"Disk image creation failed: {ex}", "error")
                raise

            image = await self._wait_until_ready(image, status_cb)

            self._current_disk_image_id = image.id
            self._current_disk_image_digest = current_digest
            await _say(f"Disk image ready: {image.id}", "success")
            logger.info("[DiskImage] Ready: %s", image.id)

            to_prune = [s for s in (stale or []) if s.id != image.id]
            if to_prune:
                await _say(
                    f"Pruning {len(to_prune)} stale disk image(s) for {self.container_image}",
                    "info",
                )
                asyncio.create_task(self._delete_images_quietly(to_prune))

            return image.id

    async def _delete_images_quietly(self, images: list[DiskImage]) -> None:
        client = self._get_group_client()
        for img in images:
            try:
                await asyncio.to_thread(client.delete_disk_image, img.id)
                logger.info("[DiskImage] Pruned %s", img.id)
            except Exception as ex:
                logger.warning("[DiskImage] Prune failed for %s: %s", img.id, ex)

    async def delete_current_disk_image(self, status_cb=None) -> None:
        """Delete the currently cached disk image (used by the UI button)."""
        async def _say(msg: str, level: str = "info") -> None:
            if status_cb:
                await status_cb(msg, level)

        async with self._prepare_lock:
            client = self._get_group_client()
            disk_image_id = self._current_disk_image_id
            if not disk_image_id:
                # Nothing cached; try to find one by image-ref label.
                all_images = await asyncio.to_thread(
                    lambda: list(client.list_disk_images())
                )
                existing = [
                    img for img in all_images
                    if (img.labels or {}).get(LABEL_IMAGE_REF) == self.container_image
                ]
                if not existing:
                    await _say("No disk image to delete.", "info")
                    return
                await _say(
                    f"Deleting {len(existing)} disk image(s) for {self.container_image}...",
                    "info",
                )
                await self._delete_images_quietly(existing)
                await _say("Disk image(s) deleted.", "success")
                return

            self._current_disk_image_id = None
            self._current_disk_image_digest = None
            try:
                await asyncio.to_thread(client.delete_disk_image, disk_image_id)
                await _say(f"Disk image deleted: {disk_image_id}", "success")
            except Exception as ex:
                await _say(f"Delete failed: {ex}", "error")

    async def prune_managed_disk_images(self, status_cb=None) -> int:
        """Delete every disk image carrying our `demo=agents` label."""
        async def _say(msg: str, level: str = "info") -> None:
            if status_cb:
                await status_cb(msg, level)

        client = self._get_group_client()
        all_images = await asyncio.to_thread(
            lambda: list(client.list_disk_images())
        )
        managed = [
            img for img in all_images
            if (img.labels or {}).get(LABEL_DEMO) == LABEL_DEMO_VALUE
        ]
        if not managed:
            await _say("No managed disk images to prune.", "info")
            return 0

        await _say(f"Pruning {len(managed)} disk image(s)...", "info")
        self._current_disk_image_id = None
        self._current_disk_image_digest = None
        await self._delete_images_quietly(managed)
        await _say(f"Pruned {len(managed)} disk image(s).", "success")
        return len(managed)

    def disk_image_info(self) -> dict:
        """Snapshot of the current disk image for the UI panel."""
        return {
            "image_ref":     self.container_image,
            "disk_image_id": self._current_disk_image_id,
            "oci_digest":    self._current_disk_image_digest,
        }

    def stats(self) -> dict:
        """Aggregated runtime stats for the UI panel."""
        now = time.monotonic()
        live_seconds = sum(now - t for t in self._sandbox_started_at.values())
        return {
            "total_sandbox_seconds":  round(self._total_sandbox_seconds + live_seconds, 1),
            "total_sandbox_runs":     self._total_sandbox_runs,
            "live_sandboxes":         len(self._sandbox_started_at),
        }

    # ── Sandbox lifecycle ──────────────────────────────────────────────────

    def _build_egress_policy(self) -> EgressPolicy:
        """Allow AI endpoints and configured telemetry; deny all other egress.

        Web search runs server-side in Foundry, so no search-engine rule is needed.
        """
        host_rules: list[EgressHostRule] = []
        seen: set[str] = set()

        def _allow_pattern(pattern: str) -> None:
            if pattern not in seen:
                seen.add(pattern)
                host_rules.append(EgressHostRule(pattern=pattern, action="Allow"))

        def _allow(url: str | None, include_siblings: bool = True) -> None:
            host = urlparse(url).hostname if url else None
            if not host:
                return
            _allow_pattern(host)
            if include_siblings and "." in host:
                _allow_pattern(f"*.{host.split('.', 1)[1]}")

        # AOAI data-plane host (model inference + AOAI fallback path).
        _allow(self.openai_endpoint)
        # Foundry project host (hosted web search / grounding via FoundryChatClient).
        _allow(self.foundry_project_endpoint)
        # Custom telemetry collectors get exact-host rules, not sibling access.
        for endpoint in self._appinsights_egress_endpoints():
            _allow(endpoint, include_siblings=False)
            host = urlparse(endpoint).hostname
            if host and host.endswith((
                ".applicationinsights.azure.com",
                ".applicationinsights.microsoft.com",
                ".services.visualstudio.com",
                ".livediagnostics.monitor.azure.com",
            )):
                # Azure Monitor can redirect to regional ingestion/live hosts;
                # exporter health metrics also use regional ingestion.
                _allow_pattern("*.in.applicationinsights.azure.com")
                _allow_pattern("*.livediagnostics.monitor.azure.com")
        return EgressPolicy(default_action="Deny", host_rules=host_rules)

    def _appinsights_egress_endpoints(self) -> list[str]:
        """Resolve ingestion/live endpoints using Azure Monitor's defaults."""
        if not self.appinsights_conn:
            return []
        parts: dict[str, str] = {}
        for item in self.appinsights_conn.split(";"):
            if not item.strip():
                continue
            key, separator, value = item.partition("=")
            if not separator or not key.strip() or not value.strip():
                raise ValueError("Invalid Application Insights connection string entry.")
            parts[key.strip().lower()] = value.strip()

        suffix = parts.get("endpointsuffix")
        location = parts.get("location")
        prefix = f"{location}." if location else ""
        endpoints: list[str] = []
        for key, service, default in (
            ("ingestionendpoint", "dc", "https://dc.services.visualstudio.com"),
            ("liveendpoint", "live", "https://rt.services.visualstudio.com"),
        ):
            endpoint = parts.get(key) or (
                f"https://{prefix}{service}.{suffix}" if suffix else default
            )
            try:
                parsed = urlparse(endpoint)
                valid = (
                    parsed.scheme == "https"
                    and bool(parsed.hostname)
                    and not parsed.username
                    and not parsed.password
                    and parsed.port in (None, 443)
                    and not parsed.query
                    and not parsed.fragment
                    and not any(c.isspace() or c == "*" for c in parsed.netloc)
                )
            except ValueError:
                valid = False
            if not valid:
                raise ValueError(
                    f"Application Insights {key} must be an HTTPS endpoint on port 443 "
                    "without credentials, wildcards, query, or fragment."
                )
            if endpoint not in endpoints:
                endpoints.append(endpoint)
        return endpoints

    def _create_sandbox_sync(
        self,
        disk_image_id: str,
        environment: dict[str, str],
        labels: dict[str, str],
    ) -> tuple[SandboxClient, Sandbox]:
        """Create a sandbox via the LRO API and return (client, info).

        Blocks until the sandbox reaches the ``Running`` state.
        """
        client = self._get_group_client()
        poller = client.begin_create_sandbox(
            disk_id=disk_image_id,
            polling_timeout=SANDBOX_PROVISION_TIMEOUT_SECONDS,
            cpu="500m",
            memory="1Gi",
            ports=[AddPortRequest(port=8080, auth=PortAuthConfig(anonymous=True))],
            environment=environment,
            labels=labels,
            egress_policy=self._build_egress_policy(),
        )
        sandbox_client: SandboxClient = poller.result()
        # Fetch the full Sandbox resource (includes the issued port URLs).
        info = sandbox_client.get()
        return sandbox_client, info

    async def create_sandbox(self, sandbox_id: str, question: str) -> None:
        """Create a sandbox running the research agent for the given question."""
        if not self._current_disk_image_id:
            raise RuntimeError(
                "prepare_disk_image() must be called before create_sandbox()."
            )

        environment: dict[str, str] = {"RESEARCH_QUESTION": question}
        if self.openai_endpoint:
            environment["AZURE_OPENAI_ENDPOINT"] = self.openai_endpoint
        if self.openai_deployment:
            environment["AZURE_OPENAI_DEPLOYMENT"] = self.openai_deployment
        # Foundry project + ai.azure.com-scoped token for the hosted web-search tool.
        if self.foundry_project_endpoint:
            environment["FOUNDRY_PROJECT_ENDPOINT"] = self.foundry_project_endpoint

        labels = {
            "demo": "agents",
            "question": question[:50],
        }

        with _span(
            "sandbox.create",
            **{"sandbox.id": sandbox_id, "sandbox.region": self.current_region},
        ):
            # Forward telemetry config + trace context so the in-VM research
            # agent exports to the same App Insights trace as this span.
            if self.appinsights_conn:
                environment["APPLICATIONINSIGHTS_CONNECTION_STRING"] = self.appinsights_conn
                environment["OTEL_SERVICE_NAME"] = "research-agent"
                environment["ENABLE_INSTRUMENTATION"] = "true"
                environment["ENABLE_SENSITIVE_DATA"] = os.environ.get(
                    "ENABLE_SENSITIVE_DATA", "true"
                )
                environment.update(_traceparent_env())

            async with self._throttle:
                token = await self.get_aoai_token()
                if token:
                    environment["AZURE_OPENAI_TOKEN"] = token
                    environment["AZURE_OPENAI_TOKEN_EXPIRES_ON"] = str(
                        self._aoai_token_expires_on
                    )
                foundry_token = await self.get_foundry_token()
                if foundry_token:
                    environment["AZURE_AI_TOKEN"] = foundry_token
                    environment["AZURE_AI_TOKEN_EXPIRES_ON"] = str(
                        self._foundry_token_expires_on
                    )
                # Acquiring the second token may have consumed the first one's
                # budget. Validate the actual pairs being sent, not mutable caches.
                for token_name in ("AZURE_OPENAI_TOKEN", "AZURE_AI_TOKEN"):
                    if token_name in environment:
                        _require_token_lifetime(
                            int(environment[f"{token_name}_EXPIRES_ON"]), token_name
                        )
                sandbox_client, info = await asyncio.to_thread(
                    self._create_sandbox_sync,
                    self._current_disk_image_id,
                    environment,
                    labels,
                )

                external_url = ""
                ports = getattr(info, "ports", None) or []
                if ports:
                    external_url = getattr(ports[0], "url", "") or ""

                self._sandbox_endpoints[sandbox_id] = external_url
                self._sbx_clients[sandbox_id] = sandbox_client
            self._sandbox_started_at[sandbox_id] = time.monotonic()

    async def delete_sandbox(self, sandbox_id: str) -> None:
        sbx_client = self._sbx_clients.pop(sandbox_id, None)
        self._sandbox_endpoints.pop(sandbox_id, None)
        started = self._sandbox_started_at.pop(sandbox_id, None)
        if started is not None:
            self._total_sandbox_seconds += time.monotonic() - started
            self._total_sandbox_runs += 1
        if sbx_client is None:
            return
        try:
            await asyncio.to_thread(sbx_client.delete)
        except Exception as ex:
            logger.warning("[Delete] %s — %s", sbx_client.sandbox_id, ex)
        finally:
            try:
                await asyncio.to_thread(sbx_client.close)
            except Exception:
                pass

    # ── HTTP polling against the research agent inside the sandbox ─────────

    def _agent_base_url(self, sandbox_id: str) -> str:
        url = self._sandbox_endpoints.get(sandbox_id)
        if not url:
            raise RuntimeError(f"No endpoint known for sandbox {sandbox_id}")
        return url.rstrip("/")

    async def get_status(self, sandbox_id: str) -> SandboxStatus:
        url = f"{self._agent_base_url(sandbox_id)}/status"
        r = await self._http.get(url)
        r.raise_for_status()
        d: dict[str, Any] = r.json()
        return SandboxStatus(
            status=d["status"],
            progress=d.get("progress", ""),
            error=d.get("error"),
            has_openai=bool(d.get("has_openai", False)),
        )

    async def get_result(self, sandbox_id: str) -> AgentResult:
        url = f"{self._agent_base_url(sandbox_id)}/result"
        r = await self._http.get(url)
        r.raise_for_status()
        d: dict[str, Any] = r.json()
        return AgentResult(
            question=d["question"],
            answer=d["answer"],
            sources=list(d.get("sources", []) or []),
            confidence=float(d.get("confidence", 0.0)),
            simulated=bool(d.get("simulated", False)),
            hint=d.get("hint"),
            diagnostics=d.get("diagnostics"),
        )

    # ── Egress diagnostics ────────────────────────────────────────────────
    async def probe_egress(
        self,
        question: str = "What is Azure Container Apps?",
        wait_seconds: int = 120,
        status_cb=None,
    ) -> dict[str, Any]:
        """Create a throwaway sandbox, let the research agent make its outbound
        calls, then read back the *stored* egress policy and the egress-decision
        audit log (which hosts were actually Allowed vs Denied). Useful to verify
        the default-deny + AI/telemetry policy is applied and matches the
        endpoints the researcher really talks to. The sandbox is always deleted.
        """
        async def _emit(msg: str, level: str = "info") -> None:
            if status_cb:
                await status_cb(msg, level)

        if not self._current_disk_image_id:
            await _emit("Preparing disk image for probe...")
            await self.prepare_disk_image(status_cb)

        sent_policy = self._build_egress_policy()._to_dict()
        sandbox_id = f"egress-probe-{uuid.uuid4().hex[:8]}"
        await _emit(f"Creating probe sandbox {sandbox_id}...")
        await self.create_sandbox(sandbox_id, question)

        try:
            client = self._sbx_clients[sandbox_id]
            await _emit(
                f"Holding probe sandbox alive for {wait_seconds}s so you can "
                "inspect/curl it (agent runs in parallel)..."
            )
            agent_status: dict[str, Any] | None = None
            agent_done_at: float | None = None
            deadline = time.monotonic() + wait_seconds
            while time.monotonic() < deadline:
                await asyncio.sleep(5)
                try:
                    st = await self.get_status(sandbox_id)
                    agent_status = {"status": st.status, "progress": st.progress}
                    if st.status in ("done", "error") and agent_done_at is None:
                        agent_done_at = time.monotonic()
                        await _emit(
                            f"Agent {st.status}; keeping sandbox {sandbox_id} "
                            "alive for the rest of the window."
                        )
                except Exception:
                    pass

            stored_policy = await asyncio.to_thread(
                lambda: client.get_egress_policy()._to_dict()
            )
            # Run probes before reading the audit log so their decisions appear.
            connectivity = await self._exec_connectivity_test(client)
            decisions = await asyncio.to_thread(client.get_egress_decisions)

            def _entries(items) -> list[dict[str, Any]]:
                return [
                    {"host": e.host, "scheme": e.scheme, "method": e.method, "path": e.path}
                    for e in (items or [])
                ]

            ne = getattr(decisions, "network_egress", None)
            allowed = _entries(getattr(ne, "allowed", None)) if ne else []
            denied = _entries(getattr(ne, "denied", None)) if ne else []

            agent_result: dict[str, Any] | None = None
            try:
                res = await self.get_result(sandbox_id)
                agent_result = {
                    "simulated": res.simulated,
                    "hint": res.hint,
                    "sources": res.sources[:5],
                    "answer_preview": (res.answer or "")[:200],
                }
            except Exception as ex:
                agent_result = {"note": f"result not ready: {ex}"}

            return {
                "sandbox_id": sandbox_id,
                "sent_policy": sent_policy,
                "stored_policy": stored_policy,
                "egress_decisions": {
                    "allowed_hosts": sorted({e["host"] for e in allowed if e["host"]}),
                    "denied_hosts": sorted({e["host"] for e in denied if e["host"]}),
                    "allowed": allowed,
                    "denied": denied,
                },
                "connectivity_test": connectivity,
                "agent_status": agent_status,
                "agent_result": agent_result,
            }
        finally:
            await _emit("Deleting probe sandbox...")
            await self.delete_sandbox(sandbox_id)

    async def _exec_connectivity_test(self, client) -> dict[str, Any]:
        """Run a tiny Python script *inside* the sandbox (via the SDK exec API)
        that tries HTTPS GETs to allowed hosts (AI/telemetry) and denied hosts
        (example.com, bing.com). Proves the default-deny egress policy is
        actually enforced at the network layer, not just stored.
        """
        allowed_hosts = [
            h
            for h in (
                urlparse(self.foundry_project_endpoint).hostname
                if self.foundry_project_endpoint
                else None,
                urlparse(self.openai_endpoint).hostname
                if self.openai_endpoint
                else None,
            )
            if h
        ]
        targets: dict[str, str] = {}
        for h in allowed_hosts:
            targets[f"allowed:{h}"] = f"https://{h}/"
        for endpoint in self._appinsights_egress_endpoints():
            targets[f"telemetry:{urlparse(endpoint).hostname}"] = endpoint
        targets["denied:example.com"] = "https://example.com/"
        targets["denied:bing.com"] = "https://www.bing.com/"

        script = (
            "import json,ssl,urllib.request,urllib.error\n"
            "ctx=ssl.create_default_context();ctx.check_hostname=False;"
            "ctx.verify_mode=ssl.CERT_NONE\n"
            f"targets={targets!r}\n"
            "out={}\n"
            "for k,u in targets.items():\n"
            "    try:\n"
            "        r=urllib.request.urlopen(u,timeout=8,context=ctx)\n"
            "        out[k]={'result':'REACHABLE','status':getattr(r,'status',None)}\n"
            "    except urllib.error.HTTPError as e:\n"
            "        out[k]={'result':'HTTP_RESPONSE','status':e.code,'error':str(e)}\n"
            "        e.close()\n"
            "    except Exception as e:\n"
            "        out[k]={'result':'BLOCKED','error':type(e).__name__+': '+str(e)[:120]}\n"
            "print(json.dumps(out))\n"
        )
        b64 = base64.b64encode(script.encode()).decode()
        cmd = f"python -c \"import base64;exec(base64.b64decode('{b64}'))\""
        try:
            res = await asyncio.to_thread(lambda: client.exec(cmd))
            raw = (res.stdout or "").strip()
            try:
                return {"targets": targets, "results": json.loads(raw)}
            except Exception:
                return {
                    "targets": targets,
                    "raw_stdout": raw[:500],
                    "stderr": (res.stderr or "")[:500],
                    "exit_code": res.exit_code,
                }
        except Exception as ex:
            return {"targets": targets, "error": f"{type(ex).__name__}: {ex}"}


# ── Helpers ─────────────────────────────────────────────────────────────────

def _required(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        raise RuntimeError(
            f"{name} is required (set via env var or .env). See .env.example."
        )
    return val
