"""
Sandbox research runner used directly by a MAF workflow executor.

Each parallel branch provisions a sandbox, polls its research agent, and
returns its JSON result without an outer LLM dispatch or rewrite step.
The manager, per-WebSocket emit callback, and branch index are bound once.
"""
from __future__ import annotations

import asyncio
import json
import logging
import uuid
from typing import Awaitable, Callable

from sandbox_manager import AgentResult, SandboxManager, RESEARCH_TIMEOUT_SECONDS as POLL_TIMEOUT_SECONDS

from .errors import rate_limit_warning, user_error

logger = logging.getLogger(__name__)


EmitFn = Callable[[dict], Awaitable[None]]

# Poll-loop resilience: a single transient error (e.g. a 502 from the ADC
# egress proxy while the sandbox port warms up) must not kill a researcher.
MAX_CONSECUTIVE_POLL_ERRORS = 8


def build_sandbox_researcher(
    agent_id: str,
    sandbox_mgr: SandboxManager,
    emit: EmitFn,
    index: int,
) -> Callable[[str], Awaitable[str]]:
    """Bind a branch's sandbox lifecycle and progress reporting."""
    async def run_in_sandbox(question: str) -> str:
        """Provision an ACA Sandbox, run the research agent, return JSON results."""
        sandbox_id = f"agent-{index}-{uuid.uuid4().hex[:8]}"

        async def log(msg: str, level: str = "info") -> None:
            await emit({"type": "log", "message": msg, "level": level})

        async def agent_status(status: str) -> None:
            await emit({
                "type": "agent",
                "index": index,
                "question": question,
                "status": status,
                "sandboxId": sandbox_id,
            })

        await agent_status("provisioning")
        try:
            await sandbox_mgr.create_sandbox(sandbox_id, question)
        except Exception as ex:
            logger.exception("[%s] create_sandbox failed", agent_id)
            message = user_error(ex)
            await agent_status("error")
            await log(f"Failed to create sandbox: {message}", "error")
            return json.dumps({
                "question": question,
                "answer": "",
                "sources": [],
                "confidence": 0.0,
                "error": message,
            })

        await agent_status("researching")
        await log(f"Sandbox {sandbox_id} running", "success")

        # Status and result retrieval have independent retry budgets.
        result: AgentResult | None = None
        poll_deadline = asyncio.get_event_loop().time() + POLL_TIMEOUT_SECONDS
        consecutive_status_errors = 0
        result_errors = 0
        try:
            heartbeat = 0
            while True:
                await asyncio.sleep(2)
                if asyncio.get_event_loop().time() > poll_deadline:
                    await agent_status("error")
                    await log(
                        f"Agent {index + 1} timed out after {POLL_TIMEOUT_SECONDS}s", "error"
                    )
                    return json.dumps({
                        "question": question,
                        "answer": f"Research timed out after {POLL_TIMEOUT_SECONDS}s.",
                        "sources": [],
                        "confidence": 0.0,
                        "error": "timeout",
                    })
                try:
                    status = await sandbox_mgr.get_status(sandbox_id)
                    consecutive_status_errors = 0
                except Exception as ex:
                    consecutive_status_errors += 1
                    logger.warning(
                        "[%s] get_status transient error %d/%d: %s",
                        agent_id, consecutive_status_errors, MAX_CONSECUTIVE_POLL_ERRORS, ex,
                    )
                    if consecutive_status_errors >= MAX_CONSECUTIVE_POLL_ERRORS:
                        message = user_error(ex)
                        await agent_status("error")
                        await log(
                            f"Agent {index + 1} error: sandbox unreachable ({message})", "error"
                        )
                        return json.dumps({
                            "question": question,
                            "answer": "",
                            "sources": [],
                            "confidence": 0.0,
                            "error": message,
                        })
                    continue
                if status.status == "done":
                    try:
                        result = await sandbox_mgr.get_result(sandbox_id)
                        break
                    except Exception as ex:
                        result_errors += 1
                        logger.warning(
                            "[%s] get_result transient error %d/%d: %s",
                            agent_id, result_errors, MAX_CONSECUTIVE_POLL_ERRORS, ex,
                        )
                        if result_errors >= MAX_CONSECUTIVE_POLL_ERRORS:
                            message = user_error(ex)
                            await agent_status("error")
                            await log(
                                f"Agent {index + 1} error: result unreachable ({message})", "error"
                            )
                            return json.dumps({
                                "question": question,
                                "answer": "",
                                "sources": [],
                                "confidence": 0.0,
                                "error": message,
                            })
                        continue
                if status.status == "error":
                    logger.warning("[%s] Sandbox error: %s", agent_id, status.error or status.progress)
                    message = user_error(status.error or status.progress)
                    await agent_status("error")
                    await log(f"Agent {index + 1} error: {message}", "error")
                    return json.dumps({
                        "question": question,
                        "answer": "",
                        "sources": [],
                        "confidence": 0.0,
                        "error": message,
                    })
                heartbeat += 1
                if heartbeat % 5 == 0:
                    await log(f"Agent {index + 1} still researching...", "info")
        finally:
            try:
                await sandbox_mgr.delete_sandbox(sandbox_id)
            except Exception:
                pass

        await agent_status("done")
        if result.simulated:
            warning = rate_limit_warning(result.diagnostics or result.hint or "")
            if warning:
                logger.warning("[%s] Simulated result diagnostics: %s", agent_id, result.diagnostics)
                result.hint = warning
                result.diagnostics = None
            hint = result.hint or (
                "Sandbox fell back to simulated output because Azure OpenAI was unavailable."
            )
            await log(f"Agent {index + 1} used simulated fallback: {hint}", "warn")
            await emit({
                "type": "hint",
                "severity": "warn",
                "agentIndex": index,
                "title": "Simulated fallback detected",
                "message": hint,
                "diagnostics": result.diagnostics or "",
            })
        await emit({
            "type": "result",
            "index": index,
            "question": result.question,
            "answer": result.answer,
            "sources": result.sources,
            "confidence": result.confidence,
            "simulated": result.simulated,
        })
        await log(f"Agent {index + 1} completed research", "success")

        return json.dumps({
            "question": result.question,
            "answer": result.answer,
            "sources": result.sources,
            "confidence": result.confidence,
            "simulated": result.simulated,
            "hint": result.hint,
            "diagnostics": result.diagnostics,
        })

    return run_in_sandbox
