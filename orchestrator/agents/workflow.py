"""
Research Workflow — MAF Workflow that orchestrates the swarm pipeline.

Shape::

    [DecomposeExecutor]
            ↓
       (1 message per question)
            ↓
    [Researcher_0] [Researcher_1] ... [Researcher_N]   ← fan-out
            ↓        ↓                    ↓
            └────────┴───── fan-in ──────┘
                            ↓
                  [SynthesizeExecutor]
                            ↓
                       final markdown
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Awaitable, Callable

from agent_framework import (
    AgentExecutorRequest,
    AgentExecutorResponse,
    AgentResponse,
    Executor,
    Message,
    Workflow,
    WorkflowBuilder,
    WorkflowContext,
    handler,
)
from typing_extensions import Never

from sandbox_manager import SandboxManager

from .decomposer_agent import build_decomposer_agent
from .errors import user_error
from .researcher_agent import build_sandbox_researcher
from .synthesizer_agent import build_synthesizer_agent

logger = logging.getLogger(__name__)

EmitFn = Callable[[dict], Awaitable[None]]

# A workflow with too many parallel researchers is expensive and may hit AOAI
# rate limits. Cap the fan-out width here.
MAX_RESEARCHERS = 6


@dataclass
class ResearchInput:
    """Input passed to the workflow start executor."""
    topic: str


# ── Stage 1: Decompose ──────────────────────────────────────────────────────

class DecomposeExecutor(Executor):
    """
    Asks the decomposer agent for sub-questions, then dispatches one
    AgentExecutorRequest per question to the parallel researcher executors.
    """

    def __init__(self, id: str = "decomposer"):
        super().__init__(id=id)
        self._agent = build_decomposer_agent()

    @handler
    async def decompose(
        self,
        payload: ResearchInput,
        ctx: WorkflowContext[AgentExecutorRequest, list[str]],
    ) -> None:
        # 1) Ask the LLM to decompose
        result = await self._agent.run(payload.topic)
        raw = (result.text or "").strip()
        questions = _parse_questions(raw, fallback_topic=payload.topic)
        # Cap at MAX_RESEARCHERS; researchers list is built to match this size
        questions = questions[:MAX_RESEARCHERS]

        # Surface the questions as a workflow output (events) so the UI gets them
        await ctx.yield_output(questions)

        # Fixed fan-in needs an acknowledgement from unused pool slots too.
        for i in range(MAX_RESEARCHERS):
            await ctx.send_message(
                AgentExecutorRequest(
                    messages=[Message("user", [questions[i]])] if i < len(questions) else [],
                    should_respond=i < len(questions),
                ),
                target_id=f"researcher_{i}",
            )


# ── Stage 2: Researchers (one deterministic Executor per branch) ────────────

class ResearcherExecutor(Executor):
    """Run sandbox research directly; failed branches still reach the fan-in."""

    def __init__(self, sandbox_mgr: SandboxManager, emit: EmitFn, index: int):
        self._emit = emit
        self._index = index
        self._completed_finding: dict | None = None

        async def capture(payload: dict) -> None:
            if payload.get("type") == "result" and payload.get("index") == index:
                self._completed_finding = {
                    key: payload[key]
                    for key in ("question", "answer", "sources", "confidence", "simulated")
                    if key in payload
                }
            await emit(payload)

        agent_id = f"researcher_{index}"
        super().__init__(id=agent_id)
        self._run_in_sandbox = build_sandbox_researcher(agent_id, sandbox_mgr, capture, index)

    @handler
    async def run(
        self,
        request: AgentExecutorRequest,
        ctx: WorkflowContext[AgentExecutorResponse, Never],
    ) -> None:
        self._completed_finding = None
        if not request.should_respond:
            await ctx.send_message(AgentExecutorResponse(
                executor_id=self.id,
                agent_response=AgentResponse(additional_properties={"skipped": True}),
                full_conversation=[],
            ))
            return
        try:
            text = await self._run_in_sandbox(request.messages[-1].text)
        except Exception as ex:
            logger.exception("[%s] Sandbox researcher failed", self.id)
            message = user_error(ex)
            finding = self._completed_finding
            if finding and finding.get("answer") and not finding.get("simulated"):
                outcome = "Keeping its completed sandbox answer."
            else:
                finding = {
                    "question": request.messages[-1].text if request.messages else "",
                    "answer": "",
                    "sources": [],
                    "error": message,
                }
                outcome = "Continuing with the other researchers."
                await self._emit({
                    "type": "agent", "index": self._index, "status": "error",
                })
            await self._emit({
                "type": "log", "level": "warn",
                "message": f"Agent {self._index + 1}: {message}",
            })
            await self._emit({
                "type": "log", "level": "warn",
                "message": f"Agent {self._index + 1}: {outcome}",
            })
            text = json.dumps(finding)
        reply = Message("assistant", [text])
        await ctx.send_message(AgentExecutorResponse(
            executor_id=self.id,
            agent_response=AgentResponse(messages=[reply]),
            full_conversation=[*request.messages, reply],
        ))


def _build_researcher_executors(
    sandbox_mgr: SandboxManager,
    emit: EmitFn,
) -> list[ResearcherExecutor]:
    """
    Build a fixed pool of MAX_RESEARCHERS sandbox researcher executors. Each one
    has a unique id (`researcher_0`, `researcher_1`, ...) so DecomposeExecutor
    can target by index. Each runner binds its sandbox manager + emit callback.
    """
    return [ResearcherExecutor(sandbox_mgr, emit, i) for i in range(MAX_RESEARCHERS)]


# ── Stage 3: Synthesize (fan-in) ────────────────────────────────────────────

class SynthesizeExecutor(Executor):
    """
    Aggregates all researcher AgentExecutorResponses, asks the synthesizer
    agent to produce a markdown report, then yields it as the workflow output.
    """

    def __init__(self, emit: EmitFn, id: str = "synthesizer"):
        super().__init__(id=id)
        self._agent = build_synthesizer_agent()
        self._emit = emit

    @handler
    async def synthesize(
        self,
        responses: list[AgentExecutorResponse],
        ctx: WorkflowContext[Never, dict],
    ) -> None:
        # Each response.text from a researcher is the JSON our sandbox runner
        # returned. Parse them back into structured findings.
        responses = [
            r for r in responses
            if not (r.agent_response.additional_properties or {}).get("skipped")
        ]
        findings: list[dict] = []
        for agent_number, r in enumerate(responses, start=1):
            text = (r.agent_response.text or "").strip()
            text = _strip_code_fences(text)
            try:
                finding = json.loads(text)
                if not isinstance(finding, dict):
                    raise ValueError("Research output must be an object.")
                if finding.get("error") or finding.get("simulated"):
                    continue
                answer = finding.get("answer")
                if not isinstance(answer, str) or not answer.strip():
                    raise ValueError("Research output has no answer.")
                sources = finding.get("sources", [])
                if not isinstance(sources, list) or not all(isinstance(s, str) for s in sources):
                    raise ValueError("Research sources must be strings.")
                finding["confidence"] = float(finding.get("confidence", 0))
                finding["agent_number"] = agent_number
                findings.append(finding)
            except (ValueError, TypeError):
                logger.warning("[%s] Invalid research output: %r", r.executor_id, text)
                await self._emit({
                    "type": "log", "level": "warn",
                    "message": f"{r.executor_id} returned unusable research output; excluded from report.",
                })

        total = len(responses)
        successful = len(findings)
        if not findings:
            await ctx.yield_output({
                "markdown": (
                    "## Research unavailable\n\n"
                    "No researcher returned a usable answer. Failed or simulated "
                    "results were not used. Check the warnings above and retry."
                ),
                "successful": 0, "total": total, "compiled": False,
            })
            return

        notice = ""
        if successful < total:
            notice = (
                f"Partial report: {successful} of {total} researchers provided usable answers. "
                "Failed or simulated results were excluded."
            )
            await self._emit({"type": "log", "message": notice, "level": "warn"})

        # Build the synthesis prompt
        prompt_lines: list[str] = ["## Individual Agent Findings\n"]
        for f in findings:
            prompt_lines.append(f"### Agent {f['agent_number']}: {f.get('question', '')}")
            prompt_lines.append(f"**Confidence:** {float(f.get('confidence', 0)):.0%}\n")
            prompt_lines.append(str(f.get("answer", "")))
            srcs = f.get("sources") or []
            if srcs:
                prompt_lines.append("\n**Sources:** " + ", ".join(srcs))
            prompt_lines.append("")

        compiled = False
        await self._emit({"type": "synthesis", "status": "generating"})
        try:
            result = await self._agent.run("\n".join(prompt_lines))
            markdown = (result.text or "").strip()
            if not markdown:
                raise ValueError("Synthesis returned no report.")
        except Exception as ex:
            logger.exception("[Synthesis] Showing collected answers instead")
            await self._emit({"type": "log", "message": user_error(ex), "level": "warn"})
            await self._emit({
                "type": "log", "level": "warn",
                "message": "AI synthesis unavailable; compiling completed answers without another model call.",
            })
            compiled = True
            await self._emit({"type": "synthesis", "status": "compiling"})
            markdown = (
                "## Collected research\n\n"
                "> AI synthesis was unavailable. These are the completed researcher "
                "answers, not an AI-synthesized report.\n\n"
                + "\n".join(prompt_lines)
            )
        await ctx.yield_output({
            "markdown": (f"> {notice}\n\n" if notice else "") + markdown,
            "successful": successful, "total": total, "compiled": compiled,
        })


# ── Build & expose ──────────────────────────────────────────────────────────

def build_research_workflow(
    sandbox_mgr: SandboxManager,
    emit: EmitFn,
) -> Workflow:
    """
    Construct the full fan-out/fan-in workflow:
        decomposer → [researcher_0..N-1] → synthesizer

    The workflow is built per request so each sandbox runner closes over
    the right WebSocket emit callback.
    """
    decomposer  = DecomposeExecutor()
    researchers = _build_researcher_executors(sandbox_mgr, emit)
    synthesizer = SynthesizeExecutor(emit)

    # NOTE: We use individual edges (decomposer → researcher_i) instead of a
    # single `add_fan_out_edges` group on purpose. MAF's fan-out edge runner
    # delivers the N targeted messages sequentially within a single edge
    # runner (`for message in source_messages: await deliver(...)`), which
    # serializes the sandbox runs for all 6 researchers. Using 6 separate edge
    # runners lets MAF parallelize them via `asyncio.gather` in the runner.
    builder = WorkflowBuilder(start_executor=decomposer)
    for r in researchers:
        builder = builder.add_edge(decomposer, r)
    wf = builder.add_fan_in_edges(list(researchers), synthesizer).build()
    return wf


# ── helpers ─────────────────────────────────────────────────────────────────

_FENCE_RE = re.compile(r"^```(?:json)?\s*\n?|\n?```$", re.MULTILINE)


def _strip_code_fences(text: str) -> str:
    return _FENCE_RE.sub("", text).strip()


def _parse_questions(raw: str, fallback_topic: str) -> list[str]:
    raw = _strip_code_fences(raw)
    try:
        data = json.loads(raw)
        if isinstance(data, list) and all(isinstance(q, str) for q in data):
            return data
    except json.JSONDecodeError:
        logger.warning("[Decompose] Could not parse LLM response: %r", raw[:200])
    # Fallback simulation
    return [
        f"What is the current state of {fallback_topic}?",
        f"What are the key challenges facing {fallback_topic}?",
        f"What recent breakthroughs have occurred in {fallback_topic}?",
        f"How does {fallback_topic} compare to alternative approaches?",
        f"What is the future outlook for {fallback_topic}?",
    ]
