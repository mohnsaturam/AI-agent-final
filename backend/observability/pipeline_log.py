"""
Pipeline Execution Logger — Human-Readable File Logs (REFACTORED)

Writes a detailed, timestamped log file for every pipeline execution.
One file per request: logs/<request_id>.log

Captures the COMPLETE A-to-Z journey:
  - User query, domain, sites
  - Every step of the 13-step flow with elapsed time
  - AI calls: prompt summary + parsed response
  - Intent JSON (full dump)
  - Cache decisions (hit/miss, hash values)
  - robots.txt compliance details (allowed/blocked paths)
  - URL discovery: each attempt, source, score, failure reason
  - DOM fetch: status code, HTML size, latency
  - Extraction: method, fields, confidence
  - Validation: errors, warnings
  - Unification: conflicts resolved, confidence
  - Final result summary
  - Per-step and total latency
"""

import json
import os
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

logger = logging.getLogger("umsa.pipeline_log")

# Log directory — created on first write
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")


def _fmt_json(data, max_lines=30):
    """Format JSON data compactly for log output, truncating if too large."""
    if not data:
        return "(empty)"
    try:
        text = json.dumps(data, indent=2, default=str, ensure_ascii=False)
        lines = text.split("\n")
        if len(lines) > max_lines:
            lines = lines[:max_lines] + [f"  ... ({len(lines) - max_lines} more lines truncated)"]
        return "\n".join(lines)
    except Exception:
        return str(data)[:500]


def _fmt_ms(ms):
    """Format milliseconds into a human-readable string."""
    if ms < 1000:
        return f"{ms}ms"
    elif ms < 60000:
        return f"{ms / 1000:.1f}s"
    else:
        return f"{ms / 60000:.1f}min"


class PipelineFileLogger:
    """
    Per-request file logger.
    Call step()/substep()/detail() at each pipeline step.
    Automatically handles file creation and timestamping.
    """

    def __init__(self, request_id: UUID, query: str, domain: str, sites: list):
        self._request_id = str(request_id)
        self._query = query
        self._domain = domain
        self._sites = sites or []
        self._log_path = os.path.join(LOG_DIR, f"{self._request_id}.log")
        self._step_count = 0
        self._start_time = time.monotonic()
        self._step_start: Optional[float] = None
        self._ai_call_count = 0
        self._total_ai_ms = 0

        # Ensure log directory exists
        os.makedirs(LOG_DIR, exist_ok=True)

        # Write the header
        self._write_header()

    def _ts(self) -> str:
        """Current timestamp."""
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    def _elapsed(self) -> str:
        """Elapsed since pipeline start."""
        ms = int((time.monotonic() - self._start_time) * 1000)
        return _fmt_ms(ms)

    def _step_elapsed(self) -> str:
        """Elapsed since last step start."""
        if self._step_start is None:
            return ""
        ms = int((time.monotonic() - self._step_start) * 1000)
        return f" ({_fmt_ms(ms)})"

    def _write(self, text: str) -> None:
        """Append text to the log file."""
        try:
            with open(self._log_path, "a", encoding="utf-8") as f:
                f.write(text)
        except Exception as e:
            logger.warning("Failed to write pipeline log: %s", e)

    def _write_header(self) -> None:
        """Write the request header block."""
        header = (
            f"{'═' * 90}\n"
            f"  PIPELINE EXECUTION LOG\n"
            f"{'═' * 90}\n"
            f"  Request ID : {self._request_id}\n"
            f"  Query      : \"{self._query}\"\n"
            f"  Domain     : {self._domain}\n"
            f"  Sites      : {', '.join(self._sites) if self._sites else 'None'}\n"
            f"  Started At : {self._ts()}\n"
            f"{'═' * 90}\n\n"
        )
        self._write(header)

    # ─── Major Step ─────────────────────────────────────────────

    def step(
        self,
        step_num: int,
        step_name: str,
        status: str,
        details: str = "",
        data: Optional[dict] = None,
        ai_call: bool = False,
        elapsed_ms: Optional[int] = None,
    ) -> None:
        """
        Log a major step in the pipeline.

        Args:
            step_num: Step number (1-13)
            step_name: Human-readable step name
            status: OK, SKIPPED, BLOCKED, FAILED, CACHE_HIT, etc.
            details: Free-form details
            data: Structured data to dump (intent JSON, cache result, etc.)
            ai_call: Whether this step made an AI call
            elapsed_ms: Override elapsed time for this step
        """
        self._step_count += 1
        self._step_start = time.monotonic()

        status_icon = {
            "OK": "✅", "STARTED": "🔄", "SKIPPED": "⏭️", "BLOCKED": "🚫",
            "FAILED": "❌", "CACHE_HIT": "💾", "WARNING": "⚠️",
        }.get(status, "ℹ️")

        if ai_call:
            self._ai_call_count += 1

        time_str = ""
        if elapsed_ms is not None:
            time_str = f" [{_fmt_ms(elapsed_ms)}]"

        ai_tag = " [AI CALL]" if ai_call else ""

        block = (
            f"[{self._ts()}] ── Step {step_num}: {step_name} ── "
            f"{status_icon} {status}{time_str}{ai_tag}\n"
        )

        if details:
            for line in details.strip().split("\n"):
                block += f"  │ {line}\n"

        if data:
            formatted = _fmt_json(data)
            for line in formatted.split("\n"):
                block += f"  │   {line}\n"

        block += "\n"
        self._write(block)

    # ─── Substep ────────────────────────────────────────────────

    def substep(
        self,
        label: str,
        status: str,
        details: str = "",
        data: Optional[dict] = None,
        elapsed_ms: Optional[int] = None,
    ) -> None:
        """Log a substep within a major step (per-site, per-URL, etc.)."""
        status_icon = {
            "OK": "✅", "SKIPPED": "⏭️", "BLOCKED": "🚫",
            "FAILED": "❌", "WARNING": "⚠️",
        }.get(status, "ℹ️")

        time_str = f" [{_fmt_ms(elapsed_ms)}]" if elapsed_ms else ""

        block = f"  [{self._ts()}]   ├─ {label}: {status_icon} {status}{time_str}\n"
        if details:
            for line in details.strip().split("\n"):
                block += f"  │     {line}\n"
        if data:
            formatted = _fmt_json(data, max_lines=15)
            for line in formatted.split("\n"):
                block += f"  │       {line}\n"
        block += "\n"
        self._write(block)

    # ─── Detail (for extra info within a step) ──────────────────

    def detail(self, label: str, value: Any) -> None:
        """Log a key-value detail within the current step."""
        if isinstance(value, (dict, list)):
            formatted = _fmt_json(value, max_lines=20)
            block = f"  │ {label}:\n"
            for line in formatted.split("\n"):
                block += f"  │   {line}\n"
        else:
            block = f"  │ {label}: {value}\n"
        self._write(block)

    def info(self, message: str) -> None:
        """Log a plain narrative line to explain what's happening."""
        if not message:
            return
        for line in message.strip().split("\n"):
            self._write(f"  │ ℹ️ {line}\n")

    def ai_reasoning(self, reasoning: str) -> None:
        """Log AI reasoning/COT in a distinct style."""
        if not reasoning:
            return
        self._write(f"  │ 🧠 AI Reasoning:\n")
        for line in reasoning.strip().split("\n"):
            self._write(f"  │    {line}\n")

    def robots_event(self, site: str, path: str, status: str, rule: str = "") -> None:
        """Log a specific robots.txt compliance event."""
        icon = "🚫" if status == "BLOCKED" else "✅"
        msg = f"{icon} robots.txt {status}: {path}"
        if rule:
            msg += f" (Rule: {rule})"
        self._write(f"  │ {msg}\n")

    # ─── Section separator ──────────────────────────────────────

    def separator(self, label: str = "") -> None:
        """Write a visual separator."""
        if label:
            pad = max(0, 85 - len(label))
            self._write(f"\n  {'─' * 5} {label} {'─' * pad}\n\n")
        else:
            self._write(f"\n  {'─' * 90}\n\n")

    # ─── Finish ─────────────────────────────────────────────────

    def finish(
        self,
        status: str,
        total_ms: int,
        summary: Optional[dict] = None,
        ai_call_count: Optional[int] = None,
        sources: Optional[list] = None,
        error: Optional[str] = None,
    ) -> None:
        """Write the final result block."""
        actual_ai_calls = ai_call_count if ai_call_count is not None else self._ai_call_count

        block = (
            f"\n{'═' * 90}\n"
            f"  PIPELINE RESULT: {status}\n"
            f"{'═' * 90}\n"
            f"  Request ID    : {self._request_id}\n"
            f"  Query         : \"{self._query}\"\n"
            f"  Domain        : {self._domain}\n"
            f"  Sites         : {', '.join(self._sites)}\n"
            f"  Final Status  : {status}\n"
            f"  Total Time    : {_fmt_ms(total_ms)} ({total_ms}ms)\n"
            f"  AI Calls Made : {actual_ai_calls}\n"
        )

        if sources:
            block += f"  Data Sources  : {', '.join(sources)}\n"

        if error:
            block += f"  Error         : {error}\n"

        block += f"  Completed At  : {self._ts()}\n"

        if summary:
            block += f"\n  {'─' * 40} Result Summary {'─' * 34}\n"
            for k, v in summary.items():
                if isinstance(v, (dict, list)):
                    formatted = _fmt_json(v, max_lines=25)
                    block += f"  {k}:\n"
                    for line in formatted.split("\n"):
                        block += f"    {line}\n"
                else:
                    block += f"  {k}: {v}\n"

        block += f"{'═' * 90}\n"
        self._write(block)
