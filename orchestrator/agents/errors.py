"""Short user-facing errors; full exceptions belong in server logs."""
from __future__ import annotations

import os
import re


def rate_limit_warning(error: object) -> str | None:
    messages = [str(error)]
    seen: set[int] = set()
    while isinstance(error, BaseException) and id(error) not in seen:
        seen.add(id(error))
        error = error.__cause__ or error.__context__
        if error is not None:
            messages.append(str(error))
    text = " ".join(messages)
    if not re.search(
        r"token[\s_-]*rate|tokens per minute|rate[\s_-]*limit.*token|token.*rate[\s_-]*limit",
        text, re.IGNORECASE,
    ):
        return None
    match = re.search(
        r"(?:call|requests?) to ([\w.-]+)(?: for [\w.-]+)? in ([\w-]+)",
        text, re.IGNORECASE,
    )
    model = match[1] if match else os.environ.get("AZURE_OPENAI_DEPLOYMENT", "the model")
    region = f" in {match[2]}" if match else ""
    return (
        f"HEADS UP: Your call to {model}{region} exceeded the token rate limit. "
        'Edit "Tokens per Minute Rate Limit" for this deployment in Foundry, '
        "or wait and retry."
    )


def user_error(error: object) -> str:
    warning = rate_limit_warning(error)
    if warning:
        return warning
    if isinstance(error, BaseException):
        seen: set[int] = set()
        while id(error) not in seen:
            seen.add(id(error))
            cause = error.__cause__ or error.__context__
            if cause is None:
                break
            error = cause
    message = " ".join(str(error).split())
    if not message or len(message) > 240 or "Traceback" in message:
        return "Research request failed. See the server logs for details."
    return message
