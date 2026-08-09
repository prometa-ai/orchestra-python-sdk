"""Rolling-window guarding for streamed ``llm_output``.

Streaming cannot be guarded by buffering the whole completion without
destroying the point of streaming, so the caller holds back the tail of what it
has emitted and evaluates a window that always overlaps the previous one. Text
already flushed cannot be recalled; that is inherent to streaming and belongs
in the profile documentation, not in a comment here.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

from .profiles import (
    DEFAULT_FLUSH_HOLDBACK_CHARS,
    GuardrailProfile,
    GuardrailProfileError,
)


@dataclass(frozen=True)
class StreamWindow:
    """One evaluation window plus the prefix it makes safe to release."""

    text: str
    releasable: str
    final: bool


class StreamingGuardWindow:
    """Holds back ``holdback_chars`` so a straddling pattern is still caught."""

    def __init__(self, holdback_chars: int = DEFAULT_FLUSH_HOLDBACK_CHARS) -> None:
        if holdback_chars < 1:
            raise ValueError("holdback_chars must be positive")
        self._holdback = holdback_chars
        self._pending = ""
        self._closed = False

    @property
    def pending(self) -> str:
        return self._pending

    def push(self, chunk: str) -> StreamWindow:
        if self._closed:
            raise ValueError("stream window is already closed")
        window = self._pending + chunk
        if len(window) <= self._holdback:
            self._pending = window
            return StreamWindow(text=window, releasable="", final=False)
        releasable = window[: -self._holdback]
        self._pending = window[-self._holdback :]
        return StreamWindow(text=window, releasable=releasable, final=False)

    def finish(self) -> StreamWindow:
        self._closed = True
        window = self._pending
        self._pending = ""
        return StreamWindow(text=window, releasable=window, final=True)


def stream_windows(
    chunks: Tuple[str, ...], holdback_chars: int = DEFAULT_FLUSH_HOLDBACK_CHARS
) -> Tuple[StreamWindow, ...]:
    """Run a whole chunk sequence through the window, including the final one."""

    guard = StreamingGuardWindow(holdback_chars)
    windows = [guard.push(chunk) for chunk in chunks]
    windows.append(guard.finish())
    return tuple(windows)


def streaming_guard_window(profile: GuardrailProfile) -> StreamingGuardWindow:
    """Open a window under one profile, or refuse the stream outright.

    Both profile settings are read here and nowhere else. A ``streaming`` or
    ``flushHoldbackChars`` value that no code path reads is configuration an
    operator believes is enforcement, which is worse than not offering it.
    """

    if profile.streaming == "deny":
        raise GuardrailProfileError("guardrail_profile_streaming_denied")
    return StreamingGuardWindow(profile.flush_holdback_chars)


__all__ = [
    "StreamWindow",
    "StreamingGuardWindow",
    "stream_windows",
    "streaming_guard_window",
]
