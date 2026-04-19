"""MCP (Model Context Protocol) tool-call auto-instrumentation.

Patches ``mcp.ClientSession.call_tool`` so every MCP tool invocation
becomes a Prometa ``tool`` span tagged with ``mcp.server.name`` and
``mcp.tool.name``.

Usage::

    from prometa import Prometa
    from prometa.integrations import mcp as prometa_mcp

    prometa = Prometa(endpoint=..., agent_name="my-agent")
    prometa_mcp.install()
"""

from __future__ import annotations

import asyncio
import functools
from typing import Any, Callable, Optional

from ..client import Prometa


_INSTALLED = False


def _client() -> Optional[Prometa]:
    return Prometa._current


def _wrap_call_tool(cls: type) -> None:
    original = getattr(cls, "call_tool", None)
    if original is None or getattr(original, "__prometa_wrapped__", False):
        return

    is_async = asyncio.iscoroutinefunction(original)

    if is_async:

        @functools.wraps(original)
        async def aw(self, name, arguments=None, *args, **kwargs):  # type: ignore[no-redef]
            client = _client()
            if client is None:
                return await original(self, name, arguments, *args, **kwargs)
            server_name = getattr(self, "server_name", None) or getattr(
                getattr(self, "server", None), "name", ""
            )
            with client._span("tool", f"mcp.call_tool:{name}") as span:
                span.attributes.update(
                    {
                        "gen_ai.framework": "mcp",
                        "mcp.tool.name": str(name),
                        "mcp.server.name": str(server_name or ""),
                    }
                )
                if isinstance(arguments, dict):
                    span.attributes["mcp.tool.args_count"] = len(arguments)
                try:
                    return await original(self, name, arguments, *args, **kwargs)
                except Exception as e:
                    span.status = "error"
                    span.attributes["error.message"] = str(e)
                    raise

        wrapped: Callable = aw
    else:

        @functools.wraps(original)
        def sw(self, name, arguments=None, *args, **kwargs):  # type: ignore[no-redef]
            client = _client()
            if client is None:
                return original(self, name, arguments, *args, **kwargs)
            server_name = getattr(self, "server_name", None) or getattr(
                getattr(self, "server", None), "name", ""
            )
            with client._span("tool", f"mcp.call_tool:{name}") as span:
                span.attributes.update(
                    {
                        "gen_ai.framework": "mcp",
                        "mcp.tool.name": str(name),
                        "mcp.server.name": str(server_name or ""),
                    }
                )
                if isinstance(arguments, dict):
                    span.attributes["mcp.tool.args_count"] = len(arguments)
                try:
                    return original(self, name, arguments, *args, **kwargs)
                except Exception as e:
                    span.status = "error"
                    span.attributes["error.message"] = str(e)
                    raise

        wrapped = sw

    wrapped.__prometa_wrapped__ = True  # type: ignore[attr-defined]
    setattr(cls, "call_tool", wrapped)


def install() -> bool:
    """Patch ``mcp.ClientSession.call_tool``. Returns True if patched,
    False if the ``mcp`` package isn't importable."""
    global _INSTALLED
    if _INSTALLED:
        return True

    try:
        from mcp.client.session import ClientSession  # type: ignore
    except Exception:
        try:
            from mcp import ClientSession  # type: ignore
        except Exception:  # pragma: no cover - MCP SDK not installed
            return False

    try:
        _wrap_call_tool(ClientSession)
    except Exception:
        return False

    _INSTALLED = True
    return True


def _noop_import_guard() -> Any:
    """Reference to silence linters about the unused `Any` import."""
    return Any
