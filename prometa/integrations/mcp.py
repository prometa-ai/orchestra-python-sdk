"""MCP (Model Context Protocol) tool-call auto-instrumentation.

Patches ``mcp.ClientSession.call_tool`` so every MCP tool invocation
becomes a Prometa ``tool`` span tagged with ``mcp.server.name`` and
``mcp.tool.name`` plus Prometa/GenAI tool-name aliases. When the
process-wide raw channel is enabled, the wrapper also captures
truncated tool arguments/results as ``prometa.raw.*`` attributes.

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

from .. import _raw_channel
from ..client import Prometa
from . import _llm_common as _llm


_INSTALLED = False


def _client() -> Optional[Prometa]:
    return Prometa._current


def _server_name(session: Any) -> str:
    return str(
        getattr(session, "server_name", None)
        or getattr(getattr(session, "server", None), "name", "")
        or ""
    )


def _initialize_tool_span(span: Any, name: Any, server_name: str, arguments: Any) -> None:
    tool_name = str(name)
    span.attributes.update(
        {
            "gen_ai.framework": "mcp",
            "mcp.tool.name": tool_name,
            "gen_ai.tool.name": tool_name,
            "prometa.tool_name": tool_name,
            "mcp.server.name": server_name,
        }
    )
    if isinstance(arguments, dict):
        span.attributes["mcp.tool.args_count"] = len(arguments)
    if _raw_channel.is_enabled() and arguments is not None:
        span.attributes["prometa.raw.input"] = _serialize_payload(arguments)


def _record_tool_result(span: Any, result: Any) -> None:
    if _raw_channel.is_enabled():
        span.attributes["prometa.raw.output"] = _serialize_payload(result)


def _serialize_payload(value: Any) -> str:
    value = _jsonable(value)
    return _llm.truncate(_llm.safe_json(value))


def _jsonable(value: Any) -> Any:
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        try:
            return model_dump(mode="json")
        except TypeError:
            try:
                return model_dump()
            except Exception:
                return value
        except Exception:
            return value
    as_dict = getattr(value, "dict", None)
    if callable(as_dict):
        try:
            return as_dict()
        except Exception:
            return value
    return value


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
            server_name = _server_name(self)
            with client._span("tool", f"mcp.call_tool:{name}") as span:
                _initialize_tool_span(span, name, server_name, arguments)
                try:
                    result = await original(self, name, arguments, *args, **kwargs)
                    _record_tool_result(span, result)
                    return result
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
            server_name = _server_name(self)
            with client._span("tool", f"mcp.call_tool:{name}") as span:
                _initialize_tool_span(span, name, server_name, arguments)
                try:
                    result = original(self, name, arguments, *args, **kwargs)
                    _record_tool_result(span, result)
                    return result
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
