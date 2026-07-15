"""Authenticated read-only MCP fixture for the K3s reference profile."""

from __future__ import annotations

import argparse
import hmac
import os
import threading
from pathlib import Path
from typing import Optional, Sequence

from mcp.server.auth.provider import AccessToken
from mcp.server.auth.settings import AuthSettings
from mcp.server.fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse


class FileTokenVerifier:
    """Read the projected Secret for every request so server rotation is atomic."""

    def __init__(self, path: Path) -> None:
        self._path = path

    async def verify_token(self, token: str) -> Optional[AccessToken]:
        try:
            expected = self._path.read_text(encoding="utf-8").strip()
        except OSError:
            return None
        if not expected or not hmac.compare_digest(token, expected):
            return None
        return AccessToken(
            token=token,
            client_id="prometa-topology-runtime",
            scopes=["tools:read"],
        )


class CallState:
    def __init__(self) -> None:
        self._count = 0
        self._lock = threading.Lock()

    def increment(self) -> int:
        with self._lock:
            self._count += 1
            return self._count

    def count(self) -> int:
        with self._lock:
            return self._count


def serve(tenant: str, port: int, token_file: Path) -> None:
    state = CallState()
    resource_url = "http://mcp-integration.tools-%s.svc.cluster.local:%s" % (
        tenant,
        port,
    )
    server = FastMCP(
        "prometa-topology-mcp-%s" % tenant,
        host="0.0.0.0",
        port=port,
        streamable_http_path="/mcp",
        json_response=True,
        stateless_http=True,
        log_level="ERROR",
        token_verifier=FileTokenVerifier(token_file),
        auth=AuthSettings(
            issuer_url="https://tenant-idp.example.test",
            resource_server_url=resource_url,
            required_scopes=["tools:read"],
        ),
    )

    @server.tool(name="lookup_tenant")
    def lookup_tenant(requestId: str):
        state.increment()
        return {"tenant": "tenant-%s" % tenant, "requestId": requestId}

    @server.custom_route("/healthz", methods=["GET"])
    async def healthz(_request: Request):
        return JSONResponse({"status": "ok"})

    @server.custom_route("/count", methods=["GET"])
    async def count(_request: Request):
        return JSONResponse({"count": state.count()})

    server.run(transport="streamable-http")


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="topology_mcp_server.py")
    parser.add_argument("--tenant", required=True, choices=("a", "b"))
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument(
        "--token-file",
        type=Path,
        default=Path(
            os.environ.get(
                "PROMETA_TOPOLOGY_MCP_TOKEN_FILE",
                "/var/run/secrets/prometa-mcp/token",
            )
        ),
    )
    args = parser.parse_args(argv)
    serve(args.tenant, args.port, args.token_file)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
