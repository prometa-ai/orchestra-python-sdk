"""Minimal official-SDK stdio server used by the runtime MCP contract test."""

from mcp.server.fastmcp import FastMCP


server = FastMCP("prometa-runtime-mcp-test", log_level="ERROR")


@server.tool(name="echo_runtime")
def echo_runtime(value: str):
    return {"echo": value}


if __name__ == "__main__":
    server.run(transport="stdio")
