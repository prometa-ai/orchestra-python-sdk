"""Framework auto-instrumentation hooks.

Each submodule exposes an `install()` function that monkey-patches the
target framework so spans flow into the active Prometa client without
explicit decorators on user code.
"""

from . import langchain  # noqa: F401  (re-export module for convenience)
from . import langgraph  # noqa: F401
from . import openai_agents  # noqa: F401
from . import crewai  # noqa: F401
from . import semantic_kernel  # noqa: F401
from . import mcp  # noqa: F401
from . import vector  # noqa: F401

__all__ = [
    "langchain",
    "langgraph",
    "openai_agents",
    "crewai",
    "semantic_kernel",
    "mcp",
    "vector",
]
