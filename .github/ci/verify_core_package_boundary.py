"""Verify the installed core wheel remains independent from runtime extras."""

from __future__ import annotations

import importlib
import re
import sys
from importlib import metadata


OPTIONAL_DISTRIBUTIONS = (
    "cryptography",
    "httpx",
    "jsonschema",
    "mcp",
    "psycopg",
)
OPTIONAL_IMPORT_ROOTS = frozenset(OPTIONAL_DISTRIBUTIONS)
EXTRA_MARKER = re.compile(r";.*\bextra\s*==")


def _loaded_optional_modules() -> tuple[str, ...]:
    loaded = {
        name.split(".", 1)[0]
        for name in sys.modules
        if name.split(".", 1)[0] in OPTIONAL_IMPORT_ROOTS
    }
    return tuple(sorted(loaded))


def _assert_optional_distributions_absent() -> None:
    present = []
    for name in OPTIONAL_DISTRIBUTIONS:
        try:
            metadata.distribution(name)
        except metadata.PackageNotFoundError:
            continue
        present.append(name)
    if present:
        raise AssertionError("core wheel installed optional distributions: %s" % present)


def main() -> None:
    distribution = metadata.distribution("prometa-sdk")
    requirements = distribution.metadata.get_all("Requires-Dist") or []
    unconditional = [item for item in requirements if EXTRA_MARKER.search(item) is None]
    if unconditional:
        raise AssertionError(
            "core wheel declares unconditional dependencies: %s" % unconditional
        )

    _assert_optional_distributions_absent()

    prometa = importlib.import_module("prometa")
    if prometa.Prometa.__name__ != "Prometa":
        raise AssertionError("core public import contract changed")
    if "prometa.runtime" in sys.modules:
        raise AssertionError("import prometa eagerly loaded prometa.runtime")
    if loaded := _loaded_optional_modules():
        raise AssertionError("import prometa loaded optional modules: %s" % (loaded,))

    runtime = importlib.import_module("prometa.runtime")
    if loaded := _loaded_optional_modules():
        raise AssertionError(
            "import prometa.runtime loaded optional modules: %s" % (loaded,)
        )
    if runtime.CAPABILITY_SCHEMA_VALIDATE in runtime.available_runtime_capabilities():
        raise AssertionError("schema capability advertised without the runtime extra")
    if runtime.official_mcp_transport_available():
        raise AssertionError("MCP transport advertised without the runtime-mcp extra")

    print("core package boundary verified")


if __name__ == "__main__":
    main()
