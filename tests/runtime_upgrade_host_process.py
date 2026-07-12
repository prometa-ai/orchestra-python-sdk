"""Start one reference host from the source tree selected by PYTHONPATH."""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from prometa.runtime import (
    build_reference_runtime_host,
    load_runtime_host_config,
    serve_reference_runtime_host,
)


def main() -> int:
    config = load_runtime_host_config(Path(os.environ["DRILL_RUNTIME_CONFIG"]))
    now = datetime.fromisoformat(os.environ["DRILL_NOW"].replace("Z", "+00:00"))
    host, _ = build_reference_runtime_host(config, environment=os.environ, now=now)
    serve_reference_runtime_host(
        host,
        bind_host="127.0.0.1",
        port=int(os.environ["DRILL_RUNTIME_PORT"]),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
