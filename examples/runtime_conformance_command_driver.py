"""One-shot subprocess adapter for the SDK reference runtime driver.

Replace ``SdkRuntimeConformanceDriver`` with an adapter that calls the tenant
runtime under test. The parent runner sends one protocol case on stdin and
accepts one bounded JSON observation on stdout.
"""

from prometa.runtime import (
    SdkRuntimeConformanceDriver,
    runtime_conformance_command_main,
)


if __name__ == "__main__":
    raise SystemExit(runtime_conformance_command_main(SdkRuntimeConformanceDriver()))
