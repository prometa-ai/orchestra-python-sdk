"""Conformance adapter that exercises the reference host request boundary."""

from __future__ import annotations

import json
from typing import Optional, Sequence

from .conformance import (
    RuntimeConformanceCase,
    RuntimeConformanceObservation,
    SdkRuntimeConformanceDriver,
    _ConformanceModelAdapter,
    _UnavailableModelAdapter,
    _UnavailableStateStore,
    _admit,
    _no_sleep,
    runtime_conformance_command_main,
)
from .admission import InMemoryAdmissionReplayStore
from .host import ReferenceRuntimeHost
from .kernel import (
    InMemoryEvidenceEmitter,
    RuntimeExecutionPolicy,
    RuntimeKernel,
)


_CONFORMANCE_TOKEN = "reference-host-conformance-token-v1"


class ReferenceHostConformanceDriver:
    """Run execution cases through the authenticated reference-host API."""

    name = "prometa-reference-runtime-host"

    def __init__(self) -> None:
        self._sdk_driver = SdkRuntimeConformanceDriver()

    async def run_case(
        self, case: RuntimeConformanceCase
    ) -> RuntimeConformanceObservation:
        if not case.case_id.startswith("execution.") or case.case_id == (
            "execution.evidence-fail-closed"
        ):
            return await self._sdk_driver.run_case(case)

        vector = dict(case.vector)
        admitted = _admit(vector, InMemoryAdmissionReplayStore())
        adapter = (
            _UnavailableModelAdapter()
            if case.case_id == "execution.model-plane-unavailable"
            else _ConformanceModelAdapter(vector["sampleOutput"])
        )
        emitter = InMemoryEvidenceEmitter()
        state_store = (
            _UnavailableStateStore()
            if case.case_id == "execution.state-store-unavailable"
            else None
        )
        policy = (
            RuntimeExecutionPolicy(
                max_attempts_per_model=2,
                initial_backoff_seconds=0,
            )
            if case.case_id == "execution.model-plane-unavailable"
            else None
        )
        host = ReferenceRuntimeHost(
            RuntimeKernel(
                admitted,
                model_adapter=adapter,
                evidence_emitter=emitter,
                runtime_id="conformance-runtime",
                runtime_version="1",
                execution_policy=policy,
                state_store=state_store,
                sleep=_no_sleep,
            ),
            api_token=_CONFORMANCE_TOKEN,
            request_timeout_seconds=10,
        )
        payload = (
            {"unexpected": True}
            if case.case_id == "execution.invalid-input"
            else vector["sampleInput"]
        )
        try:
            response = host.handle(
                "POST",
                "/v1/runtime/execute",
                {
                    "authorization": "Bearer %s" % _CONFORMANCE_TOKEN,
                    "content-type": "application/json",
                },
                json.dumps(
                    {
                        "requestId": "conformance-%s" % case.case_id.replace(".", "-"),
                        "input": payload,
                    },
                    separators=(",", ":"),
                ).encode("utf-8"),
            )
        finally:
            host.close()
        error = response.body.get("error")
        error_code = error.get("code") if isinstance(error, dict) else None
        return RuntimeConformanceObservation(
            accepted=response.status == 200,
            error_code=error_code,
            output=response.body.get("output"),
            model_invocations=adapter.invocations,
            control_plane_invocations=0,
            evidence_events=emitter.events,
        )


def main(argv: Optional[Sequence[str]] = None) -> int:
    if argv:
        raise SystemExit("prometa-runtime-host-conformance-driver accepts no arguments")
    return runtime_conformance_command_main(ReferenceHostConformanceDriver())


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = ["ReferenceHostConformanceDriver", "main"]
