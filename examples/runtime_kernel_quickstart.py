"""Run one signed and promoted model-only bundle in a tenant process.

This sample deliberately uses the in-memory replay store. Replace it with an
atomic tenant database implementation before running multiple host replicas.
Bundles that declare guards or tools require tenant GuardEvaluator/ToolBroker
implementations and will fail capability negotiation in this minimal sample.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from prometa import Prometa
from prometa.runtime import (
    BundleTrustEntry,
    BundleTrustStore,
    InMemoryAdmissionReplayStore,
    OpenAICompatibleModelAdapter,
    PrometaEvidenceEmitter,
    RuntimeAdmissionPolicy,
    RuntimeKernel,
    admit_runtime_release,
    available_runtime_capabilities,
)


def _load(path: str):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError("%s is required" % name)
    return value


async def run(args) -> None:
    bundle = _load(args.bundle)
    attestation = _load(args.attestation)
    input_payload = json.loads(args.input)

    admitted = admit_runtime_release(
        bundle,
        attestation,
        bundle_trust_store=BundleTrustStore(
            [
                BundleTrustEntry(
                    issuer=args.bundle_issuer,
                    key_id=args.bundle_key_id,
                    public_key_spki_der_base64=_required_env(
                        "ORCHESTRA_BUNDLE_PUBLIC_KEY"
                    ),
                    allowed_org_ids=frozenset({args.org_id}),
                    allowed_audiences=frozenset({"prometa-runtime"}),
                    allowed_environments=frozenset({args.environment}),
                )
            ]
        ),
        promotion_trust_store=BundleTrustStore(
            [
                BundleTrustEntry(
                    issuer=args.promotion_issuer,
                    key_id=args.promotion_key_id,
                    public_key_spki_der_base64=_required_env(
                        "ORCHESTRA_PROMOTION_PUBLIC_KEY"
                    ),
                    allowed_org_ids=frozenset({args.org_id}),
                    allowed_audiences=frozenset({"prometa-runtime-admission"}),
                    allowed_environments=frozenset({args.environment}),
                )
            ]
        ),
        replay_store=InMemoryAdmissionReplayStore(),
        policy=RuntimeAdmissionPolicy(
            expected_org_id=args.org_id,
            expected_environment=args.environment,
            expected_release_id=args.release_id,
            expected_deployment_id=args.deployment_id,
            expected_runtime=args.runtime_target,
            supported_capabilities=available_runtime_capabilities(),
        ),
        now=datetime.now(timezone.utc),
    )

    telemetry = Prometa(
        endpoint=args.telemetry_endpoint,
        api_key=_required_env("PROMETA_API_KEY"),
        solution_id=admitted.config.manifest.solution_name,
        agent_name=admitted.config.manifest.name,
        agent_id=admitted.config.manifest.agent_id,
        stage=args.environment,
    )
    kernel = RuntimeKernel(
        admitted,
        model_adapter=OpenAICompatibleModelAdapter(
            args.model_gateway,
            api_key=os.environ.get("MODEL_GATEWAY_API_KEY"),
        ),
        evidence_emitter=PrometaEvidenceEmitter(telemetry),
        runtime_id=args.runtime_id,
        runtime_version=args.runtime_version,
    )
    result = await kernel.execute(input_payload)
    print(json.dumps(result.output, ensure_ascii=False))
    telemetry.flush()


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--bundle", required=True)
    value.add_argument("--attestation", required=True)
    value.add_argument("--input", required=True, help="JSON request payload")
    value.add_argument("--org-id", required=True)
    value.add_argument("--environment", default="prod")
    value.add_argument("--release-id", required=True)
    value.add_argument("--deployment-id", required=True)
    value.add_argument("--runtime-target", default="tenant-runtime")
    value.add_argument("--runtime-id", default="tenant-runtime-01")
    value.add_argument("--runtime-version", default="0.17.0")
    value.add_argument("--bundle-issuer", required=True)
    value.add_argument("--bundle-key-id", required=True)
    value.add_argument("--promotion-issuer", required=True)
    value.add_argument("--promotion-key-id", required=True)
    value.add_argument("--model-gateway", required=True)
    value.add_argument("--telemetry-endpoint", required=True)
    return value


if __name__ == "__main__":
    asyncio.run(run(parser().parse_args()))
