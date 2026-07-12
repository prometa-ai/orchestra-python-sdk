"""Fresh reference-host process used by deployment recovery tests."""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

from prometa.runtime import (
    BundleTrustEntry,
    BundleTrustStore,
    RuntimeHostConfig,
    build_reference_runtime_host,
    serve_reference_runtime_host,
)


FIXTURE_PATH = Path(__file__).parent / "fixtures/runtime-kernel-v1.json"


def _instant(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _trust(value) -> BundleTrustStore:
    return BundleTrustStore(
        [
            BundleTrustEntry(
                issuer=value["issuer"],
                key_id=value["keyId"],
                public_key_spki_der_base64=value[
                    "publicKeySpkiDerBase64"
                ],
            )
        ]
    )


def main() -> int:
    vector = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    verification = vector["verification"]
    config = RuntimeHostConfig(
        tenant_id=os.environ["RECOVERY_TENANT_ID"],
        runtime_id=os.environ["RECOVERY_RUNTIME_ID"],
        runtime_version="0.18.0",
        org_id=verification["expectedOrgId"],
        environment=verification["expectedEnvironment"],
        release_id=verification["expectedReleaseId"],
        deployment_id=verification["expectedDeploymentId"],
        runtime_target=verification["expectedRuntime"],
        bundle=vector["bundle"],
        promotion_attestation=vector["attestation"],
        bundle_trust_store=_trust(vector["bundleTrust"]),
        promotion_trust_store=_trust(vector["promotionTrust"]),
        model_gateway_base_url=os.environ["RECOVERY_MODEL_GATEWAY_URL"],
        model_gateway_api_key_env=None,
        model_gateway_endpoint_path="/v1/chat/completions",
        model_gateway_timeout_seconds=10,
        model_gateway_max_response_bytes=1024 * 1024,
        database_dsn_env="RECOVERY_DATABASE_DSN",
        api_token_env="RECOVERY_API_TOKEN",
        request_timeout_seconds=1,
        max_request_bytes=1024,
        task_recovery_enabled=True,
        task_recovery_lease_seconds=2,
        task_recovery_max_attempts=3,
        task_recovery_history_limit=20,
    )
    host, _ = build_reference_runtime_host(
        config,
        environment=os.environ,
        now=_instant(verification["now"]),
    )
    serve_reference_runtime_host(
        host,
        bind_host="127.0.0.1",
        port=int(os.environ["RECOVERY_HOST_PORT"]),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
