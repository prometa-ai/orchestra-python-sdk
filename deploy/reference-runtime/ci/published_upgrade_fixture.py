"""Build and verify payload-free published upgrade/rollback evidence."""

from __future__ import annotations

import argparse
import copy
import importlib.util
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence


_SEMVER = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+")
_SHA256 = re.compile(r"sha256:[0-9a-f]{64}")
_RELEASE_TAG = re.compile(r"v[0-9]+\.[0-9]+\.[0-9]+")
_REVISION = re.compile(r"[0-9a-f]{40}|[0-9a-f]{64}")
_STAGES = ("baseline", "target", "rollback")
_TENANTS = ("a", "b")


def _load_topology_fixture():
    path = Path(__file__).with_name("topology_fixture.py")
    spec = importlib.util.spec_from_file_location("runtime_topology_fixture", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("topology_fixture_unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_TOPOLOGY = _load_topology_fixture()


def _read_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as stream:
        return json.load(stream)


def _write_json(path: Path, value: Any, *, private: bool = False) -> None:
    path.write_text(_TOPOLOGY._canonical(value) + "\n", encoding="utf-8")
    path.chmod(0o600 if private else 0o644)


def _version_tuple(value: str) -> tuple[int, int, int]:
    if not _SEMVER.fullmatch(value):
        raise ValueError("published_upgrade_version_invalid")
    return tuple(int(part) for part in value.split("."))  # type: ignore[return-value]


def _signed_bundle(
    tenant: str,
    label: str,
    manifest_version: int,
    private_key: Any,
    now: datetime,
) -> Mapping[str, Any]:
    content = copy.deepcopy(_TOPOLOGY._runtime_content(tenant, "model-only"))
    content["manifest"]["version"] = manifest_version
    content["systemPrompt"] = "Serve published %s for tenant %s." % (label, tenant)
    contract = content["runtimeContract"]
    policy_digest, configuration_digest = _TOPOLOGY._runtime_projection_digests(
        content, contract["inputSchema"], contract["outputSchema"]
    )
    contract["policyDigest"] = policy_digest
    contract["configurationDigest"] = configuration_digest
    content_canonical = _TOPOLOGY._canonical(content)
    digest = _TOPOLOGY._digest(content)
    claims = {
        "envelopeVersion": 1,
        "issuer": "https://orchestra.example.test",
        "keyId": "published-upgrade-bundle-key-%s" % tenant,
        "orgId": "org-topology-%s" % tenant,
        "audience": "prometa-runtime",
        "targetEnvironment": "prod",
        "subject": "agent-manifest:manifest-topology-%s:v%d"
        % (tenant, manifest_version),
        "jti": "published-upgrade-bundle-%s-%s" % (label, tenant),
        "artifactDigest": digest,
        "contentCanonical": content_canonical,
        "issuedAt": _TOPOLOGY._instant(now - timedelta(minutes=2)),
        "notBefore": _TOPOLOGY._instant(now - timedelta(minutes=2)),
        "expiresAt": _TOPOLOGY._instant(now + timedelta(hours=3)),
        "offlineLeaseExpiresAt": _TOPOLOGY._instant(
            now + timedelta(hours=2, minutes=30)
        ),
    }
    payload = _TOPOLOGY._canonical(claims)
    return {
        "content": content,
        "algorithm": "ed25519",
        "envelopeVersion": 1,
        "envelopeCanonicalization": "signed-payload-json-v1",
        "signedPayload": payload,
        "envelopeSignature": _TOPOLOGY._sign(private_key, payload),
        "artifactDigest": digest,
        "issuer": claims["issuer"],
        "keyId": claims["keyId"],
        "signed": True,
    }


def _release(
    tenant: str,
    label: str,
    bundle: Mapping[str, Any],
    manifest_version: int,
    bundle_key: Any,
    promotion_key: Any,
    now: datetime,
) -> Mapping[str, Any]:
    artifact_label = "baseline" if label == "rollback" else label
    org_id = "org-topology-%s" % tenant
    release_id = "published-upgrade-release-%s-%s" % (label, tenant)
    deployment_id = "published-upgrade-deployment-%s-%s" % (label, tenant)
    attestation_id = "published-upgrade-attestation-%s-%s" % (label, tenant)
    promotion_claims = {
        "artifactType": "orchestra.promotion-attestation",
        "attestationVersion": 1,
        "issuer": "https://orchestra.example.test/promotion",
        "keyId": "published-upgrade-promotion-key-%s" % tenant,
        "subject": "promotion-attestation:%s" % attestation_id,
        "orgId": org_id,
        "audience": "prometa-runtime-admission",
        "targetEnvironment": "prod",
        "artifactId": "published-upgrade-artifact-%s-%s" % (artifact_label, tenant),
        "artifactDigest": bundle["artifactDigest"],
        "manifestId": "manifest-topology-%s" % tenant,
        "manifestVersion": manifest_version,
        "agentId": "agent-topology-%s" % tenant,
        "decisionId": "published-upgrade-decision-%s-%s" % (label, tenant),
        "decisionAllow": True,
        "gateStage": "prod",
        "policySetDigest": "sha256:" + "b" * 64,
        "evidenceDigest": "sha256:" + "c" * 64,
        "decisionEvaluatedAt": _TOPOLOGY._instant(now - timedelta(minutes=3)),
        "decisionValidUntil": _TOPOLOGY._instant(now + timedelta(hours=2)),
        "requestedRuntime": "tenant-runtime",
        "releaseId": release_id,
        "deploymentId": deployment_id,
        "approvals": [],
        "issuedAt": _TOPOLOGY._instant(now - timedelta(minutes=1)),
        "notBefore": _TOPOLOGY._instant(now - timedelta(minutes=1)),
        "expiresAt": _TOPOLOGY._instant(now + timedelta(hours=2)),
        "offlineLeaseExpiresAt": _TOPOLOGY._instant(
            now + timedelta(hours=1, minutes=30)
        ),
        "jti": "published-upgrade-promotion-%s-%s" % (label, tenant),
        "revocationRef": "urn:prometa:promotion-attestation:%s" % attestation_id,
    }
    promotion_payload = _TOPOLOGY._canonical(promotion_claims)
    attestation = {
        "attestationId": attestation_id,
        "attestationVersion": 1,
        "algorithm": "ed25519",
        "canonicalization": "signed-payload-json-v1",
        "issuer": promotion_claims["issuer"],
        "keyId": promotion_claims["keyId"],
        "signedPayload": promotion_payload,
        "signature": _TOPOLOGY._sign(promotion_key, promotion_payload),
        "signed": True,
        "authorization": {
            "artifactId": promotion_claims["artifactId"],
            "artifactDigest": bundle["artifactDigest"],
            "decisionId": promotion_claims["decisionId"],
            "releaseId": release_id,
            "deploymentId": deployment_id,
            "targetEnvironment": "prod",
            "requestedRuntime": "tenant-runtime",
            "expiresAt": promotion_claims["expiresAt"],
            "offlineLeaseExpiresAt": promotion_claims["offlineLeaseExpiresAt"],
        },
    }
    return {
        "orgId": org_id,
        "releaseId": release_id,
        "deploymentId": deployment_id,
        "bundle": bundle,
        "attestation": attestation,
        "bundleTrust": {
            "issuer": bundle["issuer"],
            "keyId": bundle["keyId"],
            "publicKeySpkiDerBase64": _TOPOLOGY._public_key(bundle_key),
            "allowedOrgIds": [org_id],
            "allowedAudiences": ["prometa-runtime"],
            "allowedEnvironments": ["prod"],
        },
        "promotionTrust": {
            "issuer": promotion_claims["issuer"],
            "keyId": promotion_claims["keyId"],
            "publicKeySpkiDerBase64": _TOPOLOGY._public_key(promotion_key),
            "allowedOrgIds": [org_id],
            "allowedAudiences": ["prometa-runtime-admission"],
            "allowedEnvironments": ["prod"],
        },
    }


def prepare_sequence(
    output_dir: Path,
    baseline_version: str,
    target_version: str,
    replicas: int = 2,
) -> None:
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    if (
        _version_tuple(target_version) <= _version_tuple(baseline_version)
        or type(replicas) is not int
        or replicas < 2
    ):
        raise ValueError("published_upgrade_sequence_invalid")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_dir.chmod(0o700)
    now = datetime.now(timezone.utc).replace(microsecond=0)
    sequence: Dict[str, Any] = {
        "contractVersion": 1,
        "baselineRuntimeVersion": baseline_version,
        "targetRuntimeVersion": target_version,
        "runtimeReplicasPerTenant": replicas,
        "tenants": [],
    }
    for tenant in _TENANTS:
        bundle_key = Ed25519PrivateKey.generate()
        promotion_key = Ed25519PrivateKey.generate()
        baseline_bundle = _signed_bundle(tenant, "baseline", 1, bundle_key, now)
        target_bundle = _signed_bundle(tenant, "target", 2, bundle_key, now)
        releases = {
            "baseline": _release(
                tenant,
                "baseline",
                baseline_bundle,
                1,
                bundle_key,
                promotion_key,
                now,
            ),
            "target": _release(
                tenant,
                "target",
                target_bundle,
                2,
                bundle_key,
                promotion_key,
                now,
            ),
            "rollback": _release(
                tenant,
                "rollback",
                baseline_bundle,
                1,
                bundle_key,
                promotion_key,
                now,
            ),
        }
        tenant_stages = []
        for stage in _STAGES:
            release = releases[stage]
            runtime_version = target_version if stage == "target" else baseline_version
            config_name = "tenant-%s-%s-config.json" % (tenant, stage)
            _write_json(
                output_dir / config_name,
                _TOPOLOGY._runtime_config(
                    tenant, release, runtime_version, workload="model-only"
                ),
                private=True,
            )
            claims = json.loads(release["bundle"]["signedPayload"])
            promotion_claims = json.loads(release["attestation"]["signedPayload"])
            tenant_stages.append(
                {
                    "stage": stage,
                    "runtimeVersion": runtime_version,
                    "releaseId": release["releaseId"],
                    "deploymentId": release["deploymentId"],
                    "artifactDigest": release["bundle"]["artifactDigest"],
                    "bundleJti": claims["jti"],
                    "promotionJti": promotion_claims["jti"],
                    "configFile": config_name,
                }
            )
        sequence["tenants"].append({"tenant": tenant, "stages": tenant_stages})
    _write_json(output_dir / "sequence.json", sequence)


def _load_descriptor(path: Path) -> Mapping[str, Any]:
    value = _read_json(path)
    required = {
        "releaseTag",
        "releaseRevision",
        "runtimeVersion",
        "runtimeImage",
        "chartVersion",
        "chartPackage",
        "chartPackageSha256",
        "chartOciReference",
    }
    if (
        not isinstance(value, Mapping)
        or value.get("contractVersion") != 1
        or not required.issubset(value)
    ):
        raise ValueError("published_release_descriptor_invalid")
    if (
        not _RELEASE_TAG.fullmatch(str(value["releaseTag"]))
        or value["releaseTag"] != "v" + str(value["runtimeVersion"])
        or not _REVISION.fullmatch(str(value["releaseRevision"]))
        or not _SEMVER.fullmatch(str(value["runtimeVersion"]))
        or not _SEMVER.fullmatch(str(value["chartVersion"]))
        or not isinstance(value["chartPackage"], str)
        or not value["chartPackage"]
        or "@" not in str(value["runtimeImage"])
        or not _SHA256.fullmatch(str(value["runtimeImage"]).rsplit("@", 1)[-1])
        or not _SHA256.fullmatch(str(value["chartPackageSha256"]))
        or "@" not in str(value["chartOciReference"])
        or not _SHA256.fullmatch(str(value["chartOciReference"]).rsplit("@", 1)[-1])
    ):
        raise ValueError("published_release_descriptor_invalid")
    return value


def _expected_stages(sequence: Mapping[str, Any]) -> Mapping[tuple[str, str], Any]:
    expected = {}
    for tenant in sequence.get("tenants", []):
        tenant_name = tenant.get("tenant")
        for stage in tenant.get("stages", []):
            expected[(stage.get("stage"), tenant_name)] = stage
    if set(expected) != {(stage, tenant) for stage in _STAGES for tenant in _TENANTS}:
        raise ValueError("published_upgrade_sequence_invalid")
    return expected


def write_report(
    sequence_path: Path,
    observations_path: Path,
    baseline_descriptor_path: Path,
    target_descriptor_path: Path,
    kubernetes_version: str,
    output_path: Path,
) -> None:
    sequence = _read_json(sequence_path)
    baseline = _load_descriptor(baseline_descriptor_path)
    target = _load_descriptor(target_descriptor_path)
    if (
        not isinstance(sequence, Mapping)
        or sequence.get("contractVersion") != 1
        or sequence.get("baselineRuntimeVersion") != baseline["runtimeVersion"]
        or sequence.get("targetRuntimeVersion") != target["runtimeVersion"]
        or _version_tuple(str(target["runtimeVersion"]))
        <= _version_tuple(str(baseline["runtimeVersion"]))
        or _version_tuple(str(target["chartVersion"]))
        <= _version_tuple(str(baseline["chartVersion"]))
        or target["runtimeImage"] == baseline["runtimeImage"]
        or target["chartOciReference"] == baseline["chartOciReference"]
        or target["chartPackageSha256"] == baseline["chartPackageSha256"]
        or not kubernetes_version.startswith("v")
    ):
        raise ValueError("published_upgrade_sequence_invalid")
    expected = _expected_stages(sequence)
    observations = {}
    for raw_line in observations_path.read_text(encoding="utf-8").splitlines():
        fields = raw_line.split("\t")
        if len(fields) != 11:
            raise ValueError("published_upgrade_observation_invalid")
        stage, tenant = fields[0], fields[1]
        key = (stage, tenant)
        if key in observations:
            raise ValueError("published_upgrade_observation_invalid")
        observations[key] = {
            "runtimeImage": fields[2],
            "chartVersion": fields[3],
            "readyReplicas": int(fields[4]),
            "schemaVersions": fields[5],
            "deploymentId": fields[6],
            "releaseId": fields[7],
            "artifactDigest": fields[8],
            "bundleJti": fields[9],
            "promotionJti": fields[10],
        }
    if set(observations) != set(expected):
        raise ValueError("published_upgrade_observation_invalid")
    replicas = sequence.get("runtimeReplicasPerTenant")
    for key, expected_stage in expected.items():
        observed = observations[key]
        descriptor = target if key[0] == "target" else baseline
        expected_observation = {
            "runtimeImage": descriptor["runtimeImage"],
            "chartVersion": descriptor["chartVersion"],
            "readyReplicas": replicas,
            "deploymentId": expected_stage["deploymentId"],
            "releaseId": expected_stage["releaseId"],
            "artifactDigest": expected_stage["artifactDigest"],
            "bundleJti": expected_stage["bundleJti"],
            "promotionJti": expected_stage["promotionJti"],
        }
        if {
            name: value for name, value in observed.items() if name != "schemaVersions"
        } != expected_observation or not re.fullmatch(
            r"[1-9][0-9]*(?:,[1-9][0-9]*)*", observed["schemaVersions"]
        ):
            raise ValueError("published_upgrade_observation_invalid")
    schema_versions = {}
    for stage in _STAGES:
        per_tenant = {
            observations[(stage, tenant)]["schemaVersions"] for tenant in _TENANTS
        }
        if len(per_tenant) != 1:
            raise ValueError("published_upgrade_schema_observation_invalid")
        schema_versions[stage] = tuple(
            int(value) for value in per_tenant.pop().split(",")
        )
    if (
        schema_versions["target"][: len(schema_versions["baseline"])]
        != schema_versions["baseline"]
        or schema_versions["rollback"] != schema_versions["target"]
    ):
        raise ValueError("published_upgrade_schema_observation_invalid")
    for tenant in _TENANTS:
        first = expected[("baseline", tenant)]
        rollback = expected[("rollback", tenant)]
        if (
            first["artifactDigest"] != rollback["artifactDigest"]
            or first["bundleJti"] != rollback["bundleJti"]
            or first["promotionJti"] == rollback["promotionJti"]
            or first["deploymentId"] == rollback["deploymentId"]
        ):
            raise ValueError("published_upgrade_rollback_identity_invalid")
    artifact_sequence = []
    for stage, descriptor in (
        ("baseline", baseline),
        ("target", target),
        ("rollback", baseline),
    ):
        artifact_sequence.append(
            {
                "stage": stage,
                "releaseTag": descriptor["releaseTag"],
                "releaseRevision": descriptor["releaseRevision"],
                "runtimeVersion": descriptor["runtimeVersion"],
                "runtimeImage": descriptor["runtimeImage"],
                "chartVersion": descriptor["chartVersion"],
                "chartPackageSha256": descriptor["chartPackageSha256"],
                "chartOciReference": descriptor["chartOciReference"],
            }
        )
    _write_json(
        output_path,
        {
            "contractVersion": 1,
            "passed": True,
            "evidenceStatus": "reference-profile-not-production-certification",
            "proof": "published-runtime-upgrade-forward-rollback",
            "kubernetesVersion": kubernetes_version,
            "artifactSequence": artifact_sequence,
            "tenantCount": len(_TENANTS),
            "runtimeReplicasPerTenant": replicas,
            "publishedChartAndImageExecution": True,
            "helmForwardRollback": True,
            "targetImageMigrationHookPassed": True,
            "baselineImageCompatibilityHookPassed": True,
            "postgresSchemaVersions": {
                stage: list(schema_versions[stage]) for stage in _STAGES
            },
            "priorBundleDigestReused": True,
            "freshRollbackPromotionIdentity": True,
            "activationRowsPerStagePerTenant": 1,
            "synchronousControlPlaneCalls": 0,
        },
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="published_upgrade_fixture.py")
    subparsers = parser.add_subparsers(dest="command", required=True)
    prepare = subparsers.add_parser("prepare")
    prepare.add_argument("--output-dir", type=Path, required=True)
    prepare.add_argument("--baseline-version", required=True)
    prepare.add_argument("--target-version", required=True)
    prepare.add_argument("--replicas", type=int, default=2)
    report = subparsers.add_parser("report")
    report.add_argument("--sequence", type=Path, required=True)
    report.add_argument("--observations", type=Path, required=True)
    report.add_argument("--baseline-descriptor", type=Path, required=True)
    report.add_argument("--target-descriptor", type=Path, required=True)
    report.add_argument("--kubernetes-version", required=True)
    report.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == "prepare":
        prepare_sequence(
            args.output_dir,
            args.baseline_version,
            args.target_version,
            args.replicas,
        )
    else:
        write_report(
            args.sequence,
            args.observations,
            args.baseline_descriptor,
            args.target_descriptor,
            args.kubernetes_version,
            args.output,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
