import importlib.util
import json
import stat
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest


ROOT = Path(__file__).parent.parent
DEPLOY = ROOT / "deploy/reference-runtime"
PROFILE = DEPLOY / "topology-profiles.json"
MCP_PROFILE = DEPLOY / "topology-profiles.mcp.json"
FIXTURE_PATH = DEPLOY / "ci/topology_fixture.py"
PROBE_PATH = DEPLOY / "ci/topology_probe.py"
MCP_SERVER_PATH = DEPLOY / "ci/topology_mcp_server.py"


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _profile():
    return json.loads(PROFILE.read_text(encoding="utf-8"))["profiles"][0]


def _pod(name: str, ip: str, node: str):
    return {
        "metadata": {"name": name},
        "spec": {"nodeName": node},
        "status": {
            "podIP": ip,
            "conditions": [{"type": "Ready", "status": "True"}],
        },
    }


def test_topology_profile_is_pinned_and_explicitly_non_production():
    document = json.loads(PROFILE.read_text(encoding="utf-8"))

    assert document["contractVersion"] == 1
    assert len(document["profiles"]) == 1
    assert _profile() == {
        "name": "k3d-k3s-kube-router-v2",
        "workload": "model-only",
        "evidenceStatus": "reference-profile-not-production-certification",
        "networkPolicyController": "k3s-kube-router",
        "runtimeVersion": "0.18.0",
        "chartVersion": "0.3.1",
        "k3dVersion": "v5.8.3",
        "k3dChecksums": {
            "darwin-amd64": (
                "fd0f8e9e8ea4d8bc3674572ca6ed0833b639bf57c43c708616d937377324cfea"
            ),
            "darwin-arm64": (
                "8da468daa7dc7cf7cdd4735f90a9bb05179fa27858250f62e3d8cdf5b5ca0698"
            ),
            "linux-amd64": (
                "dbaa79a76ace7f4ca230a1ff41dc7d8a5036a8ad0309e9c54f9bf3836dbe853e"
            ),
            "linux-arm64": (
                "0b8110f2229631af7402fb828259330985918b08fefd38b7f1b788a1c8687216"
            ),
        },
        "k3sImage": "rancher/k3s:v1.34.8-k3s1",
        "k3sImageDigest": (
            "sha256:8f2019c4a443f02fb6aecd4b8e605402535c52be6588ea4b9ee52e1f5851ad72"
        ),
        "postgresImage": "postgres:16.13-alpine",
        "postgresImageDigest": (
            "sha256:4e6e670bb069649261c9c18031f0aded7bb249a5b6664ddec29c013a89310d50"
        ),
        "postgresNodeImage": "prometa-topology-postgres:16.13-alpine",
        "serverNodes": 1,
        "agentNodes": 1,
        "tenantCount": 2,
        "runtimeReplicasPerTenant": 2,
        "uniqueLoadRequestsPerTenant": 24,
        "duplicateAttemptsPerTenant": 12,
    }


def test_topology_scripts_are_executable_and_bound_to_payload_free_report():
    installer = DEPLOY / "ci/install-k3d.sh"
    harness = DEPLOY / "ci/topology-certification.sh"
    harness_text = harness.read_text(encoding="utf-8")

    assert installer.stat().st_mode & stat.S_IXUSR
    assert harness.stat().st_mode & stat.S_IXUSR
    assert "reference-profile-not-production-certification" in harness_text
    assert "topology_fixture.py" in harness_text
    assert "topology_probe.py" in harness_text
    assert "partition-policy" in harness_text
    assert "task_store_unavailable" in harness_text
    assert "prometa_runtime_release_activation" in harness_text
    assert "controlPlanePull" not in harness_text
    assert "PROMETA_RUNTIME_TOPOLOGY_RECEIPT_PROOF" in harness_text
    assert "PROMETA_RUNTIME_TOPOLOGY_PROFILE" in harness_text
    assert "mcp_tool_call_indeterminate" in harness_text
    assert "runtime-mcp-credentials" in harness_text
    assert "verify-platform-receipts" in harness_text


def test_mcp_topology_profile_reuses_pins_but_has_an_explicit_workload():
    model = _profile()
    mcp = json.loads(MCP_PROFILE.read_text(encoding="utf-8"))["profiles"][0]

    assert mcp["name"] == "k3d-k3s-kube-router-mcp-v2"
    assert mcp["workload"] == "mcp-read-only"
    assert mcp["evidenceStatus"] == (
        "reference-profile-not-production-certification"
    )
    for key in (
        "networkPolicyController",
        "runtimeVersion",
        "chartVersion",
        "k3dVersion",
        "k3dChecksums",
        "k3sImage",
        "k3sImageDigest",
        "postgresImage",
        "postgresImageDigest",
        "postgresNodeImage",
        "serverNodes",
        "agentNodes",
        "tenantCount",
        "runtimeReplicasPerTenant",
    ):
        assert mcp[key] == model[key]
    assert mcp["uniqueLoadRequestsPerTenant"] == 12
    assert mcp["duplicateAttemptsPerTenant"] == 8


def test_topology_profile_name_is_bound_to_its_workload(tmp_path):
    fixture = _load_module("topology_fixture_profile_binding", FIXTURE_PATH)
    document = json.loads(MCP_PROFILE.read_text(encoding="utf-8"))
    document["profiles"][0]["workload"] = "model-only"
    mismatched = tmp_path / "mismatched-profile.json"
    mismatched.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(ValueError, match="topology_profile_invalid"):
        fixture._load_profile(mismatched)


def test_topology_fixture_builds_two_isolated_tenant_releases(tmp_path):
    pytest.importorskip("cryptography")
    fixture = _load_module("topology_fixture_prepare", FIXTURE_PATH)
    output = tmp_path / "fixture"

    fixture.prepare(
        PROFILE,
        output,
        PROBE_PATH,
        "prometa-runtime-host:topology-test",
        "0.18.0",
    )

    config_a = json.loads((output / "tenant-a-config.json").read_text())
    config_b = json.loads((output / "tenant-b-config.json").read_text())
    values_a = json.loads((output / "tenant-a-values.json").read_text())
    resources = json.loads((output / "support-resources.json").read_text())

    assert config_a["tenantId"] == "tenant-topology-a"
    assert config_b["tenantId"] == "tenant-topology-b"
    assert config_a["orgId"] != config_b["orgId"]
    assert config_a["releaseId"] != config_b["releaseId"]
    assert config_a["deploymentId"] != config_b["deploymentId"]
    assert config_a["bundle"]["artifactDigest"] != config_b["bundle"]["artifactDigest"]
    content_a = config_a["bundle"]["content"]
    content_b = config_b["bundle"]["content"]
    contract_a = content_a["runtimeContract"]
    contract_b = content_b["runtimeContract"]
    assert content_a["schemaVersion"] == 2
    assert contract_a["contractVersion"] == 2
    assert contract_a["capabilityRequirements"] == [
        {"name": "evidence.emit", "minVersion": 1, "maxVersion": 1},
        {"name": "model.invoke", "minVersion": 1, "maxVersion": 1},
        {"name": "schema.validate", "minVersion": 1, "maxVersion": 1},
    ]
    assert contract_a["secretReferences"] == []
    assert fixture._runtime_contract_digests(config_a["bundle"]) == (
        contract_a["policyDigest"],
        contract_a["configurationDigest"],
    )
    assert contract_a["policyDigest"] == contract_b["policyDigest"]
    assert contract_a["configurationDigest"] != contract_b["configurationDigest"]
    assert config_a["modelGateway"]["baseUrl"].startswith(
        "http://model-gateway.models-a."
    )
    assert "controlPlanePull" not in config_a
    assert "receiptDelivery" not in config_a
    assert config_a["taskRecovery"] == {
        "leaseSeconds": 15,
        "maxAttempts": 3,
        "historyLimit": 50,
    }

    assert values_a["replicaCount"] == 2
    assert values_a["podDisruptionBudget"] == {
        "enabled": True,
        "minAvailable": 1,
    }
    assert values_a["topologySpreadConstraints"][0]["whenUnsatisfiable"] == (
        "DoNotSchedule"
    )
    ingress_peer = values_a["networkPolicy"]["ingress"][0]["from"][0]
    assert ingress_peer["namespaceSelector"]["matchLabels"] == {
        "kubernetes.io/metadata.name": "gateway-a"
    }
    assert ingress_peer["podSelector"]["matchLabels"] == {
        "app.kubernetes.io/name": "tenant-ai-gateway"
    }
    egress_namespaces = {
        peer["namespaceSelector"]["matchLabels"]["kubernetes.io/metadata.name"]
        for rule in values_a["networkPolicy"]["egress"]
        for peer in rule["to"]
    }
    assert egress_namespaces == {"data-a", "models-a"}

    namespaces = {
        item["metadata"]["name"]
        for item in resources["items"]
        if item["kind"] == "Namespace"
    }
    assert namespaces == {
        "runtime-a",
        "gateway-a",
        "models-a",
        "data-a",
        "runtime-b",
        "gateway-b",
        "models-b",
        "data-b",
    }
    postgres_images = {
        item["spec"]["template"]["spec"]["containers"][0]["image"]
        for item in resources["items"]
        if item["kind"] == "Deployment" and item["metadata"]["name"] == "postgres"
    }
    assert postgres_images == {"prometa-topology-postgres:16.13-alpine"}
    for name in (
        "tenant-a-config.json",
        "tenant-b-config.json",
        "tenant-a-credentials.env",
        "tenant-b-credentials.env",
        "support-resources.json",
    ):
        assert stat.S_IMODE((output / name).stat().st_mode) == 0o600

    with pytest.raises(ValueError, match="runtime_version_mismatch"):
        fixture.prepare(
            PROFILE,
            tmp_path / "wrong-version",
            PROBE_PATH,
            "prometa-runtime-host:topology-test",
            "0.19.0",
        )


def test_topology_fixture_builds_read_only_mcp_tenants_with_separate_secrets(
    tmp_path,
):
    pytest.importorskip("cryptography")
    fixture = _load_module("topology_fixture_mcp_prepare", FIXTURE_PATH)
    output = tmp_path / "fixture"

    fixture.prepare(
        MCP_PROFILE,
        output,
        PROBE_PATH,
        "prometa-runtime-host:topology-test",
        "0.18.0",
        mcp_server_source_path=MCP_SERVER_PATH,
    )

    config = json.loads((output / "tenant-a-config.json").read_text())
    content = config["bundle"]["content"]
    values = json.loads((output / "tenant-a-values.json").read_text())
    resources = json.loads((output / "support-resources.json").read_text())

    assert "taskRecovery" not in config
    assert content["runtimeContract"]["requiredCapabilities"] == [
        "evidence.emit.v1",
        "model.invoke.v1",
        "schema.validate.v1",
        "tool.broker.v1",
    ]
    assert content["schemaVersion"] == 2
    assert content["runtimeContract"]["contractVersion"] == 2
    assert content["runtimeContract"]["capabilityRequirements"][-1] == {
        "name": "tool.broker",
        "minVersion": 1,
        "maxVersion": 1,
    }
    assert content["mcpServers"] == ["Tenant Tools"]
    assert content["requiredScopes"] == ["tools:read"]
    assert content["grantedScopes"] == ["tools:read"]
    assert content["tools"][0]["sideEffects"] == "read-only"
    assert content["tools"][0]["approvalRequired"] is False
    assert config["mcpBroker"]["servers"][0]["transport"] == "streamable-http"
    assert config["mcpBroker"]["servers"][0]["allowInsecureHttp"] is True
    assert config["mcpBroker"]["credentialBindings"] == [
        {
            "serverName": "Tenant Tools",
            "authMode": "api-key",
            "httpHeaders": {
                "Authorization": "MCP_TOPOLOGY_AUTHORIZATION"
            },
            "stdioEnvironment": {},
        }
    ]
    assert values["extraEnv"] == [
        {
            "name": "MCP_TOPOLOGY_AUTHORIZATION",
            "valueFrom": {
                "secretKeyRef": {
                    "name": "runtime-mcp-credentials",
                    "key": "authorization",
                }
            },
        }
    ]
    egress_namespaces = {
        peer["namespaceSelector"]["matchLabels"]["kubernetes.io/metadata.name"]
        for rule in values["networkPolicy"]["egress"]
        for peer in rule["to"]
    }
    assert egress_namespaces == {"data-a", "models-a", "tools-a"}

    namespaces = {
        item["metadata"]["name"]
        for item in resources["items"]
        if item["kind"] == "Namespace"
    }
    assert {"tools-a", "tools-b"}.issubset(namespaces)
    secrets = {
        (item["metadata"]["namespace"], item["metadata"]["name"])
        for item in resources["items"]
        if item["kind"] == "Secret"
    }
    assert ("tools-a", "mcp-server-credentials") in secrets
    assert ("runtime-a", "runtime-mcp-credentials") in secrets
    policy = next(
        item
        for item in resources["items"]
        if item["kind"] == "NetworkPolicy"
        and item["metadata"] == {
            "name": "mcp-integration",
            "namespace": "tools-a",
        }
    )
    peer = policy["spec"]["ingress"][0]["from"][0]
    assert peer["namespaceSelector"]["matchLabels"] == {
        "kubernetes.io/metadata.name": "runtime-a"
    }
    assert peer["podSelector"]["matchLabels"] == {
        "app.kubernetes.io/component": "runtime"
    }
    assert policy["spec"]["egress"] == []

    for tenant in ("a", "b"):
        for suffix in ("mcp-server.env", "mcp-runtime.env"):
            path = output / f"tenant-{tenant}-rotated-{suffix}"
            assert stat.S_IMODE(path.stat().st_mode) == 0o600
    private_document = (output / "support-resources.json").read_text()
    assert "Bearer " in private_document
    assert stat.S_IMODE((output / "support-resources.json").stat().st_mode) == 0o600

    with pytest.raises(ValueError, match="mcp_server_source_missing"):
        fixture.prepare(
            MCP_PROFILE,
            tmp_path / "missing-server",
            PROBE_PATH,
            "prometa-runtime-host:topology-test",
            "0.18.0",
        )


def test_topology_fixture_optionally_wires_live_platform_receipts(tmp_path):
    pytest.importorskip("cryptography")
    fixture = _load_module("topology_fixture_receipts", FIXTURE_PATH)
    output = tmp_path / "fixture"

    fixture.prepare(
        PROFILE,
        output,
        PROBE_PATH,
        "prometa-runtime-host:topology-test",
        "0.18.0",
        "http://172.22.0.9:3000",
        "172.22.0.9/32",
    )

    config = json.loads((output / "tenant-a-config.json").read_text())
    values = json.loads((output / "tenant-a-values.json").read_text())
    credentials = (output / "tenant-a-credentials.env").read_text()
    platform = json.loads((output / "platform-receipt-fixture.json").read_text())

    assert config["receiptDelivery"] == {
        "baseUrl": "http://172.22.0.9:3000",
        "apiKeyEnv": "ORCHESTRA_RUNTIME_RECEIPT_API_KEY",
        "allowInsecureHttp": True,
        "timeoutSeconds": 3,
        "pollIntervalSeconds": 1,
        "leaseSeconds": 15,
        "initialBackoffSeconds": 1,
        "maxBackoffSeconds": 8,
    }
    assert "receipt-api-key=pk_topology_" in credentials
    assert values["credentials"]["receiptApiKeyOptional"] is False
    assert values["networkPolicy"]["egress"][-1] == {
        "to": [{"ipBlock": {"cidr": "172.22.0.9/32"}}],
        "ports": [{"protocol": "TCP", "port": 3000}],
    }
    assert platform["contractVersion"] == 1
    assert [tenant["tenant"] for tenant in platform["tenants"]] == ["a", "b"]
    assert platform["tenants"][0]["writeApiKey"].startswith("pk_topology_")
    assert platform["tenants"][0]["readApiKey"].startswith("pk_topology_")
    assert platform["tenants"][0]["writeApiKey"] != platform["tenants"][0]["readApiKey"]
    assert (
        stat.S_IMODE((output / "platform-receipt-fixture.json").stat().st_mode) == 0o600
    )

    with pytest.raises(ValueError, match="receipt_endpoint_incomplete"):
        fixture.prepare(
            PROFILE,
            tmp_path / "incomplete",
            PROBE_PATH,
            "prometa-runtime-host:topology-test",
            "0.18.0",
            "http://172.22.0.9:3000",
        )
    with pytest.raises(ValueError, match="receipt_endpoint_invalid"):
        fixture.prepare(
            PROFILE,
            tmp_path / "broad-cidr",
            PROBE_PATH,
            "prometa-runtime-host:topology-test",
            "0.18.0",
            "http://172.22.0.9:3000",
            "172.22.0.0/24",
        )


def test_topology_live_platform_verifier_checks_projection_and_isolation(
    tmp_path, monkeypatch
):
    pytest.importorskip("cryptography")
    fixture = _load_module("topology_fixture_platform_verify", FIXTURE_PATH)
    assets = tmp_path / "fixture"
    fixture.prepare(
        PROFILE,
        assets,
        PROBE_PATH,
        "prometa-runtime-host:topology-test",
        "0.18.0",
        "http://172.22.0.9:3000",
        "172.22.0.9/32",
    )
    platform_path = assets / "platform-receipt-fixture.json"
    tenants = json.loads(platform_path.read_text())["tenants"]

    def document(tenant):
        authorization = tenant["promotionAttestation"]["authorization"]
        attestation_id = tenant["promotionAttestation"]["attestationId"]
        contract = tenant["bundle"]["content"]["runtimeContract"]
        payloads = []
        for transition, outcome, instant in (
            ("admitted", "accepted", "2026-07-13T00:00:00.000Z"),
            ("active", "succeeded", "2026-07-13T00:00:01.000Z"),
        ):
            payloads.append(
                {
                    "receiptId": f"runtime-{transition}-{tenant['tenant']}",
                    "attestationId": attestation_id,
                    "artifactDigest": authorization["artifactDigest"],
                    "releaseId": tenant["releaseId"],
                    "deploymentId": tenant["deploymentId"],
                    "targetEnvironment": "prod",
                    "runtimeTarget": "tenant-runtime",
                    "runtimeId": tenant["runtimeId"],
                    "runtimeVersion": tenant["runtimeVersion"],
                    "policyDigest": contract["policyDigest"],
                    "configurationDigest": contract["configurationDigest"],
                    "transition": transition,
                    "outcome": outcome,
                    "reason": None,
                    "eventAt": instant,
                }
            )
        return {
            "receipts": [{"payload": payload} for payload in reversed(payloads)],
            "count": 2,
            "totalCount": 2,
            "projection": {
                "source": "authenticated_tenant_runtime_receipts",
                "authority": "tenant_runtime_assertion",
                "completeness": "complete",
                "receiptCount": 2,
                "attestationCount": 1,
                "deploymentId": tenant["deploymentId"],
                "releaseId": tenant["releaseId"],
                "runtimeTarget": "tenant-runtime",
                "warningCodes": [],
                "outOfOrderCount": 0,
                "milestones": {"admitted": True, "active": True},
                "latestAssertion": {"transition": "active", "outcome": "succeeded"},
            },
        }

    documents = {tenant["deploymentId"]: document(tenant) for tenant in tenants}

    def request(_base_url, path, api_key, *, method="GET", body=None):
        if method == "POST":
            if body["receiptId"] == "topology-invalid-binding-a":
                return 409, {"code": "attestation_binding_mismatch"}
            return 404, {"code": "attestation_not_found"}
        deployment_id = parse_qs(urlsplit(path).query)["deploymentId"][0]
        owner = next(item for item in tenants if item["deploymentId"] == deployment_id)
        if api_key != owner["readApiKey"]:
            return 200, {
                "receipts": [],
                "count": 0,
                "totalCount": 0,
                "projection": None,
            }
        return 200, documents[deployment_id]

    monkeypatch.setattr(fixture, "_platform_request", request)
    proof = tmp_path / "proof.json"
    fixture.verify_platform_receipts(
        platform_path, "http://127.0.0.1:3000", proof, timeout_seconds=1
    )

    assert json.loads(proof.read_text()) == {
        "contractVersion": 1,
        "mode": "live-platform",
        "runtimeContractVersion": 2,
        "runtimeReceiptsPerTenant": 2,
        "asynchronousReceiptDelivery": True,
        "policyConfigurationDigestBinding": True,
        "platformProjectionDigestBinding": True,
        "platformBindingValidation": True,
        "platformProjectionVisible": True,
        "receiptReadTenantIsolation": True,
        "receiptWriteTenantIsolation": True,
    }

    first = tenants[0]
    invalid = document(first)
    invalid["receipts"][0]["payload"]["policyDigest"] = "sha256:" + "d" * 64
    with pytest.raises(ValueError, match="platform_receipt_projection_invalid"):
        fixture._validate_platform_projection(invalid, first)


def test_topology_partition_removes_only_database_egress(tmp_path):
    fixture = _load_module("topology_fixture_partition", FIXTURE_PATH)
    source = tmp_path / "policy.json"
    original = tmp_path / "original.json"
    partition = tmp_path / "partition.json"
    source.write_text(
        json.dumps(
            {
                "apiVersion": "networking.k8s.io/v1",
                "kind": "NetworkPolicy",
                "metadata": {
                    "name": "runtime",
                    "namespace": "runtime-a",
                    "uid": "discarded",
                    "resourceVersion": "discarded",
                },
                "spec": {
                    "podSelector": {},
                    "policyTypes": ["Ingress", "Egress"],
                    "egress": [
                        {"ports": [{"protocol": "UDP", "port": 53}]},
                        {"ports": [{"protocol": "TCP", "port": 5432}]},
                        {"ports": [{"protocol": "TCP", "port": 8000}]},
                    ],
                },
            }
        )
    )

    fixture.write_partition_policies(source, original, partition)

    clean = json.loads(original.read_text())
    denied = json.loads(partition.read_text())
    assert set(clean["metadata"]) == {"name", "namespace"}
    assert [rule["ports"][0]["port"] for rule in clean["spec"]["egress"]] == [
        53,
        5432,
        8000,
    ]
    assert [rule["ports"][0]["port"] for rule in denied["spec"]["egress"]] == [
        53,
        8000,
    ]


def test_topology_inspection_requires_ready_spread_and_replacement(tmp_path):
    fixture = _load_module("topology_fixture_inspect", FIXTURE_PATH)
    initial = tmp_path / "initial.json"
    initial_result = tmp_path / "initial-result.json"
    replaced = tmp_path / "replaced.json"
    replaced_result = tmp_path / "replaced-result.json"
    initial.write_text(
        json.dumps(
            {
                "items": [
                    _pod("runtime-one", "10.42.0.10", "node-one"),
                    _pod("runtime-two", "10.42.1.10", "node-two"),
                ]
            }
        )
    )
    replaced.write_text(
        json.dumps(
            {
                "items": [
                    _pod("runtime-three", "10.42.0.11", "node-one"),
                    _pod("runtime-two", "10.42.1.10", "node-two"),
                ]
            }
        )
    )

    fixture.inspect_pods(initial, initial_result, 2)
    fixture.inspect_pods(replaced, replaced_result, 2, initial_result)

    assert json.loads(initial_result.read_text())["nodeCount"] == 2
    assert json.loads(replaced_result.read_text())["replacementNames"] == [
        "runtime-three"
    ]


def test_topology_log_and_report_evidence_is_payload_free(tmp_path):
    fixture = _load_module("topology_fixture_report", FIXTURE_PATH)
    created = tmp_path / "created.log"
    joined = tmp_path / "joined.log"
    log_result = tmp_path / "activations.json"
    report = tmp_path / "report.json"
    created.write_text(
        '{"type":"prometa.runtime.host","status":"ready","activation":"created"}\n'
    )
    joined.write_text(
        '{"type":"prometa.runtime.host","status":"ready","activation":"joined"}\n'
    )

    fixture.inspect_host_logs([created, joined], log_result, 1, 1)
    fixture.write_report(PROFILE, report, "v1.34.8+k3s1", 1, 1, 2, 2)

    evidence = json.loads(report.read_text())
    assert evidence["passed"] is True
    assert evidence["evidenceStatus"] == (
        "reference-profile-not-production-certification"
    )
    assert evidence["runtimeVersion"] == "0.18.0"
    assert evidence["chartVersion"] == "0.3.1"
    assert evidence["bundleSchemaVersion"] == 2
    assert evidence["runtimeContractVersion"] == 2
    assert evidence["capabilityRangeAdmission"] is True
    assert evidence["policyConfigurationDigestBinding"] is True
    assert evidence["typedSecretReferenceFieldPresent"] is True
    assert evidence["duplicateWinnersPerTenant"] == 1
    assert evidence["activationRowsPerTenant"] == 1
    assert evidence["synchronousControlPlaneCalls"] == 0
    assert all(
        evidence[key] is True
        for key in (
            "authorizedIngress",
            "podLabelIngressIsolation",
            "crossTenantIngressIsolation",
            "crossTenantEgressIsolation",
            "databasePartitionDeniedBeforeModel",
            "databasePartitionRecovery",
            "podReplacementJoinedActivation",
            "taskStatusSurvivedPodReplacement",
        )
    )
    lowered = report.read_text(encoding="utf-8").lower()
    for forbidden in (
        '"question"',
        '"answer"',
        '"token"',
        '"password"',
        '"signedpayload"',
        '"signature"',
    ):
        assert forbidden not in lowered


def test_topology_report_includes_only_validated_live_receipt_evidence(tmp_path):
    fixture = _load_module("topology_fixture_receipt_report", FIXTURE_PATH)
    proof = tmp_path / "proof.json"
    report = tmp_path / "report.json"
    proof.write_text(
        json.dumps(
            {
                "contractVersion": 1,
                "mode": "live-platform",
                "runtimeContractVersion": 2,
                "runtimeReceiptsPerTenant": 2,
                "asynchronousReceiptDelivery": True,
                "policyConfigurationDigestBinding": True,
                "platformProjectionDigestBinding": True,
                "platformBindingValidation": True,
                "platformProjectionVisible": True,
                "receiptReadTenantIsolation": True,
                "receiptWriteTenantIsolation": True,
            }
        )
    )

    fixture.write_report(
        PROFILE,
        report,
        "v1.34.8+k3s1",
        1,
        1,
        2,
        2,
        proof,
        2,
        2,
    )

    evidence = json.loads(report.read_text())
    assert evidence["receiptEvidenceMode"] == "live-platform"
    assert evidence["runtimeReceiptsPerTenant"] == 2
    assert evidence["receiptOutboxDeliveredPerTenant"] == 2
    assert evidence["asynchronousReceiptDelivery"] is True
    assert evidence["platformProjectionDigestBinding"] is True
    assert evidence["platformBindingValidation"] is True
    assert evidence["platformProjectionVisible"] is True
    assert evidence["receiptReadTenantIsolation"] is True
    assert evidence["receiptWriteTenantIsolation"] is True
    assert evidence["synchronousControlPlaneCalls"] == 0

    with pytest.raises(ValueError, match="platform_receipt_proof_invalid"):
        fixture.write_report(
            PROFILE,
            tmp_path / "invalid.json",
            "v1.34.8+k3s1",
            1,
            1,
            2,
            2,
            proof,
            1,
            2,
        )


def test_mcp_topology_report_is_payload_free_and_requires_observations(tmp_path):
    fixture = _load_module("topology_fixture_mcp_report", FIXTURE_PATH)
    report = tmp_path / "mcp-report.json"

    fixture.write_report(
        MCP_PROFILE,
        report,
        "v1.34.8+k3s1",
        1,
        1,
        2,
        2,
        mcp_audit_count_a=42,
        mcp_audit_count_b=40,
        mcp_indeterminate_count_a=1,
        mcp_indeterminate_count_b=1,
    )

    evidence = json.loads(report.read_text())
    assert evidence["workload"] == "mcp-read-only"
    assert evidence["mcpToolSideEffects"] == "read-only"
    assert evidence["mcpAuditRows"] == {"tenantA": 42, "tenantB": 40}
    assert evidence["mcpIndeterminateRowsPerTenant"] == 1
    assert "taskStatusSurvivedPodReplacement" not in evidence
    for key in (
        "officialStreamableHttpTransport",
        "signedMcpReleaseBinding",
        "separateMcpSecretProjection",
        "crossReplicaMcpIdempotency",
        "crossTenantMcpIngressIsolation",
        "crossTenantMcpEgressIsolation",
        "mcpAuditPersistedAcrossPodReplacement",
        "mcpCredentialRotationRequiresRollout",
        "mcpIndeterminateQuarantine",
    ):
        assert evidence[key] is True
    lowered = report.read_text(encoding="utf-8").lower()
    for forbidden in (
        '"question"',
        '"answer"',
        '"token"',
        '"password"',
        '"signedpayload"',
        '"signature"',
    ):
        assert forbidden not in lowered

    with pytest.raises(ValueError, match="mcp_topology_observation_invalid"):
        fixture.write_report(
            MCP_PROFILE,
            tmp_path / "invalid-mcp-report.json",
            "v1.34.8+k3s1",
            1,
            1,
            2,
            2,
            mcp_audit_count_a=42,
            mcp_audit_count_b=40,
            mcp_indeterminate_count_a=0,
            mcp_indeterminate_count_b=1,
        )


def test_topology_probe_rejects_ambiguous_json_and_invalid_urls():
    probe = _load_module("topology_probe_contract", PROBE_PATH)

    with pytest.raises(ValueError):
        probe._strict_json(b'{"duplicate":1,"duplicate":2}')
    with pytest.raises(probe.ProbeError, match="probe_urls_invalid"):
        probe._urls("https://runtime.example.test")
    assert probe._urls("http://10.42.0.1:8080,http://10.42.1.1:8080") == (
        "http://10.42.0.1:8080",
        "http://10.42.1.1:8080",
    )


def test_topology_probe_dispatches_mcp_duplicate_mode(monkeypatch):
    probe = _load_module("topology_probe_mcp_dispatch", PROBE_PATH)
    observed = {}

    def capture(*args):
        observed["args"] = args

    monkeypatch.setattr(probe, "duplicate_probe", capture)

    assert (
        probe.main(
            [
                "duplicates",
                "--urls",
                "http://10.42.0.1:8080,http://10.42.1.1:8080",
                "--request-id",
                "duplicate-a",
                "--attempts",
                "8",
                "--expect-answer",
                "tenant-a",
                "--mcp",
            ]
        )
        == 0
    )
    assert observed["args"] == (
        ("http://10.42.0.1:8080", "http://10.42.1.1:8080"),
        "duplicate-a",
        8,
        "tenant-a",
        12,
        True,
    )
