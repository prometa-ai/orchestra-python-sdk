"""Generate and inspect ephemeral tenant-runtime topology certification data."""

from __future__ import annotations

import argparse
import base64
import copy
import hashlib
import ipaddress
import json
import re
import secrets
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import Request, urlopen


TENANTS = ("a", "b")
RUNTIME_BUNDLE_SCHEMA_VERSION = 2
RUNTIME_CONTRACT_VERSION = 2
PROFILE_WORKLOADS = {
    "k3d-k3s-kube-router-v2": "model-only",
    "k3d-k3s-kube-router-mcp-v2": "mcp-read-only",
}
_TOOL_POLICY_KEYS = (
    "name",
    "source",
    "mcpServer",
    "operation",
    "sideEffects",
    "riskLevel",
    "authBinding",
    "scopes",
    "approvalRequired",
    "requiredGuardrails",
)
_TOOL_CONFIGURATION_KEYS = (
    "name",
    "source",
    "mcpServer",
    "operation",
    "inputSchema",
    "rateLimitPerMin",
)
_SHA256_PATTERN = re.compile(r"sha256:[0-9a-f]{64}")
_IMAGE_TAG_PATTERN = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}")
_RELEASE_TAG_PATTERN = re.compile(r"v[0-9]+\.[0-9]+\.[0-9]+(?:[-+][A-Za-z0-9_.-]+)?")
_REVISION_PATTERN = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})")


def _canonical(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _instant(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def _digest(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _selected(value: Mapping[str, Any], keys: Sequence[str]) -> Dict[str, Any]:
    return {key: value[key] for key in keys if key in value}


def _capability_requirement(capability: str) -> Mapping[str, Any]:
    name, separator, raw_version = capability.rpartition(".v")
    if not separator or not name or not raw_version.isdigit() or raw_version == "0":
        raise ValueError("runtime_capability_invalid")
    version = int(raw_version)
    return {"name": name, "minVersion": version, "maxVersion": version}


def _runtime_projection_digests(
    content: Mapping[str, Any], input_schema: Any, output_schema: Any
) -> Tuple[str, str]:
    tools = content.get("tools")
    if not isinstance(tools, list) or any(
        not isinstance(value, Mapping) for value in tools
    ):
        raise ValueError("runtime_projection_invalid")
    policy_digest = _digest(
        {
            "guardrails": content.get("guardrails", []),
            "identity": content.get("identity"),
            "tools": [_selected(value, _TOOL_POLICY_KEYS) for value in tools],
            "requiredScopes": content.get("requiredScopes", []),
            "grantedScopes": content.get("grantedScopes", []),
        }
    )
    configuration_digest = _digest(
        {
            "manifest": content.get("manifest"),
            "systemPrompt": content.get("systemPrompt"),
            "models": content.get("models"),
            "primaryModel": content.get("primaryModel"),
            "topology": content.get("topology"),
            "tools": [
                _selected(value, _TOOL_CONFIGURATION_KEYS) for value in tools
            ],
            "skills": content.get("skills", []),
            "knowledge": content.get("knowledge", []),
            "memory": content.get("memory", []),
            "subAgents": content.get("subAgents", []),
            "workflows": content.get("workflows", []),
            "triggers": content.get("triggers", []),
            "evaluation": content.get("evaluation", []),
            "inputSchema": input_schema,
            "outputSchema": output_schema,
            "mcpServers": content.get("mcpServers", []),
        }
    )
    return policy_digest, configuration_digest


def _read_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as stream:
        return json.load(stream)


def _write_json(path: Path, value: Any, *, private: bool = False) -> None:
    path.write_text(_canonical(value) + "\n", encoding="utf-8")
    path.chmod(0o600 if private else 0o644)


def _load_profile(path: Path) -> Mapping[str, Any]:
    document = _read_json(path)
    profiles = document.get("profiles") if isinstance(document, Mapping) else None
    if (
        not isinstance(document, Mapping)
        or document.get("contractVersion") != 1
        or not isinstance(profiles, list)
        or len(profiles) != 1
        or not isinstance(profiles[0], Mapping)
        or PROFILE_WORKLOADS.get(profiles[0].get("name"))
        != profiles[0].get("workload")
    ):
        raise ValueError("topology_profile_invalid")
    profile = profiles[0]
    required_strings = (
        "evidenceStatus",
        "workload",
        "networkPolicyController",
        "runtimeVersion",
        "chartVersion",
        "k3dVersion",
        "k3sImage",
        "k3sImageDigest",
        "postgresImage",
        "postgresImageDigest",
        "postgresNodeImage",
    )
    required_counts = (
        "serverNodes",
        "agentNodes",
        "tenantCount",
        "runtimeReplicasPerTenant",
        "uniqueLoadRequestsPerTenant",
        "duplicateAttemptsPerTenant",
    )
    if any(not isinstance(profile.get(key), str) for key in required_strings):
        raise ValueError("topology_profile_invalid")
    if any(
        type(profile.get(key)) is not int or profile[key] < 1
        for key in required_counts
    ):
        raise ValueError("topology_profile_invalid")
    if profile["tenantCount"] != len(TENANTS):
        raise ValueError("topology_profile_invalid")
    return profile


def _public_key(private_key: Any) -> str:
    from cryptography.hazmat.primitives import serialization

    der = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return base64.b64encode(der).decode("ascii")


def _sign(private_key: Any, payload: str) -> str:
    return base64.b64encode(private_key.sign(payload.encode("utf-8"))).decode(
        "ascii"
    )


def _runtime_content(tenant: str, workload: str) -> Mapping[str, Any]:
    primary = {
        "name": "Primary",
        "provider": "inference-engine",
        "modelName": "golden-model",
        "role": "primary",
        "temperature": 0.0,
        "maxOutputTokens": 128,
        "structuredOutput": True,
    }
    mcp_enabled = workload == "mcp-read-only"
    tool = {
        "name": "Tenant lookup",
        "source": "mcp",
        "operation": "lookup_tenant",
        "inputSchema": {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {"requestId": {"type": "string", "minLength": 1}},
            "required": ["requestId"],
            "additionalProperties": False,
        },
        "mcpServer": "Tenant Tools",
        "sideEffects": "read-only",
        "riskLevel": "low",
        "authBinding": "api-key",
        "scopes": ["tools:read"],
        "approvalRequired": False,
        "requiredGuardrails": [],
    }
    required_capabilities = [
        "evidence.emit.v1",
        "model.invoke.v1",
        "schema.validate.v1",
    ]
    if mcp_enabled:
        required_capabilities.append("tool.broker.v1")
    input_schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {"question": {"type": "string", "minLength": 1}},
        "required": ["question"],
        "additionalProperties": False,
    }
    output_schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {"answer": {"type": "string", "minLength": 1}},
        "required": ["answer"],
        "additionalProperties": False,
    }
    content: Dict[str, Any] = {
        "schemaVersion": RUNTIME_BUNDLE_SCHEMA_VERSION,
        "manifest": {
            "id": "manifest-topology-%s" % tenant,
            "name": "Topology Tenant %s" % tenant.upper(),
            "description": "Ephemeral tenant-cluster certification fixture",
            "version": 1,
            "status": "published",
            "agentId": "agent-topology-%s" % tenant,
            "solutionId": "solution-topology",
            "solutionName": "Topology Certification",
            "deployable": True,
        },
        "systemPrompt": (
            "Use the signed tenant lookup tool, then return the isolated tenant "
            "identifier."
            if mcp_enabled
            else "Return the isolated tenant identifier."
        ),
        "models": [primary],
        "primaryModel": primary,
        "topology": {"pattern": "single-react", "maxIterations": 1},
        "tools": [tool] if mcp_enabled else [],
        "skills": [],
        "knowledge": [],
        "memory": [],
        "subAgents": [],
        "workflows": [],
        "guardrails": [],
        "identity": None,
        "triggers": [],
        "evaluation": [],
        "mcpServers": ["Tenant Tools"] if mcp_enabled else [],
        "requiredScopes": ["tools:read"] if mcp_enabled else [],
        "grantedScopes": ["tools:read"] if mcp_enabled else [],
        "readiness": {
            "quality": 100,
            "security": 100,
            "maturity": 80,
            "productivity": 60,
        },
    }
    policy_digest, configuration_digest = _runtime_projection_digests(
        content, input_schema, output_schema
    )
    content["runtimeContract"] = {
        "contractVersion": RUNTIME_CONTRACT_VERSION,
        "requiredCapabilities": required_capabilities,
        "capabilityRequirements": [
            _capability_requirement(value) for value in required_capabilities
        ],
        "inputSchema": input_schema,
        "outputSchema": output_schema,
        "policyDigest": policy_digest,
        "configurationDigest": configuration_digest,
        "secretReferences": [],
    }
    return content


def _signed_release(
    tenant: str, now: datetime, workload: str = "model-only"
) -> Mapping[str, Any]:
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    bundle_key = Ed25519PrivateKey.generate()
    promotion_key = Ed25519PrivateKey.generate()
    org_id = "org-topology-%s" % tenant
    content = _runtime_content(tenant, workload)
    content_canonical = _canonical(content)
    digest = "sha256:" + hashlib.sha256(
        content_canonical.encode("utf-8")
    ).hexdigest()
    bundle_claims = {
        "envelopeVersion": 1,
        "issuer": "https://orchestra.example.test",
        "keyId": "topology-bundle-key-%s" % tenant,
        "orgId": org_id,
        "audience": "prometa-runtime",
        "targetEnvironment": "prod",
        "subject": "agent-manifest:manifest-topology-%s:v1" % tenant,
        "jti": "topology-bundle-%s" % tenant,
        "artifactDigest": digest,
        "contentCanonical": content_canonical,
        "issuedAt": _instant(now - timedelta(minutes=2)),
        "notBefore": _instant(now - timedelta(minutes=2)),
        "expiresAt": _instant(now + timedelta(hours=1)),
        "offlineLeaseExpiresAt": _instant(now + timedelta(minutes=45)),
    }
    bundle_payload = _canonical(bundle_claims)
    bundle = {
        "content": content,
        "algorithm": "ed25519",
        "envelopeVersion": 1,
        "envelopeCanonicalization": "signed-payload-json-v1",
        "signedPayload": bundle_payload,
        "envelopeSignature": _sign(bundle_key, bundle_payload),
        "artifactDigest": digest,
        "issuer": bundle_claims["issuer"],
        "keyId": bundle_claims["keyId"],
        "signed": True,
    }
    attestation_id = "topology-attestation-%s" % tenant
    release_id = "topology-release-%s" % tenant
    deployment_id = "topology-deployment-%s" % tenant
    promotion_claims = {
        "artifactType": "orchestra.promotion-attestation",
        "attestationVersion": 1,
        "issuer": "https://orchestra.example.test/promotion",
        "keyId": "topology-promotion-key-%s" % tenant,
        "subject": "promotion-attestation:%s" % attestation_id,
        "orgId": org_id,
        "audience": "prometa-runtime-admission",
        "targetEnvironment": "prod",
        "artifactId": "topology-artifact-%s" % tenant,
        "artifactDigest": digest,
        "manifestId": "manifest-topology-%s" % tenant,
        "manifestVersion": 1,
        "agentId": "agent-topology-%s" % tenant,
        "decisionId": "topology-decision-%s" % tenant,
        "decisionAllow": True,
        "gateStage": "prod",
        "policySetDigest": "sha256:" + "b" * 64,
        "evidenceDigest": "sha256:" + "c" * 64,
        "decisionEvaluatedAt": _instant(now - timedelta(minutes=3)),
        "decisionValidUntil": _instant(now + timedelta(minutes=40)),
        "requestedRuntime": "tenant-runtime",
        "releaseId": release_id,
        "deploymentId": deployment_id,
        "approvals": [],
        "issuedAt": _instant(now - timedelta(minutes=1)),
        "notBefore": _instant(now - timedelta(minutes=1)),
        "expiresAt": _instant(now + timedelta(minutes=30)),
        "offlineLeaseExpiresAt": _instant(now + timedelta(minutes=20)),
        "jti": "topology-promotion-%s" % tenant,
        "revocationRef": "urn:prometa:promotion-attestation:%s"
        % attestation_id,
    }
    promotion_payload = _canonical(promotion_claims)
    attestation = {
        "attestationId": attestation_id,
        "attestationVersion": 1,
        "algorithm": "ed25519",
        "canonicalization": "signed-payload-json-v1",
        "issuer": promotion_claims["issuer"],
        "keyId": promotion_claims["keyId"],
        "signedPayload": promotion_payload,
        "signature": _sign(promotion_key, promotion_payload),
        "signed": True,
        "authorization": {
            "artifactId": promotion_claims["artifactId"],
            "artifactDigest": digest,
            "decisionId": promotion_claims["decisionId"],
            "releaseId": release_id,
            "deploymentId": deployment_id,
            "targetEnvironment": "prod",
            "requestedRuntime": "tenant-runtime",
            "expiresAt": promotion_claims["expiresAt"],
            "offlineLeaseExpiresAt": promotion_claims[
                "offlineLeaseExpiresAt"
            ],
        },
    }
    return {
        "orgId": org_id,
        "releaseId": release_id,
        "deploymentId": deployment_id,
        "bundle": bundle,
        "attestation": attestation,
        "bundleTrust": {
            "issuer": bundle_claims["issuer"],
            "keyId": bundle_claims["keyId"],
            "publicKeySpkiDerBase64": _public_key(bundle_key),
            "allowedOrgIds": [org_id],
            "allowedAudiences": ["prometa-runtime"],
            "allowedEnvironments": ["prod"],
        },
        "promotionTrust": {
            "issuer": promotion_claims["issuer"],
            "keyId": promotion_claims["keyId"],
            "publicKeySpkiDerBase64": _public_key(promotion_key),
            "allowedOrgIds": [org_id],
            "allowedAudiences": ["prometa-runtime-admission"],
            "allowedEnvironments": ["prod"],
        },
    }


def _runtime_config(
    tenant: str,
    release: Mapping[str, Any],
    runtime_version: str,
    receipt_base_url: Optional[str] = None,
    workload: str = "model-only",
) -> Mapping[str, Any]:
    config: Dict[str, Any] = {
        "configVersion": 1,
        "tenantId": "tenant-topology-%s" % tenant,
        "runtimeId": "runtime-topology-%s" % tenant,
        "runtimeVersion": runtime_version,
        "orgId": release["orgId"],
        "environment": "prod",
        "releaseId": release["releaseId"],
        "deploymentId": release["deploymentId"],
        "runtimeTarget": "tenant-runtime",
        "bundle": release["bundle"],
        "promotionAttestation": release["attestation"],
        "bundleTrust": [release["bundleTrust"]],
        "promotionTrust": [release["promotionTrust"]],
        "modelGateway": {
            "baseUrl": "http://model-gateway.models-%s.svc.cluster.local:8000"
            % tenant,
            "endpointPath": "/v1/chat/completions",
            "timeoutSeconds": 2,
            "maxResponseBytes": 1048576,
        },
        "databaseDsnEnv": "PROMETA_RUNTIME_DATABASE_URL",
        "apiTokenEnv": "PROMETA_RUNTIME_API_TOKEN",
        "requestTimeoutSeconds": 8,
        "maxRequestBytes": 65536,
    }
    if workload == "model-only":
        config["taskRecovery"] = {
            "leaseSeconds": 15,
            "maxAttempts": 3,
            "historyLimit": 50,
        }
    else:
        config["mcpBroker"] = {
            "servers": [
                {
                    "name": "Tenant Tools",
                    "connectionId": "tenant-tools-%s" % tenant,
                    "transport": "streamable-http",
                    "environment": "production",
                    "authMode": "api-key",
                    "scopes": ["tools:read"],
                    "riskLevel": "low",
                    "endpoint": (
                        "http://mcp-integration.tools-%s.svc.cluster.local:8000/mcp"
                        % tenant
                    ),
                    "allowInsecureHttp": True,
                    "timeoutSeconds": 3,
                    "maxResponseBytes": 65536,
                }
            ],
            "grants": [
                {
                    "toolName": "lookup_tenant",
                    "agentIds": ["agent-topology-%s" % tenant],
                    "permission": "read",
                    "riskLevel": "low",
                    "serverConnectionId": "tenant-tools-%s" % tenant,
                }
            ],
            "policy": {
                "maxRiskLevel": "low",
                "requireApprovalFor": ["write", "destructive"],
                "requireIdempotencyFor": ["write", "destructive"],
            },
            "egress": {
                "allowedHttpOrigins": [
                    "http://mcp-integration.tools-%s.svc.cluster.local:8000"
                    % tenant
                ],
                "allowedStdioCommands": [],
            },
            "credentialBindings": [
                {
                    "serverName": "Tenant Tools",
                    "authMode": "api-key",
                    "httpHeaders": {
                        "Authorization": "MCP_TOPOLOGY_AUTHORIZATION"
                    },
                    "stdioEnvironment": {},
                }
            ],
            "toolTimeoutSeconds": 5,
            "reservationTimeoutSeconds": 15,
        }
    if receipt_base_url is not None:
        config["receiptDelivery"] = {
            "baseUrl": receipt_base_url,
            "apiKeyEnv": "ORCHESTRA_RUNTIME_RECEIPT_API_KEY",
            "allowInsecureHttp": True,
            "timeoutSeconds": 3,
            "pollIntervalSeconds": 1,
            "leaseSeconds": 15,
            "initialBackoffSeconds": 1,
            "maxBackoffSeconds": 8,
        }
    return config


def _restricted_pod_security() -> Mapping[str, Any]:
    return {
        "runAsNonRoot": True,
        "runAsUser": 10001,
        "runAsGroup": 10001,
        "seccompProfile": {"type": "RuntimeDefault"},
    }


def _restricted_container_security() -> Mapping[str, Any]:
    return {
        "allowPrivilegeEscalation": False,
        "readOnlyRootFilesystem": True,
        "capabilities": {"drop": ["ALL"]},
    }


def _support_volumes() -> Tuple[Mapping[str, Any], ...]:
    return (
        {"name": "support", "configMap": {"name": "topology-support"}},
        {"name": "tmp", "emptyDir": {"sizeLimit": "32Mi"}},
    )


def _support_mounts() -> Tuple[Mapping[str, Any], ...]:
    return (
        {"name": "support", "mountPath": "/opt/topology", "readOnly": True},
        {"name": "tmp", "mountPath": "/tmp"},
    )


def _namespace(name: str, tenant: str, policy: str) -> Mapping[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": name,
            "labels": {
                "topology.prometa.io/tenant": tenant,
                "pod-security.kubernetes.io/enforce": policy,
                "pod-security.kubernetes.io/enforce-version": "v1.34",
                "pod-security.kubernetes.io/audit": policy,
                "pod-security.kubernetes.io/warn": policy,
            },
        },
    }


def _config_map(namespace: str, source: str) -> Mapping[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "topology-support", "namespace": namespace},
        "data": {"topology_probe.py": source},
    }


def _postgres_resources(
    tenant: str, image: str, password: str
) -> Tuple[Mapping[str, Any], ...]:
    namespace = "data-%s" % tenant
    labels = {"app.kubernetes.io/name": "postgresql"}
    return (
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "postgres-credentials", "namespace": namespace},
            "stringData": {"password": password},
        },
        {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "postgres", "namespace": namespace},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": labels},
                "template": {
                    "metadata": {"labels": labels},
                    "spec": {
                        "containers": [
                            {
                                "name": "postgres",
                                "image": image,
                                "imagePullPolicy": "Never",
                                "ports": [{"name": "postgres", "containerPort": 5432}],
                                "env": [
                                    {"name": "POSTGRES_USER", "value": "runtime"},
                                    {"name": "POSTGRES_DB", "value": "runtime"},
                                    {
                                        "name": "POSTGRES_PASSWORD",
                                        "valueFrom": {
                                            "secretKeyRef": {
                                                "name": "postgres-credentials",
                                                "key": "password",
                                            }
                                        },
                                    },
                                ],
                                "readinessProbe": {
                                    "exec": {
                                        "command": [
                                            "pg_isready",
                                            "-U",
                                            "runtime",
                                            "-d",
                                            "runtime",
                                        ]
                                    },
                                    "periodSeconds": 2,
                                    "timeoutSeconds": 2,
                                    "failureThreshold": 30,
                                },
                                "resources": {
                                    "requests": {"cpu": "50m", "memory": "64Mi"},
                                    "limits": {"cpu": "500m", "memory": "256Mi"},
                                },
                                "volumeMounts": [
                                    {"name": "data", "mountPath": "/var/lib/postgresql/data"}
                                ],
                            }
                        ],
                        "volumes": [{"name": "data", "emptyDir": {}}],
                    },
                },
            },
        },
        {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "postgres", "namespace": namespace},
            "spec": {
                "selector": labels,
                "ports": [{"name": "postgres", "port": 5432, "targetPort": 5432}],
            },
        },
    )


def _model_resources(
    tenant: str, image: str, source: str, workload: str
) -> Tuple[Mapping[str, Any], ...]:
    namespace = "models-%s" % tenant
    labels = {"app.kubernetes.io/name": "model-gateway"}
    arguments = ["model-gateway", "--tenant", "tenant-%s" % tenant]
    if workload == "mcp-read-only":
        arguments.append("--mcp")
    container = {
        "name": "model-gateway",
        "image": image,
        "imagePullPolicy": "Never",
        "command": ["python", "/opt/topology/topology_probe.py"],
        "args": arguments,
        "ports": [{"name": "http", "containerPort": 8000}],
        "securityContext": _restricted_container_security(),
        "readinessProbe": {
            "httpGet": {"path": "/count", "port": "http"},
            "periodSeconds": 2,
            "timeoutSeconds": 2,
            "failureThreshold": 30,
        },
        "resources": {
            "requests": {"cpu": "25m", "memory": "32Mi"},
            "limits": {"cpu": "250m", "memory": "128Mi"},
        },
        "volumeMounts": list(_support_mounts()),
    }
    return (
        _config_map(namespace, source),
        {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "model-gateway", "namespace": namespace},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": labels},
                "template": {
                    "metadata": {"labels": labels},
                    "spec": {
                        "securityContext": _restricted_pod_security(),
                        "containers": [container],
                        "volumes": list(_support_volumes()),
                    },
                },
            },
        },
        {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "model-gateway", "namespace": namespace},
            "spec": {
                "selector": labels,
                "ports": [{"name": "http", "port": 8000, "targetPort": 8000}],
            },
        },
    )


def _mcp_resources(
    tenant: str,
    image: str,
    source: str,
    token: str,
) -> Tuple[Mapping[str, Any], ...]:
    namespace = "tools-%s" % tenant
    labels = {"app.kubernetes.io/name": "mcp-integration"}
    return (
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "topology-mcp-server", "namespace": namespace},
            "data": {"topology_mcp_server.py": source},
        },
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "mcp-server-credentials", "namespace": namespace},
            "stringData": {"token": token},
        },
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": "runtime-mcp-credentials",
                "namespace": "runtime-%s" % tenant,
            },
            "stringData": {"authorization": "Bearer %s" % token},
        },
        {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "mcp-integration", "namespace": namespace},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": labels},
                "template": {
                    "metadata": {"labels": labels},
                    "spec": {
                        "automountServiceAccountToken": False,
                        "securityContext": _restricted_pod_security(),
                        "containers": [
                            {
                                "name": "mcp-integration",
                                "image": image,
                                "imagePullPolicy": "Never",
                                "command": [
                                    "python",
                                    "/opt/topology/topology_mcp_server.py",
                                ],
                                "args": ["--tenant", tenant, "--port", "8000"],
                                "ports": [{"name": "http", "containerPort": 8000}],
                                "securityContext": _restricted_container_security(),
                                "readinessProbe": {
                                    "httpGet": {"path": "/healthz", "port": "http"},
                                    "periodSeconds": 2,
                                    "timeoutSeconds": 2,
                                    "failureThreshold": 45,
                                },
                                "resources": {
                                    "requests": {"cpu": "25m", "memory": "64Mi"},
                                    "limits": {"cpu": "500m", "memory": "256Mi"},
                                },
                                "volumeMounts": [
                                    {
                                        "name": "support",
                                        "mountPath": "/opt/topology",
                                        "readOnly": True,
                                    },
                                    {
                                        "name": "credentials",
                                        "mountPath": "/var/run/secrets/prometa-mcp",
                                        "readOnly": True,
                                    },
                                    {"name": "tmp", "mountPath": "/tmp"},
                                ],
                            }
                        ],
                        "volumes": [
                            {
                                "name": "support",
                                "configMap": {"name": "topology-mcp-server"},
                            },
                            {
                                "name": "credentials",
                                "secret": {"secretName": "mcp-server-credentials"},
                            },
                            {"name": "tmp", "emptyDir": {"sizeLimit": "32Mi"}},
                        ],
                    },
                },
            },
        },
        {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": "mcp-integration", "namespace": namespace},
            "spec": {
                "selector": labels,
                "ports": [{"name": "http", "port": 8000, "targetPort": 8000}],
            },
        },
        {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {"name": "mcp-integration", "namespace": namespace},
            "spec": {
                "podSelector": {"matchLabels": labels},
                "policyTypes": ["Ingress", "Egress"],
                "ingress": [
                    {
                        "from": [
                            {
                                "namespaceSelector": {
                                    "matchLabels": {
                                        "kubernetes.io/metadata.name": "runtime-%s"
                                        % tenant
                                    }
                                },
                                "podSelector": {
                                    "matchLabels": {
                                        "app.kubernetes.io/component": "runtime"
                                    }
                                },
                            }
                        ],
                        "ports": [{"protocol": "TCP", "port": 8000}],
                    }
                ],
                "egress": [],
            },
        },
    )


def _probe_pod(
    tenant: str,
    image: str,
    *,
    name: str,
    app_name: str,
) -> Mapping[str, Any]:
    namespace = "gateway-%s" % tenant
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {"app.kubernetes.io/name": app_name},
        },
        "spec": {
            "restartPolicy": "Always",
            "automountServiceAccountToken": False,
            "securityContext": _restricted_pod_security(),
            "containers": [
                {
                    "name": "probe",
                    "image": image,
                    "imagePullPolicy": "Never",
                    "command": ["python", "/opt/topology/topology_probe.py"],
                    "args": ["sleep"],
                    "env": [
                        {
                            "name": "RUNTIME_API_TOKEN",
                            "valueFrom": {
                                "secretKeyRef": {
                                    "name": "probe-credentials",
                                    "key": "api-token",
                                }
                            },
                        }
                    ],
                    "securityContext": _restricted_container_security(),
                    "resources": {
                        "requests": {"cpu": "10m", "memory": "24Mi"},
                        "limits": {"cpu": "500m", "memory": "128Mi"},
                    },
                    "volumeMounts": list(_support_mounts()),
                }
            ],
            "volumes": list(_support_volumes()),
        },
    }


def _gateway_resources(
    tenant: str, image: str, source: str, api_token: str
) -> Tuple[Mapping[str, Any], ...]:
    namespace = "gateway-%s" % tenant
    return (
        _config_map(namespace, source),
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "probe-credentials", "namespace": namespace},
            "stringData": {"api-token": api_token},
        },
        _probe_pod(
            tenant,
            image,
            name="probe",
            app_name="tenant-ai-gateway",
        ),
        _probe_pod(
            tenant,
            image,
            name="rogue",
            app_name="rogue-client",
        ),
    )


def _chart_values(
    tenant: str,
    runtime_image: str,
    replicas: int,
    receipt_endpoint_cidr: Optional[str] = None,
    workload: str = "model-only",
    profile_name: str = "k3d-k3s-kube-router-v2",
) -> Mapping[str, Any]:
    image_values = _runtime_image_values(runtime_image)
    runtime_namespace = "runtime-%s" % tenant
    values: Dict[str, Any] = {
        "fullnameOverride": "runtime",
        "replicaCount": replicas,
        "image": {**image_values, "pullPolicy": "Never"},
        "runtimeConfig": {
            "existingSecret": "runtime-release",
            "rolloutId": "topology-deployment-%s" % tenant,
        },
        "credentials": {"existingSecret": "runtime-credentials"},
        "podLabels": {"topology.prometa.io/tenant": tenant},
        "resources": {
            "requests": {"cpu": "50m", "memory": "96Mi"},
            "limits": {"cpu": "500m", "memory": "256Mi"},
        },
        "probes": {
            "startup": {
                "periodSeconds": 2,
                "timeoutSeconds": 2,
                "failureThreshold": 45,
            },
            "liveness": {
                "initialDelaySeconds": 2,
                "periodSeconds": 5,
                "timeoutSeconds": 2,
                "failureThreshold": 3,
            },
            "readiness": {
                "initialDelaySeconds": 1,
                "periodSeconds": 3,
                "timeoutSeconds": 2,
                "failureThreshold": 3,
            },
        },
        "gracefulShutdown": {
            "terminationGracePeriodSeconds": 20,
            "preStopSleepSeconds": 1,
        },
        "migration": {
            "enabled": True,
            "compatibilityCheck": True,
            "serviceAccountName": "default",
            "networkPolicy": {
                "enabled": True,
                "allowDNS": True,
                "egress": [
                    {
                        "to": [
                            {
                                "namespaceSelector": {
                                    "matchLabels": {
                                        "kubernetes.io/metadata.name": "data-%s"
                                        % tenant
                                    }
                                },
                                "podSelector": {
                                    "matchLabels": {
                                        "app.kubernetes.io/name": "postgresql"
                                    }
                                },
                            }
                        ],
                        "ports": [{"protocol": "TCP", "port": 5432}],
                    }
                ],
            },
        },
        "backup": {"enabled": False},
        "networkPolicy": {
            "enabled": True,
            "allowDNS": True,
            "ingress": [
                {
                    "from": [
                        {
                            "namespaceSelector": {
                                "matchLabels": {
                                    "kubernetes.io/metadata.name": "gateway-%s"
                                    % tenant
                                }
                            },
                            "podSelector": {
                                "matchLabels": {
                                    "app.kubernetes.io/name": "tenant-ai-gateway"
                                }
                            },
                        }
                    ],
                    "ports": [{"protocol": "TCP", "port": 8080}],
                }
            ],
            "egress": [
                {
                    "to": [
                        {
                            "namespaceSelector": {
                                "matchLabels": {
                                    "kubernetes.io/metadata.name": "data-%s"
                                    % tenant
                                }
                            },
                            "podSelector": {
                                "matchLabels": {
                                    "app.kubernetes.io/name": "postgresql"
                                }
                            },
                        }
                    ],
                    "ports": [{"protocol": "TCP", "port": 5432}],
                },
                {
                    "to": [
                        {
                            "namespaceSelector": {
                                "matchLabels": {
                                    "kubernetes.io/metadata.name": "models-%s"
                                    % tenant
                                }
                            },
                            "podSelector": {
                                "matchLabels": {
                                    "app.kubernetes.io/name": "model-gateway"
                                }
                            },
                        }
                    ],
                    "ports": [{"protocol": "TCP", "port": 8000}],
                },
            ],
        },
        "podDisruptionBudget": {"enabled": True, "minAvailable": 1},
        "autoscaling": {"enabled": False},
        "strategy": {
            "type": "RollingUpdate",
            "rollingUpdate": {"maxUnavailable": 0, "maxSurge": 1},
        },
        "topologySpreadConstraints": [
            {
                "maxSkew": 1,
                "topologyKey": "kubernetes.io/hostname",
                "whenUnsatisfiable": "DoNotSchedule",
                "labelSelector": {
                    "matchLabels": {
                        "app.kubernetes.io/name": "prometa-runtime",
                        "app.kubernetes.io/instance": "runtime",
                    }
                },
            }
        ],
        "extraVolumes": [
            {"name": "topology-support", "configMap": {"name": "topology-support"}}
        ],
        "extraVolumeMounts": [
            {
                "name": "topology-support",
                "mountPath": "/opt/topology",
                "readOnly": True,
            }
        ],
        "service": {"port": 8080},
        "serviceAccount": {"automountServiceAccountToken": False},
        "podAnnotations": {
            "topology.prometa.io/profile": profile_name,
            "topology.prometa.io/runtime-namespace": runtime_namespace,
        },
    }
    if workload == "mcp-read-only":
        values["extraEnv"] = [
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
        values["networkPolicy"]["egress"].append(
            {
                "to": [
                    {
                        "namespaceSelector": {
                            "matchLabels": {
                                "kubernetes.io/metadata.name": "tools-%s" % tenant
                            }
                        },
                        "podSelector": {
                            "matchLabels": {
                                "app.kubernetes.io/name": "mcp-integration"
                            }
                        },
                    }
                ],
                "ports": [{"protocol": "TCP", "port": 8000}],
            }
        )
    if receipt_endpoint_cidr is not None:
        values["credentials"]["receiptApiKeyOptional"] = False
        values["networkPolicy"]["egress"].append(
            {
                "to": [{"ipBlock": {"cidr": receipt_endpoint_cidr}}],
                "ports": [{"protocol": "TCP", "port": 3000}],
            }
        )
    return values


def _runtime_image_values(runtime_image: str) -> Mapping[str, str]:
    if not runtime_image or len(runtime_image) > 512 or any(
        character.isspace() for character in runtime_image
    ):
        raise ValueError("runtime_image_invalid")
    if "@" in runtime_image:
        if runtime_image.count("@") != 1:
            raise ValueError("runtime_image_invalid")
        repository, digest = runtime_image.rsplit("@", 1)
        if not repository or not _SHA256_PATTERN.fullmatch(digest):
            raise ValueError("runtime_image_invalid")
        return {"repository": repository, "digest": digest}
    if ":" not in runtime_image:
        raise ValueError("runtime_image_invalid")
    repository, tag = runtime_image.rsplit(":", 1)
    if not repository or not _IMAGE_TAG_PATTERN.fullmatch(tag):
        raise ValueError("runtime_image_invalid")
    return {"repository": repository, "tag": tag}


def _validated_receipt_endpoint(
    base_url: Optional[str], endpoint_cidr: Optional[str]
) -> Tuple[Optional[str], Optional[str]]:
    if (base_url is None) != (endpoint_cidr is None):
        raise ValueError("receipt_endpoint_incomplete")
    if base_url is None or endpoint_cidr is None:
        return None, None
    parsed = urlsplit(base_url)
    if (
        parsed.scheme != "http"
        or not parsed.hostname
        or parsed.port != 3000
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in ("", "/")
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("receipt_base_url_invalid")
    try:
        address = ipaddress.ip_address(parsed.hostname)
        network = ipaddress.ip_network(endpoint_cidr, strict=True)
    except ValueError:
        raise ValueError("receipt_endpoint_invalid") from None
    if (
        address.version != 4
        or network.version != 4
        or network.prefixlen != 32
        or address != network.network_address
    ):
        raise ValueError("receipt_endpoint_invalid")
    return "http://%s:3000" % address, str(network)


def prepare(
    profile_path: Path,
    output_dir: Path,
    probe_source_path: Path,
    runtime_image: str,
    runtime_version: str,
    receipt_base_url: Optional[str] = None,
    receipt_endpoint_cidr: Optional[str] = None,
    mcp_server_source_path: Optional[Path] = None,
) -> None:
    profile = _load_profile(profile_path)
    workload = str(profile["workload"])
    mcp_enabled = workload == "mcp-read-only"
    _runtime_image_values(runtime_image)
    if runtime_version != profile["runtimeVersion"]:
        raise ValueError("runtime_version_mismatch")
    receipt_base_url, receipt_endpoint_cidr = _validated_receipt_endpoint(
        receipt_base_url, receipt_endpoint_cidr
    )
    if mcp_enabled and receipt_base_url is not None:
        raise ValueError("mcp_receipt_combination_not_certified")
    if mcp_enabled and mcp_server_source_path is None:
        raise ValueError("mcp_server_source_missing")
    source = probe_source_path.read_text(encoding="utf-8")
    mcp_source = (
        mcp_server_source_path.read_text(encoding="utf-8")
        if mcp_server_source_path is not None
        else None
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    output_dir.chmod(0o700)
    now = datetime.now(timezone.utc).replace(microsecond=0)
    resources = []
    platform_tenants = []
    namespace_profiles = [
        ("runtime", "restricted"),
        ("gateway", "restricted"),
        ("models", "restricted"),
        ("data", "baseline"),
    ]
    if mcp_enabled:
        namespace_profiles.append(("tools", "restricted"))
    for tenant in TENANTS:
        for prefix, policy in namespace_profiles:
            resources.append(_namespace("%s-%s" % (prefix, tenant), tenant, policy))
        release = _signed_release(tenant, now, workload)
        api_token = secrets.token_urlsafe(36)
        receipt_write_key = "pk_topology_" + secrets.token_urlsafe(36)
        receipt_read_key = "pk_topology_" + secrets.token_urlsafe(36)
        postgres_password = secrets.token_urlsafe(30)
        dsn = (
            "postgresql://runtime:%s@postgres.data-%s.svc.cluster.local:5432/"
            "runtime?connect_timeout=2"
        ) % (quote(postgres_password, safe=""), tenant)
        _write_json(
            output_dir / ("tenant-%s-config.json" % tenant),
            _runtime_config(
                tenant,
                release,
                runtime_version,
                receipt_base_url=receipt_base_url,
                workload=workload,
            ),
            private=True,
        )
        credentials = (
            "database-url=%s\nmigration-database-url=%s\napi-token=%s\n%s"
        ) % (
            dsn,
            dsn,
            api_token,
            (
                "receipt-api-key=%s\n" % receipt_write_key
                if receipt_base_url is not None
                else ""
            ),
        )
        credentials_path = output_dir / ("tenant-%s-credentials.env" % tenant)
        credentials_path.write_text(credentials, encoding="utf-8")
        credentials_path.chmod(0o600)
        _write_json(
            output_dir / ("tenant-%s-values.json" % tenant),
            _chart_values(
                tenant,
                runtime_image,
                int(profile["runtimeReplicasPerTenant"]),
                receipt_endpoint_cidr=receipt_endpoint_cidr,
                workload=workload,
                profile_name=str(profile["name"]),
            ),
        )
        if receipt_base_url is not None:
            platform_tenants.append(
                {
                    "tenant": tenant,
                    "orgId": release["orgId"],
                    "runtimeId": "runtime-topology-%s" % tenant,
                    "runtimeVersion": runtime_version,
                    "releaseId": release["releaseId"],
                    "deploymentId": release["deploymentId"],
                    "writeApiKey": receipt_write_key,
                    "readApiKey": receipt_read_key,
                    "bundle": release["bundle"],
                    "promotionAttestation": release["attestation"],
                }
            )
        resources.append(_config_map("runtime-%s" % tenant, source))
        resources.extend(
            _postgres_resources(
                tenant,
                str(profile["postgresNodeImage"]),
                postgres_password,
            )
        )
        resources.extend(_model_resources(tenant, runtime_image, source, workload))
        resources.extend(_gateway_resources(tenant, runtime_image, source, api_token))
        if mcp_enabled:
            mcp_token = secrets.token_urlsafe(36)
            rotated_mcp_token = secrets.token_urlsafe(36)
            resources.extend(
                _mcp_resources(tenant, runtime_image, mcp_source or "", mcp_token)
            )
            for suffix, value in (
                ("mcp-server.env", "token=%s\n" % rotated_mcp_token),
                (
                    "mcp-runtime.env",
                    "authorization=Bearer %s\n" % rotated_mcp_token,
                ),
            ):
                path = output_dir / ("tenant-%s-rotated-%s" % (tenant, suffix))
                path.write_text(value, encoding="utf-8")
                path.chmod(0o600)
    _write_json(
        output_dir / "support-resources.json",
        {"apiVersion": "v1", "kind": "List", "items": resources},
        private=True,
    )
    if receipt_base_url is not None:
        _write_json(
            output_dir / "platform-receipt-fixture.json",
            {
                "contractVersion": 1,
                "purpose": "ephemeral-runtime-receipt-topology-certification",
                "tenants": platform_tenants,
            },
            private=True,
        )


def _clean_network_policy(document: Mapping[str, Any]) -> Dict[str, Any]:
    if document.get("apiVersion") != "networking.k8s.io/v1" or document.get(
        "kind"
    ) != "NetworkPolicy":
        raise ValueError("network_policy_invalid")
    metadata = document.get("metadata")
    spec = document.get("spec")
    if not isinstance(metadata, Mapping) or not isinstance(spec, Mapping):
        raise ValueError("network_policy_invalid")
    clean_metadata = {
        key: copy.deepcopy(metadata[key])
        for key in ("name", "namespace", "labels", "annotations")
        if key in metadata
    }
    if not clean_metadata.get("name") or not clean_metadata.get("namespace"):
        raise ValueError("network_policy_invalid")
    return {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": clean_metadata,
        "spec": copy.deepcopy(spec),
    }


def write_partition_policies(
    input_path: Path,
    original_output: Path,
    partition_output: Path,
) -> None:
    original = _clean_network_policy(_read_json(input_path))
    partition = copy.deepcopy(original)
    egress = partition["spec"].get("egress")
    if not isinstance(egress, list):
        raise ValueError("network_policy_invalid")
    kept = []
    removed = 0
    for rule in egress:
        ports = rule.get("ports", []) if isinstance(rule, Mapping) else []
        if any(
            isinstance(port, Mapping) and int(port.get("port", -1)) == 5432
            for port in ports
        ):
            removed += 1
        else:
            kept.append(rule)
    if removed != 1 or len(kept) < 2:
        raise ValueError("database_egress_rule_invalid")
    partition["spec"]["egress"] = kept
    _write_json(original_output, original)
    _write_json(partition_output, partition)


def inspect_pods(
    input_path: Path,
    output_path: Path,
    expected_replicas: int,
    previous_path: Optional[Path] = None,
) -> None:
    document = _read_json(input_path)
    items = document.get("items") if isinstance(document, Mapping) else None
    if not isinstance(items, list) or len(items) != expected_replicas:
        raise ValueError("runtime_replica_count_invalid")
    records = []
    for item in items:
        metadata = item.get("metadata", {})
        status = item.get("status", {})
        conditions = status.get("conditions", [])
        ready = any(
            condition.get("type") == "Ready" and condition.get("status") == "True"
            for condition in conditions
            if isinstance(condition, Mapping)
        )
        name = metadata.get("name")
        pod_ip = status.get("podIP")
        node_name = item.get("spec", {}).get("nodeName")
        if not ready or not all(
            isinstance(value, str) and value for value in (name, pod_ip, node_name)
        ):
            raise ValueError("runtime_pod_not_ready")
        records.append((name, pod_ip, node_name))
    records.sort()
    nodes = {record[2] for record in records}
    if len(nodes) != expected_replicas:
        raise ValueError("runtime_topology_spread_failed")
    previous_names = set()
    if previous_path is not None:
        previous = _read_json(previous_path)
        previous_names = set(previous.get("podNames", []))
        if len(previous_names) != expected_replicas:
            raise ValueError("previous_runtime_pods_invalid")
    names = [record[0] for record in records]
    replacements = sorted(set(names) - previous_names) if previous_names else []
    if previous_path is not None and not replacements:
        raise ValueError("runtime_replacement_missing")
    _write_json(
        output_path,
        {
            "replicaCount": len(records),
            "nodeCount": len(nodes),
            "podNames": names,
            "podIps": [record[1] for record in records],
            "replacementNames": replacements,
        },
    )


def inspect_host_logs(
    inputs: Sequence[Path],
    output_path: Path,
    expected_created: int,
    expected_joined: int,
) -> None:
    counts = {"created": 0, "joined": 0}
    for path in inputs:
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            try:
                line = json.loads(raw_line)
            except json.JSONDecodeError:
                continue
            if (
                isinstance(line, Mapping)
                and line.get("type") == "prometa.runtime.host"
                and line.get("status") == "ready"
                and line.get("activation") in counts
            ):
                counts[str(line["activation"])] += 1
    if counts != {"created": expected_created, "joined": expected_joined}:
        raise ValueError("runtime_activation_join_evidence_invalid")
    _write_json(output_path, counts)


def _strict_json_bytes(payload: bytes) -> Any:
    if len(payload) > 1024 * 1024:
        raise ValueError("platform_receipt_response_too_large")

    def strict_object(pairs: Sequence[Tuple[str, Any]]) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("platform_receipt_response_invalid")
            result[key] = value
        return result

    try:
        return json.loads(payload.decode("utf-8"), object_pairs_hook=strict_object)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError("platform_receipt_response_invalid") from None


def _runtime_contract_digests(bundle: Any) -> Tuple[str, str]:
    if not isinstance(bundle, Mapping) or not isinstance(
        bundle.get("signedPayload"), str
    ):
        raise ValueError("platform_receipt_runtime_contract_invalid")
    try:
        claims = json.loads(str(bundle["signedPayload"]))
        content_canonical = claims.get("contentCanonical")
        content = json.loads(content_canonical)
    except (AttributeError, TypeError, json.JSONDecodeError):
        raise ValueError("platform_receipt_runtime_contract_invalid") from None
    contract = (
        content.get("runtimeContract") if isinstance(content, Mapping) else None
    )
    artifact_digest = (
        "sha256:"
        + hashlib.sha256(str(content_canonical).encode("utf-8")).hexdigest()
    )
    if (
        not isinstance(content, Mapping)
        or content_canonical != _canonical(content)
        or content != bundle.get("content")
        or not isinstance(claims, Mapping)
        or claims.get("artifactDigest") != artifact_digest
        or bundle.get("artifactDigest") != artifact_digest
        or content.get("schemaVersion") != RUNTIME_BUNDLE_SCHEMA_VERSION
        or not isinstance(contract, Mapping)
        or contract.get("contractVersion") != RUNTIME_CONTRACT_VERSION
        or not isinstance(contract.get("capabilityRequirements"), list)
        or not isinstance(contract.get("secretReferences"), list)
    ):
        raise ValueError("platform_receipt_runtime_contract_invalid")
    digests = (contract.get("policyDigest"), contract.get("configurationDigest"))
    for value in digests:
        if (
            not isinstance(value, str)
            or len(value) != 71
            or not value.startswith("sha256:")
            or any(character not in "0123456789abcdef" for character in value[7:])
        ):
            raise ValueError("platform_receipt_runtime_contract_invalid")
    expected = _runtime_projection_digests(
        content, contract.get("inputSchema"), contract.get("outputSchema")
    )
    if digests != expected:
        raise ValueError("platform_receipt_runtime_contract_invalid")
    return str(digests[0]), str(digests[1])


def _receipt_fixture(path: Path) -> Tuple[Mapping[str, Any], ...]:
    document = _read_json(path)
    tenants = document.get("tenants") if isinstance(document, Mapping) else None
    if (
        not isinstance(document, Mapping)
        or document.get("contractVersion") != 1
        or document.get("purpose") != "ephemeral-runtime-receipt-topology-certification"
        or not isinstance(tenants, list)
        or len(tenants) != len(TENANTS)
    ):
        raise ValueError("platform_receipt_fixture_invalid")
    result = []
    for expected_tenant, tenant in zip(TENANTS, tenants):
        if not isinstance(tenant, Mapping) or tenant.get("tenant") != expected_tenant:
            raise ValueError("platform_receipt_fixture_invalid")
        required = (
            "orgId",
            "runtimeId",
            "runtimeVersion",
            "releaseId",
            "deploymentId",
            "writeApiKey",
            "readApiKey",
        )
        if any(
            not isinstance(tenant.get(key), str) or not tenant[key] for key in required
        ):
            raise ValueError("platform_receipt_fixture_invalid")
        attestation = tenant.get("promotionAttestation")
        bundle = tenant.get("bundle")
        if not isinstance(attestation, Mapping) or not isinstance(bundle, Mapping):
            raise ValueError("platform_receipt_fixture_invalid")
        authorization = attestation.get("authorization")
        if (
            not isinstance(authorization, Mapping)
            or authorization.get("releaseId") != tenant["releaseId"]
            or authorization.get("deploymentId") != tenant["deploymentId"]
            or bundle.get("artifactDigest") != authorization.get("artifactDigest")
        ):
            raise ValueError("platform_receipt_fixture_invalid")
        _runtime_contract_digests(bundle)
        result.append(tenant)
    return tuple(result)


def _platform_request(
    base_url: str,
    path: str,
    api_key: str,
    *,
    method: str = "GET",
    body: Optional[Mapping[str, Any]] = None,
) -> Tuple[int, Any]:
    payload = None if body is None else _canonical(body).encode("utf-8")
    headers = {"accept": "application/json", "x-api-key": api_key}
    if payload is not None:
        headers["content-type"] = "application/json"
    url = base_url.rstrip("/") + path
    request = Request(
        url,
        data=payload,
        headers=headers,
        method=method,
    )
    try:
        with urlopen(request, timeout=5) as response:
            if response.geturl() != url:
                raise ValueError("platform_receipt_api_redirected")
            return int(response.status), _strict_json_bytes(
                response.read(1024 * 1024 + 1)
            )
    except HTTPError as error:
        return int(error.code), _strict_json_bytes(error.read(1024 * 1024 + 1))
    except (OSError, URLError):
        raise ValueError("platform_receipt_api_unavailable") from None


def _validate_platform_projection(document: Any, tenant: Mapping[str, Any]) -> None:
    if not isinstance(document, Mapping):
        raise ValueError("platform_receipt_projection_invalid")
    receipts = document.get("receipts")
    projection = document.get("projection")
    if (
        document.get("count") != 2
        or document.get("totalCount") != 2
        or not isinstance(receipts, list)
        or len(receipts) != 2
        or not isinstance(projection, Mapping)
    ):
        raise ValueError("platform_receipt_projection_pending")
    authorization = tenant["promotionAttestation"]["authorization"]
    attestation_id = tenant["promotionAttestation"].get("attestationId")
    policy_digest, configuration_digest = _runtime_contract_digests(tenant["bundle"])
    transitions = set()
    for receipt in receipts:
        payload = receipt.get("payload") if isinstance(receipt, Mapping) else None
        if (
            not isinstance(payload, Mapping)
            or payload.get("attestationId") != attestation_id
            or payload.get("artifactDigest") != authorization["artifactDigest"]
            or payload.get("releaseId") != tenant["releaseId"]
            or payload.get("deploymentId") != tenant["deploymentId"]
            or payload.get("runtimeId") != tenant["runtimeId"]
            or payload.get("runtimeVersion") != tenant["runtimeVersion"]
            or payload.get("runtimeTarget") != "tenant-runtime"
            or payload.get("policyDigest") != policy_digest
            or payload.get("configurationDigest") != configuration_digest
        ):
            raise ValueError("platform_receipt_projection_invalid")
        transitions.add(payload.get("transition"))
    milestones = projection.get("milestones")
    latest = projection.get("latestAssertion")
    if (
        transitions != {"admitted", "active"}
        or projection.get("source") != "authenticated_tenant_runtime_receipts"
        or projection.get("authority") != "tenant_runtime_assertion"
        or projection.get("completeness") != "complete"
        or projection.get("receiptCount") != 2
        or projection.get("attestationCount") != 1
        or projection.get("deploymentId") != tenant["deploymentId"]
        or projection.get("releaseId") != tenant["releaseId"]
        or projection.get("runtimeTarget") != "tenant-runtime"
        or projection.get("warningCodes") != []
        or projection.get("outOfOrderCount") != 0
        or not isinstance(milestones, Mapping)
        or milestones.get("admitted") is not True
        or milestones.get("active") is not True
        or not isinstance(latest, Mapping)
        or latest.get("transition") != "active"
        or latest.get("outcome") != "succeeded"
    ):
        raise ValueError("platform_receipt_projection_invalid")


def verify_platform_receipts(
    fixture_path: Path,
    base_url: str,
    output_path: Path,
    timeout_seconds: int = 90,
) -> None:
    parsed = urlsplit(base_url)
    if (
        parsed.scheme not in ("http", "https")
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in ("", "/")
        or parsed.query
        or parsed.fragment
        or timeout_seconds < 1
        or timeout_seconds > 300
    ):
        raise ValueError("platform_receipt_verify_config_invalid")
    tenants = _receipt_fixture(fixture_path)
    documents: Dict[str, Mapping[str, Any]] = {}
    for tenant in tenants:
        deadline = time.monotonic() + timeout_seconds
        path = "/api/runtime-receipts?" + urlencode(
            {
                "deploymentId": tenant["deploymentId"],
                "releaseId": tenant["releaseId"],
            }
        )
        while True:
            status, document = _platform_request(base_url, path, tenant["readApiKey"])
            try:
                if status != 200:
                    raise ValueError("platform_receipt_projection_invalid")
                _validate_platform_projection(document, tenant)
                documents[str(tenant["tenant"])] = document
                break
            except ValueError:
                if time.monotonic() >= deadline:
                    raise ValueError("platform_receipt_projection_timeout") from None
                time.sleep(1)

    first, second = tenants
    cross_path = "/api/runtime-receipts?" + urlencode(
        {
            "deploymentId": second["deploymentId"],
            "releaseId": second["releaseId"],
        }
    )
    status, cross_read = _platform_request(base_url, cross_path, first["readApiKey"])
    if (
        status != 200
        or not isinstance(cross_read, Mapping)
        or cross_read.get("count") != 0
        or cross_read.get("totalCount") != 0
        or cross_read.get("projection") is not None
    ):
        raise ValueError("platform_receipt_read_isolation_failed")

    own_payload = dict(documents[str(first["tenant"])]["receipts"][0]["payload"])
    own_payload["receiptId"] = "topology-invalid-binding-a"
    own_payload["deploymentId"] = second["deploymentId"]
    status, binding_error = _platform_request(
        base_url,
        "/api/runtime-receipts",
        first["writeApiKey"],
        method="POST",
        body=own_payload,
    )
    if (
        status != 409
        or not isinstance(binding_error, Mapping)
        or binding_error.get("code") != "attestation_binding_mismatch"
    ):
        raise ValueError("platform_receipt_binding_validation_failed")

    foreign_payload = dict(documents[str(second["tenant"])]["receipts"][0]["payload"])
    foreign_payload["receiptId"] = "topology-cross-tenant-write-a"
    status, isolation_error = _platform_request(
        base_url,
        "/api/runtime-receipts",
        first["writeApiKey"],
        method="POST",
        body=foreign_payload,
    )
    if (
        status != 404
        or not isinstance(isolation_error, Mapping)
        or isolation_error.get("code") != "attestation_not_found"
    ):
        raise ValueError("platform_receipt_write_isolation_failed")

    _write_json(
        output_path,
        {
            "contractVersion": 1,
            "mode": "live-platform",
            "runtimeContractVersion": RUNTIME_CONTRACT_VERSION,
            "runtimeReceiptsPerTenant": 2,
            "asynchronousReceiptDelivery": True,
            "policyConfigurationDigestBinding": True,
            "platformProjectionDigestBinding": True,
            "platformBindingValidation": True,
            "platformProjectionVisible": True,
            "receiptReadTenantIsolation": True,
            "receiptWriteTenantIsolation": True,
        },
    )


def write_report(
    profile_path: Path,
    output_path: Path,
    kubernetes_version: str,
    activation_count_a: int,
    activation_count_b: int,
    node_count_a: int,
    node_count_b: int,
    receipt_proof_path: Optional[Path] = None,
    receipt_outbox_delivered_a: Optional[int] = None,
    receipt_outbox_delivered_b: Optional[int] = None,
    mcp_audit_count_a: Optional[int] = None,
    mcp_audit_count_b: Optional[int] = None,
    mcp_indeterminate_count_a: Optional[int] = None,
    mcp_indeterminate_count_b: Optional[int] = None,
    artifact_mode: str = "source-build",
    runtime_image_reference: Optional[str] = None,
    chart_artifact_sha256: Optional[str] = None,
    chart_oci_reference: Optional[str] = None,
    release_tag: Optional[str] = None,
    release_revision: Optional[str] = None,
) -> None:
    profile = _load_profile(profile_path)
    expected_replicas = int(profile["runtimeReplicasPerTenant"])
    if (
        not kubernetes_version.startswith("v")
        or activation_count_a != 1
        or activation_count_b != 1
        or node_count_a != expected_replicas
        or node_count_b != expected_replicas
    ):
        raise ValueError("topology_observation_invalid")
    if artifact_mode == "source-build":
        if any(
            value is not None
            for value in (
                runtime_image_reference,
                chart_artifact_sha256,
                chart_oci_reference,
                release_tag,
                release_revision,
            )
        ):
            raise ValueError("topology_artifact_evidence_invalid")
        artifact_source = {"mode": "source-build"}
    elif artifact_mode == "published-release":
        if (
            runtime_image_reference is None
            or chart_artifact_sha256 is None
            or chart_oci_reference is None
            or release_tag is None
            or release_revision is None
        ):
            raise ValueError("topology_artifact_evidence_invalid")
        runtime_values = _runtime_image_values(runtime_image_reference)
        if (
            "digest" not in runtime_values
            or not _SHA256_PATTERN.fullmatch(chart_artifact_sha256)
            or "@" not in chart_oci_reference
            or not _SHA256_PATTERN.fullmatch(chart_oci_reference.rsplit("@", 1)[-1])
            or not _RELEASE_TAG_PATTERN.fullmatch(release_tag)
            or not _REVISION_PATTERN.fullmatch(release_revision)
        ):
            raise ValueError("topology_artifact_evidence_invalid")
        artifact_source = {
            "mode": "published-release",
            "releaseTag": release_tag,
            "releaseRevision": release_revision,
            "runtimeImage": runtime_image_reference,
            "chartPackageSha256": chart_artifact_sha256,
            "chartOciReference": chart_oci_reference,
        }
    else:
        raise ValueError("topology_artifact_evidence_invalid")
    report = {
        "contractVersion": 1,
        "profile": profile["name"],
        "workload": profile["workload"],
        "evidenceStatus": profile["evidenceStatus"],
        "passed": True,
        "kubernetesVersion": kubernetes_version,
        "k3dVersion": profile["k3dVersion"],
        "k3sImageDigest": profile["k3sImageDigest"],
        "postgresImageDigest": profile["postgresImageDigest"],
        "networkPolicyController": profile["networkPolicyController"],
        "runtimeVersion": profile["runtimeVersion"],
        "chartVersion": profile["chartVersion"],
        "artifactSource": artifact_source,
        "bundleSchemaVersion": RUNTIME_BUNDLE_SCHEMA_VERSION,
        "runtimeContractVersion": RUNTIME_CONTRACT_VERSION,
        "capabilityRangeAdmission": True,
        "policyConfigurationDigestBinding": True,
        "typedSecretReferenceFieldPresent": True,
        "tenantCount": profile["tenantCount"],
        "runtimeReplicasPerTenant": expected_replicas,
        "runtimeNodesPerTenant": expected_replicas,
        "uniqueLoadRequestsPerTenant": profile["uniqueLoadRequestsPerTenant"],
        "duplicateAttemptsPerTenant": profile["duplicateAttemptsPerTenant"],
        "duplicateWinnersPerTenant": 1,
        "activationRowsPerTenant": 1,
        "authorizedIngress": True,
        "podLabelIngressIsolation": True,
        "crossTenantIngressIsolation": True,
        "crossTenantEgressIsolation": True,
        "databasePartitionDeniedBeforeModel": True,
        "databasePartitionRecovery": True,
        "podReplacementJoinedActivation": True,
        "synchronousControlPlaneCalls": 0,
    }
    if profile["workload"] == "model-only":
        report["taskStatusSurvivedPodReplacement"] = True
    else:
        audit_counts = (mcp_audit_count_a, mcp_audit_count_b)
        indeterminate_counts = (
            mcp_indeterminate_count_a,
            mcp_indeterminate_count_b,
        )
        if (
            any(type(value) is not int or value < 1 for value in audit_counts)
            or indeterminate_counts != (1, 1)
            or receipt_proof_path is not None
        ):
            raise ValueError("mcp_topology_observation_invalid")
        report.update(
            {
                "mcpToolSideEffects": "read-only",
                "officialStreamableHttpTransport": True,
                "signedMcpReleaseBinding": True,
                "separateMcpSecretProjection": True,
                "crossReplicaMcpIdempotency": True,
                "crossTenantMcpIngressIsolation": True,
                "crossTenantMcpEgressIsolation": True,
                "mcpAuditPersistedAcrossPodReplacement": True,
                "mcpCredentialRotationRequiresRollout": True,
                "mcpIndeterminateQuarantine": True,
                "mcpAuditRows": {"tenantA": audit_counts[0], "tenantB": audit_counts[1]},
                "mcpIndeterminateRowsPerTenant": 1,
            }
        )
    if receipt_proof_path is not None:
        proof = _read_json(receipt_proof_path)
        expected = {
            "contractVersion": 1,
            "mode": "live-platform",
            "runtimeContractVersion": RUNTIME_CONTRACT_VERSION,
            "runtimeReceiptsPerTenant": 2,
            "asynchronousReceiptDelivery": True,
            "policyConfigurationDigestBinding": True,
            "platformProjectionDigestBinding": True,
            "platformBindingValidation": True,
            "platformProjectionVisible": True,
            "receiptReadTenantIsolation": True,
            "receiptWriteTenantIsolation": True,
        }
        if (
            proof != expected
            or receipt_outbox_delivered_a != 2
            or receipt_outbox_delivered_b != 2
        ):
            raise ValueError("platform_receipt_proof_invalid")
        report.update(
            {
                "receiptEvidenceMode": "live-platform",
                "runtimeReceiptsPerTenant": 2,
                "receiptOutboxDeliveredPerTenant": 2,
                "asynchronousReceiptDelivery": True,
                "platformProjectionDigestBinding": True,
                "platformBindingValidation": True,
                "platformProjectionVisible": True,
                "receiptReadTenantIsolation": True,
                "receiptWriteTenantIsolation": True,
            }
        )
    _write_json(output_path, report)


def _json_scalar(path: Path, key: str) -> str:
    document = _read_json(path)
    if not isinstance(document, Mapping) or key not in document:
        raise ValueError("json_value_missing")
    value = document[key]
    if isinstance(value, bool):
        return "true" if value else "false"
    if not isinstance(value, (str, int)):
        raise ValueError("json_value_invalid")
    return str(value)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="topology_fixture.py")
    subparsers = parser.add_subparsers(dest="command", required=True)

    profile_value = subparsers.add_parser("profile-value")
    profile_value.add_argument("--profile", type=Path, required=True)
    profile_value.add_argument("--key", required=True)

    prepare_parser = subparsers.add_parser("prepare")
    prepare_parser.add_argument("--profile", type=Path, required=True)
    prepare_parser.add_argument("--output-dir", type=Path, required=True)
    prepare_parser.add_argument("--probe-source", type=Path, required=True)
    prepare_parser.add_argument("--runtime-image", required=True)
    prepare_parser.add_argument("--runtime-version", required=True)
    prepare_parser.add_argument("--receipt-base-url")
    prepare_parser.add_argument("--receipt-endpoint-cidr")
    prepare_parser.add_argument("--mcp-server-source", type=Path)

    verify_platform = subparsers.add_parser("verify-platform-receipts")
    verify_platform.add_argument("--fixture", type=Path, required=True)
    verify_platform.add_argument("--base-url", required=True)
    verify_platform.add_argument("--output", type=Path, required=True)
    verify_platform.add_argument("--timeout-seconds", type=int, default=90)

    partition_parser = subparsers.add_parser("partition-policy")
    partition_parser.add_argument("--input", type=Path, required=True)
    partition_parser.add_argument("--original-output", type=Path, required=True)
    partition_parser.add_argument("--partition-output", type=Path, required=True)

    inspect_parser = subparsers.add_parser("inspect-pods")
    inspect_parser.add_argument("--input", type=Path, required=True)
    inspect_parser.add_argument("--output", type=Path, required=True)
    inspect_parser.add_argument("--expected-replicas", type=int, required=True)
    inspect_parser.add_argument("--previous", type=Path)

    logs_parser = subparsers.add_parser("inspect-host-logs")
    logs_parser.add_argument("--input", action="append", type=Path, required=True)
    logs_parser.add_argument("--output", type=Path, required=True)
    logs_parser.add_argument("--expected-created", type=int, required=True)
    logs_parser.add_argument("--expected-joined", type=int, required=True)

    json_value = subparsers.add_parser("json-value")
    json_value.add_argument("--input", type=Path, required=True)
    json_value.add_argument("--key", required=True)

    pod_urls = subparsers.add_parser("pod-urls")
    pod_urls.add_argument("--input", type=Path, required=True)
    pod_urls.add_argument("--port", type=int, default=8080)

    pod_names = subparsers.add_parser("pod-names")
    pod_names.add_argument("--input", type=Path, required=True)
    pod_names.add_argument("--replacement-only", action="store_true")

    report_parser = subparsers.add_parser("report")
    report_parser.add_argument("--profile", type=Path, required=True)
    report_parser.add_argument("--output", type=Path, required=True)
    report_parser.add_argument("--kubernetes-version", required=True)
    report_parser.add_argument("--activation-count-a", type=int, required=True)
    report_parser.add_argument("--activation-count-b", type=int, required=True)
    report_parser.add_argument("--node-count-a", type=int, required=True)
    report_parser.add_argument("--node-count-b", type=int, required=True)
    report_parser.add_argument("--receipt-proof", type=Path)
    report_parser.add_argument("--receipt-outbox-delivered-a", type=int)
    report_parser.add_argument("--receipt-outbox-delivered-b", type=int)
    report_parser.add_argument("--mcp-audit-count-a", type=int)
    report_parser.add_argument("--mcp-audit-count-b", type=int)
    report_parser.add_argument("--mcp-indeterminate-count-a", type=int)
    report_parser.add_argument("--mcp-indeterminate-count-b", type=int)
    report_parser.add_argument(
        "--artifact-mode",
        choices=("source-build", "published-release"),
        default="source-build",
    )
    report_parser.add_argument("--runtime-image-reference")
    report_parser.add_argument("--chart-artifact-sha256")
    report_parser.add_argument("--chart-oci-reference")
    report_parser.add_argument("--release-tag")
    report_parser.add_argument("--release-revision")

    args = parser.parse_args(argv)
    if args.command == "profile-value":
        profile = _load_profile(args.profile)
        value = profile.get(args.key)
        if not isinstance(value, (str, int)) or isinstance(value, bool):
            raise ValueError("topology_profile_value_invalid")
        print(value)
    elif args.command == "prepare":
        prepare(
            args.profile,
            args.output_dir,
            args.probe_source,
            args.runtime_image,
            args.runtime_version,
            args.receipt_base_url,
            args.receipt_endpoint_cidr,
            args.mcp_server_source,
        )
    elif args.command == "verify-platform-receipts":
        verify_platform_receipts(
            args.fixture,
            args.base_url,
            args.output,
            args.timeout_seconds,
        )
    elif args.command == "partition-policy":
        write_partition_policies(
            args.input,
            args.original_output,
            args.partition_output,
        )
    elif args.command == "inspect-pods":
        inspect_pods(
            args.input,
            args.output,
            args.expected_replicas,
            args.previous,
        )
    elif args.command == "inspect-host-logs":
        inspect_host_logs(
            args.input,
            args.output,
            args.expected_created,
            args.expected_joined,
        )
    elif args.command == "json-value":
        print(_json_scalar(args.input, args.key))
    elif args.command == "pod-urls":
        document = _read_json(args.input)
        print(
            ",".join(
                "http://%s:%d" % (pod_ip, args.port)
                for pod_ip in document.get("podIps", [])
            )
        )
    elif args.command == "pod-names":
        document = _read_json(args.input)
        key = "replacementNames" if args.replacement_only else "podNames"
        print(" ".join(document.get(key, [])))
    elif args.command == "report":
        write_report(
            args.profile,
            args.output,
            args.kubernetes_version,
            args.activation_count_a,
            args.activation_count_b,
            args.node_count_a,
            args.node_count_b,
            args.receipt_proof,
            args.receipt_outbox_delivered_a,
            args.receipt_outbox_delivered_b,
            args.mcp_audit_count_a,
            args.mcp_audit_count_b,
            args.mcp_indeterminate_count_a,
            args.mcp_indeterminate_count_b,
            args.artifact_mode,
            args.runtime_image_reference,
            args.chart_artifact_sha256,
            args.chart_oci_reference,
            args.release_tag,
            args.release_revision,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
