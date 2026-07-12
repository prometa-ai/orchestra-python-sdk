"""Generate and inspect ephemeral tenant-runtime topology certification data."""

from __future__ import annotations

import argparse
import base64
import copy
import hashlib
import json
import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple
from urllib.parse import quote


PROFILE_NAME = "k3d-k3s-kube-router-v1"
TENANTS = ("a", "b")


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
        or profiles[0].get("name") != PROFILE_NAME
    ):
        raise ValueError("topology_profile_invalid")
    profile = profiles[0]
    required_strings = (
        "evidenceStatus",
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


def _runtime_content(tenant: str) -> Mapping[str, Any]:
    primary = {
        "name": "Primary",
        "provider": "inference-engine",
        "modelName": "golden-model",
        "role": "primary",
        "temperature": 0.0,
        "maxOutputTokens": 128,
        "structuredOutput": True,
    }
    return {
        "schemaVersion": 1,
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
        "systemPrompt": "Return the isolated tenant identifier.",
        "models": [primary],
        "primaryModel": primary,
        "topology": {"pattern": "single-react", "maxIterations": 1},
        "tools": [],
        "skills": [],
        "knowledge": [],
        "memory": [],
        "subAgents": [],
        "workflows": [],
        "guardrails": [],
        "identity": None,
        "triggers": [],
        "evaluation": [],
        "mcpServers": [],
        "requiredScopes": [],
        "grantedScopes": [],
        "readiness": {
            "quality": 100,
            "security": 100,
            "maturity": 80,
            "productivity": 60,
        },
        "runtimeContract": {
            "contractVersion": 1,
            "requiredCapabilities": [
                "evidence.emit.v1",
                "model.invoke.v1",
                "schema.validate.v1",
            ],
            "inputSchema": {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {
                    "question": {"type": "string", "minLength": 1}
                },
                "required": ["question"],
                "additionalProperties": False,
            },
            "outputSchema": {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {"answer": {"type": "string", "minLength": 1}},
                "required": ["answer"],
                "additionalProperties": False,
            },
        },
    }


def _signed_release(tenant: str, now: datetime) -> Mapping[str, Any]:
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    bundle_key = Ed25519PrivateKey.generate()
    promotion_key = Ed25519PrivateKey.generate()
    org_id = "org-topology-%s" % tenant
    content = _runtime_content(tenant)
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
) -> Mapping[str, Any]:
    return {
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
        "taskRecovery": {
            "leaseSeconds": 15,
            "maxAttempts": 3,
            "historyLimit": 50,
        },
    }


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
    tenant: str, image: str, source: str
) -> Tuple[Mapping[str, Any], ...]:
    namespace = "models-%s" % tenant
    labels = {"app.kubernetes.io/name": "model-gateway"}
    container = {
        "name": "model-gateway",
        "image": image,
        "imagePullPolicy": "Never",
        "command": ["python", "/opt/topology/topology_probe.py"],
        "args": ["model-gateway", "--tenant", "tenant-%s" % tenant],
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
) -> Mapping[str, Any]:
    repository, tag = runtime_image.rsplit(":", 1)
    runtime_namespace = "runtime-%s" % tenant
    return {
        "fullnameOverride": "runtime",
        "replicaCount": replicas,
        "image": {
            "repository": repository,
            "tag": tag,
            "pullPolicy": "Never",
        },
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
            "topology.prometa.io/profile": PROFILE_NAME,
            "topology.prometa.io/runtime-namespace": runtime_namespace,
        },
    }


def prepare(
    profile_path: Path,
    output_dir: Path,
    probe_source_path: Path,
    runtime_image: str,
    runtime_version: str,
) -> None:
    profile = _load_profile(profile_path)
    if ":" not in runtime_image or len(runtime_image) > 256:
        raise ValueError("runtime_image_invalid")
    if runtime_version != profile["runtimeVersion"]:
        raise ValueError("runtime_version_mismatch")
    source = probe_source_path.read_text(encoding="utf-8")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_dir.chmod(0o700)
    now = datetime.now(timezone.utc).replace(microsecond=0)
    resources = []
    for tenant in TENANTS:
        for prefix, policy in (
            ("runtime", "restricted"),
            ("gateway", "restricted"),
            ("models", "restricted"),
            ("data", "baseline"),
        ):
            resources.append(_namespace("%s-%s" % (prefix, tenant), tenant, policy))
        release = _signed_release(tenant, now)
        api_token = secrets.token_urlsafe(36)
        postgres_password = secrets.token_urlsafe(30)
        dsn = (
            "postgresql://runtime:%s@postgres.data-%s.svc.cluster.local:5432/"
            "runtime?connect_timeout=2"
        ) % (quote(postgres_password, safe=""), tenant)
        _write_json(
            output_dir / ("tenant-%s-config.json" % tenant),
            _runtime_config(tenant, release, runtime_version),
            private=True,
        )
        credentials = (
            "database-url=%s\n"
            "migration-database-url=%s\n"
            "api-token=%s\n"
        ) % (dsn, dsn, api_token)
        credentials_path = output_dir / ("tenant-%s-credentials.env" % tenant)
        credentials_path.write_text(credentials, encoding="utf-8")
        credentials_path.chmod(0o600)
        _write_json(
            output_dir / ("tenant-%s-values.json" % tenant),
            _chart_values(
                tenant,
                runtime_image,
                int(profile["runtimeReplicasPerTenant"]),
            ),
        )
        resources.append(_config_map("runtime-%s" % tenant, source))
        resources.extend(
            _postgres_resources(
                tenant,
                str(profile["postgresNodeImage"]),
                postgres_password,
            )
        )
        resources.extend(_model_resources(tenant, runtime_image, source))
        resources.extend(_gateway_resources(tenant, runtime_image, source, api_token))
    _write_json(
        output_dir / "support-resources.json",
        {"apiVersion": "v1", "kind": "List", "items": resources},
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


def write_report(
    profile_path: Path,
    output_path: Path,
    kubernetes_version: str,
    activation_count_a: int,
    activation_count_b: int,
    node_count_a: int,
    node_count_b: int,
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
    report = {
        "contractVersion": 1,
        "profile": profile["name"],
        "evidenceStatus": profile["evidenceStatus"],
        "passed": True,
        "kubernetesVersion": kubernetes_version,
        "k3dVersion": profile["k3dVersion"],
        "k3sImageDigest": profile["k3sImageDigest"],
        "postgresImageDigest": profile["postgresImageDigest"],
        "networkPolicyController": profile["networkPolicyController"],
        "runtimeVersion": profile["runtimeVersion"],
        "chartVersion": profile["chartVersion"],
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
        "taskStatusSurvivedPodReplacement": True,
        "synchronousControlPlaneCalls": 0,
    }
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
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
