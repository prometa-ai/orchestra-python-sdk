from __future__ import annotations

import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip("jsonschema")

from prometa.runtime import (
    BASE_RUNTIME_CAPABILITIES,
    CAPABILITY_SCHEMA_VALIDATE,
    CAPABILITY_WORKFLOW_CONTEXT_RESOLVE,
    CAPABILITY_WORKFLOW_DECISION_EMIT,
    CAPABILITY_WORKFLOW_POLICY_EVALUATE,
    CAPABILITY_WORKFLOW_STATE_PERSIST,
    BundleVerificationError,
    parse_runtime_bundle,
)
from prometa.runtime.workflow_ontology import canonical_digest
from tests.test_runtime_workflow_ontology import _artifact


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "runtime-kernel-v2.json"
WORKFLOW_CAPABILITIES = {
    CAPABILITY_WORKFLOW_POLICY_EVALUATE,
    CAPABILITY_WORKFLOW_CONTEXT_RESOLVE,
    CAPABILITY_WORKFLOW_STATE_PERSIST,
    CAPABILITY_WORKFLOW_DECISION_EMIT,
}


def _content_v2():
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))["bundle"]["content"]


def _selected(value, keys):
    return {key: value[key] for key in keys if key in value}


def _refresh_v3_digests(content):
    contract = content["runtimeContract"]
    tools = content["tools"]
    contract["policyDigest"] = canonical_digest(
        {
            "guardrails": content.get("guardrails", []),
            "identity": content.get("identity"),
            "tools": [
                _selected(
                    tool,
                    (
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
                    ),
                )
                for tool in tools
            ],
            "requiredScopes": content.get("requiredScopes", []),
            "grantedScopes": content.get("grantedScopes", []),
            "workflowOntologies": content["workflowOntologies"],
        }
    )
    contract["configurationDigest"] = canonical_digest(
        {
            "manifest": content.get("manifest"),
            "systemPrompt": content.get("systemPrompt"),
            "models": content.get("models"),
            "primaryModel": content.get("primaryModel"),
            "topology": content.get("topology"),
            "tools": [
                _selected(
                    tool,
                    (
                        "name",
                        "source",
                        "mcpServer",
                        "operation",
                        "inputSchema",
                        "rateLimitPerMin",
                    ),
                )
                for tool in tools
            ],
            "skills": content.get("skills", []),
            "knowledge": content.get("knowledge", []),
            "memory": content.get("memory", []),
            "subAgents": content.get("subAgents", []),
            "workflows": content.get("workflows", []),
            "triggers": content.get("triggers", []),
            "evaluation": content.get("evaluation", []),
            "inputSchema": contract.get("inputSchema"),
            "outputSchema": contract.get("outputSchema"),
            "mcpServers": content.get("mcpServers", []),
            "workflowOntologies": content["workflowOntologies"],
        }
    )


def _content_v3():
    content = copy.deepcopy(_content_v2())
    content["schemaVersion"] = 3
    content["workflowOntologies"] = [_artifact()]
    artifact = content["workflowOntologies"][0]
    content["workflows"] = [
        {
            "name": "Invoice reference",
            "config": {
                "workflowType": "company-process",
                "ontologyId": artifact["ontologyId"],
                "ontologyVersion": artifact["version"],
                "ontologyDigest": artifact["ontologyDigest"],
                "mode": artifact["mode"],
            },
        }
    ]
    contract = content["runtimeContract"]
    contract["contractVersion"] = 3
    contract["requiredCapabilities"] = sorted(
        {*contract["requiredCapabilities"], *WORKFLOW_CAPABILITIES}
    )
    contract["capabilityRequirements"] = sorted(
        [
            *contract["capabilityRequirements"],
            *[
                {
                    "name": capability.rsplit(".v", 1)[0],
                    "minVersion": 1,
                    "maxVersion": 1,
                }
                for capability in WORKFLOW_CAPABILITIES
            ],
        ],
        key=lambda value: value["name"],
    )
    _refresh_v3_digests(content)
    return content


def _parse(content, supported=None):
    return parse_runtime_bundle(
        SimpleNamespace(content=content),
        supported_capabilities=supported
        or {
            *BASE_RUNTIME_CAPABILITIES,
            CAPABILITY_SCHEMA_VALIDATE,
            *WORKFLOW_CAPABILITIES,
        },
    )


def _assert_code(code, callback):
    with pytest.raises(BundleVerificationError) as caught:
        callback()
    assert caught.value.code == code


def test_v3_admits_exact_workflow_artifact_and_capability_ranges():
    config = _parse(_content_v3())
    assert config.contract.contract_version == 3
    assert config.workflow_ontologies[0].ontology_id == "ontology-1"
    assert config.workflow_ontologies[0].version == 1
    assert config.workflow_ontologies[0].mode == "enforce"


def test_v3_fails_closed_for_missing_capability_and_digest_drift():
    content = _content_v3()
    _assert_code(
        "unsupported_runtime_capability",
        lambda: _parse(
            content,
            {
                *BASE_RUNTIME_CAPABILITIES,
                CAPABILITY_SCHEMA_VALIDATE,
                CAPABILITY_WORKFLOW_POLICY_EVALUATE,
                CAPABILITY_WORKFLOW_CONTEXT_RESOLVE,
                CAPABILITY_WORKFLOW_STATE_PERSIST,
            },
        ),
    )
    drifted = copy.deepcopy(content)
    drifted["workflowOntologies"][0]["mode"] = "observe"
    _assert_code("company_workflow_binding_mismatch", lambda: _parse(drifted))

    mismatched = _content_v3()
    mismatched["workflows"][0]["config"]["ontologyDigest"] = "sha256:" + "0" * 64
    _refresh_v3_digests(mismatched)
    _assert_code("company_workflow_binding_mismatch", lambda: _parse(mismatched))


def test_workflow_artifacts_are_v3_only_and_v3_requires_one():
    v2 = _content_v2()
    v2["workflowOntologies"] = []
    _assert_code(
        "workflow_ontology_requires_runtime_v3",
        lambda: _parse(
            v2,
            {*BASE_RUNTIME_CAPABILITIES, CAPABILITY_SCHEMA_VALIDATE},
        ),
    )

    v3 = _content_v3()
    v3["workflowOntologies"] = []
    _refresh_v3_digests(v3)
    _assert_code("runtime_v3_workflow_ontology_missing", lambda: _parse(v3))
