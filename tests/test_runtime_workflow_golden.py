from __future__ import annotations

import copy
import json
from pathlib import Path

from prometa.runtime.workflow_ontology import (
    evaluate_workflow_policy,
    parse_workflow_ontology_artifact,
)


_FIXTURE = json.loads(
    (Path(__file__).parent / "fixtures" / "workflow-policy-golden-v1.json").read_text(
        encoding="utf-8"
    )
)


def test_typescript_and_python_workflow_policy_golden_vectors_are_identical():
    assert _FIXTURE["schema"] == "prometa.workflow-policy-golden.v1"
    artifact = parse_workflow_ontology_artifact(_FIXTURE["artifact"])
    assert artifact.ontology_digest == _FIXTURE["artifact"]["ontologyDigest"]
    assert artifact.policy_digest == _FIXTURE["artifact"]["policyDigest"]
    assert (
        artifact.sector_snapshot_digest == _FIXTURE["artifact"]["sectorSnapshotDigest"]
    )

    for vector in _FIXTURE["vectors"]:
        runtime_input = copy.deepcopy(_FIXTURE["baseInput"])
        runtime_input.update(copy.deepcopy(vector["patch"]))
        assert (
            evaluate_workflow_policy(
                artifact.compiled_policy,
                runtime_input,
            ).as_dict()
            == vector["expected"]
        ), vector["id"]
