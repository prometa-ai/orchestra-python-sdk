"""Local company-workflow policy parsing and deterministic evaluation.

The runtime consumes only immutable, signed bundle artifacts. It resolves no
ontology or authorization data from Orchestra during execution.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from math import isfinite
from typing import Any, Mapping, Optional, Protocol, Sequence, Tuple


_DIGEST_PATTERN = "sha256:"
_ARTIFACT_KEYS = frozenset(
    {
        "ontologyId",
        "version",
        "mode",
        "ontologyDigest",
        "policyDigest",
        "sectorSnapshotDigest",
        "compiledPolicy",
    }
)
_COMPILED_KEYS = frozenset(
    {
        "schemaVersion",
        "ontologyDigest",
        "policyDigest",
        "sectorSnapshotDigest",
        "spec",
        "stateIds",
        "terminalStateIds",
        "transitionIdsByStateAndTask",
    }
)
_SPEC_KEYS = frozenset(
    {
        "schemaVersion",
        "name",
        "description",
        "sectorBinding",
        "allowedConditionPaths",
        "roles",
        "businessObjects",
        "states",
        "tasks",
        "transitions",
        "facts",
        "evidenceRequirements",
        "obligations",
        "controls",
    }
)
_SECTOR_BINDING_KEYS = frozenset({"sector", "snapshot", "snapshotDigest"})


class WorkflowOntologyError(RuntimeError):
    """Stable fail-closed workflow policy error."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code.replace("_", " "))


@dataclass(frozen=True)
class RuntimeWorkflowOntology:
    ontology_id: str
    version: int
    mode: str
    ontology_digest: str
    policy_digest: str
    sector_snapshot_digest: str
    compiled_policy: Mapping[str, Any]


@dataclass(frozen=True)
class WorkflowPolicyDecision:
    recommended_outcome: str
    applied_outcome: str
    reason_codes: Tuple[str, ...]
    matched_control_ids: Tuple[str, ...]
    missing_fact_ids: Tuple[str, ...]
    stale_fact_ids: Tuple[str, ...]
    obligation_ids: Tuple[str, ...]
    evidence_requirement_ids: Tuple[str, ...]
    proposed_transition_id: Optional[str]
    proposed_state: Optional[str]
    counterfactual_reason_codes: Tuple[str, ...]

    def as_dict(self) -> Mapping[str, Any]:
        value = {
            "recommendedOutcome": self.recommended_outcome,
            "appliedOutcome": self.applied_outcome,
            "reasonCodes": list(self.reason_codes),
            "matchedControlIds": list(self.matched_control_ids),
            "missingFactIds": list(self.missing_fact_ids),
            "staleFactIds": list(self.stale_fact_ids),
            "obligationIds": list(self.obligation_ids),
            "evidenceRequirementIds": list(self.evidence_requirement_ids),
            "counterfactualReasonCodes": list(self.counterfactual_reason_codes),
        }
        if self.proposed_transition_id is not None:
            value["proposedTransitionId"] = self.proposed_transition_id
        if self.proposed_state is not None:
            value["proposedState"] = self.proposed_state
        return value


@dataclass(frozen=True)
class WorkflowExecutionContext:
    ontology_id: str
    version: int
    instance_id: str
    actor_ref: str


@dataclass(frozen=True)
class VerifiedWorkflowContext:
    current_state: str
    state_version: int
    actor_role_ids: Tuple[str, ...]
    purpose: str
    facts: Mapping[str, Mapping[str, Any]]
    approvals: Tuple[Mapping[str, Any], ...] = ()
    evidence_references: Tuple[str, ...] = ()
    used_idempotency_keys: Tuple[str, ...] = ()


@dataclass(frozen=True)
class WorkflowContextRequest:
    request_id: str
    workflow: WorkflowExecutionContext
    task_id: str
    transition_id: Optional[str]
    request_attributes: Mapping[str, Any]


class WorkflowContextResolver(Protocol):
    async def resolve(self, request: WorkflowContextRequest) -> VerifiedWorkflowContext:
        """Resolve verified roles, purpose, state and authoritative facts."""


@dataclass(frozen=True)
class WorkflowPostconditionRequest:
    context_request: WorkflowContextRequest
    prior_context: VerifiedWorkflowContext
    proposed_state: str
    tool_audit_reference: Optional[str]
    tool_result: Any


class WorkflowPostconditionValidator(Protocol):
    async def validate(
        self, request: WorkflowPostconditionRequest
    ) -> VerifiedWorkflowContext:
        """Validate tenant-owned result semantics and return refreshed facts."""


@dataclass(frozen=True)
class WorkflowStateCommitRequest:
    request_id: str
    workflow: WorkflowExecutionContext
    expected_state: str
    expected_version: int
    next_state: str
    transition_id: str
    ontology_digest: str
    policy_digest: str
    sector_snapshot_digest: str
    approval_references: Tuple[str, ...]
    evidence_references: Tuple[str, ...]
    idempotency_key: Optional[str]


@dataclass(frozen=True)
class WorkflowIndeterminateRequest:
    request_id: str
    workflow: WorkflowExecutionContext
    state: str
    state_version: int
    task_id: str
    transition_id: Optional[str]
    reason_code: str
    ontology_digest: str
    policy_digest: str
    sector_snapshot_digest: str


class WorkflowStateStore(Protocol):
    async def compare_and_set(self, request: WorkflowStateCommitRequest) -> bool:
        """Append and commit only when the expected state/version still match."""

    async def mark_indeterminate(self, request: WorkflowIndeterminateRequest) -> None:
        """Quarantine a possibly crossed side-effect boundary."""


@dataclass(frozen=True)
class WorkflowDecisionEvidence:
    request_id: str
    workflow_id: str
    workflow_version: int
    workflow_instance_id: str
    ontology_digest: str
    policy_digest: str
    sector_snapshot_digest: str
    state: str
    state_version: int
    task_id: str
    transition_id: Optional[str]
    recommended_outcome: str
    applied_outcome: str
    reason_codes: Tuple[str, ...]
    control_ids: Tuple[str, ...]
    obligation_ids: Tuple[str, ...]
    fact_set_digest: str
    missing_fact_ids: Tuple[str, ...]
    stale_fact_ids: Tuple[str, ...]
    approval_references: Tuple[str, ...]
    evidence_references: Tuple[str, ...]
    occurred_at: str


class WorkflowDecisionEmitter(Protocol):
    def emit(self, decision: WorkflowDecisionEvidence) -> None:
        """Buffer or persist one payload-free workflow decision."""


class InMemoryWorkflowStateStore:
    """Deterministic CAS ledger for conformance and single-process tests."""

    def __init__(self) -> None:
        self._instances = {}
        self.ledger = []

    def seed(
        self,
        workflow: WorkflowExecutionContext,
        state: str,
        version: int = 0,
    ) -> None:
        self._instances[
            (workflow.ontology_id, workflow.version, workflow.instance_id)
        ] = (state, version, False)

    async def compare_and_set(self, request: WorkflowStateCommitRequest) -> bool:
        key = (
            request.workflow.ontology_id,
            request.workflow.version,
            request.workflow.instance_id,
        )
        current = self._instances.get(key)
        if current is None:
            current = (request.expected_state, request.expected_version, False)
        if current != (request.expected_state, request.expected_version, False):
            return False
        next_version = request.expected_version + 1
        self._instances[key] = (request.next_state, next_version, False)
        self.ledger.append(request)
        return True

    async def mark_indeterminate(self, request: WorkflowIndeterminateRequest) -> None:
        key = (
            request.workflow.ontology_id,
            request.workflow.version,
            request.workflow.instance_id,
        )
        self._instances[key] = (request.state, request.state_version, True)
        self.ledger.append(request)


class InMemoryWorkflowDecisionEmitter:
    def __init__(self) -> None:
        self.decisions = []

    def emit(self, decision: WorkflowDecisionEvidence) -> None:
        self.decisions.append(decision)


def parse_workflow_execution_context(value: Any) -> WorkflowExecutionContext:
    context = _mapping(value, "invalid_workflow_context")
    _exact_keys(
        context,
        {"workflowId", "version", "instanceId", "actorRef"},
        "invalid_workflow_context",
    )
    version = context.get("version")
    if type(version) is not int or version < 1:
        raise WorkflowOntologyError("invalid_workflow_context")
    return WorkflowExecutionContext(
        ontology_id=_string(context.get("workflowId"), "invalid_workflow_context", 256),
        version=version,
        instance_id=_string(context.get("instanceId"), "invalid_workflow_context", 256),
        actor_ref=_string(context.get("actorRef"), "invalid_workflow_context", 512),
    )


def workflow_fact_set_digest(facts: Mapping[str, Mapping[str, Any]]) -> str:
    projection = {}
    for fact_id, fact in sorted(facts.items()):
        if not isinstance(fact_id, str) or not isinstance(fact, Mapping):
            raise WorkflowOntologyError("invalid_verified_workflow_context")
        projection[fact_id] = {
            "valueDigest": canonical_digest(fact.get("value")),
            "observedAt": fact.get("observedAt"),
            "authoritative": fact.get("authoritative"),
            "source": fact.get("source"),
        }
    return canonical_digest(projection)


def canonical_digest(value: Any) -> str:
    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise WorkflowOntologyError("workflow_artifact_not_canonical_json") from exc
    return _DIGEST_PATTERN + hashlib.sha256(encoded).hexdigest()


def _mapping(value: Any, code: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise WorkflowOntologyError(code)
    return value


def _sequence(value: Any, code: str, maximum: int = 512) -> Sequence[Any]:
    if not isinstance(value, list) or len(value) > maximum:
        raise WorkflowOntologyError(code)
    return value


def _exact_keys(value: Mapping[str, Any], allowed, code: str) -> None:
    if set(value) != set(allowed):
        raise WorkflowOntologyError(code)


def _string(value: Any, code: str, maximum: int = 512) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > maximum
    ):
        raise WorkflowOntologyError(code)
    return value


def _digest(value: Any, code: str) -> str:
    candidate = _string(value, code, 71)
    if (
        not candidate.startswith(_DIGEST_PATTERN)
        or len(candidate) != 71
        or any(child not in "0123456789abcdef" for child in candidate[7:])
    ):
        raise WorkflowOntologyError(code)
    return candidate


def _string_tuple(value: Any, code: str) -> Tuple[str, ...]:
    entries = _sequence(value, code)
    result = tuple(_string(entry, code, 256) for entry in entries)
    if len(set(result)) != len(result):
        raise WorkflowOntologyError(code)
    return result


def parse_workflow_ontology_artifact(value: Any) -> RuntimeWorkflowOntology:
    """Parse one exact workflow artifact and rederive every executable digest."""

    artifact = _mapping(value, "invalid_workflow_ontology_artifact")
    _exact_keys(artifact, _ARTIFACT_KEYS, "invalid_workflow_ontology_artifact")
    ontology_id = _string(
        artifact.get("ontologyId"), "invalid_workflow_ontology_artifact", 256
    )
    version = artifact.get("version")
    if type(version) is not int or version < 1:
        raise WorkflowOntologyError("invalid_workflow_ontology_artifact")
    mode = artifact.get("mode")
    if mode not in {"observe", "enforce"}:
        raise WorkflowOntologyError("invalid_workflow_ontology_artifact")

    ontology_digest = _digest(
        artifact.get("ontologyDigest"), "invalid_workflow_ontology_digest"
    )
    policy_digest = _digest(
        artifact.get("policyDigest"), "invalid_workflow_policy_digest"
    )
    sector_digest = _digest(
        artifact.get("sectorSnapshotDigest"),
        "invalid_workflow_sector_snapshot_digest",
    )
    compiled = _mapping(
        artifact.get("compiledPolicy"), "invalid_compiled_workflow_policy"
    )
    _exact_keys(compiled, _COMPILED_KEYS, "invalid_compiled_workflow_policy")
    if compiled.get("schemaVersion") != 1:
        raise WorkflowOntologyError("unsupported_workflow_policy_schema")
    spec = _mapping(compiled.get("spec"), "invalid_compiled_workflow_policy")
    _exact_keys(spec, _SPEC_KEYS, "invalid_compiled_workflow_policy")
    if spec.get("schemaVersion") != 1:
        raise WorkflowOntologyError("unsupported_workflow_policy_schema")
    states = tuple(
        _mapping(item, "invalid_compiled_workflow_policy")
        for item in _sequence(spec.get("states"), "invalid_compiled_workflow_policy")
    )
    tasks = tuple(
        _mapping(item, "invalid_compiled_workflow_policy")
        for item in _sequence(spec.get("tasks"), "invalid_compiled_workflow_policy")
    )
    transitions = tuple(
        _mapping(item, "invalid_compiled_workflow_policy")
        for item in _sequence(
            spec.get("transitions"), "invalid_compiled_workflow_policy"
        )
    )
    for key in (
        "roles",
        "businessObjects",
        "facts",
        "evidenceRequirements",
        "obligations",
        "controls",
        "allowedConditionPaths",
    ):
        _sequence(spec.get(key), "invalid_compiled_workflow_policy")
    sector_binding = _mapping(
        spec.get("sectorBinding"), "invalid_compiled_workflow_policy"
    )
    _exact_keys(
        sector_binding,
        _SECTOR_BINDING_KEYS,
        "invalid_compiled_workflow_policy",
    )
    _string(
        sector_binding.get("sector"),
        "invalid_compiled_workflow_policy",
        128,
    )
    sector_snapshot = _mapping(
        sector_binding.get("snapshot"),
        "invalid_compiled_workflow_policy",
    )
    embedded_sector_digest = _digest(
        sector_binding.get("snapshotDigest"),
        "invalid_workflow_sector_snapshot_digest",
    )

    state_ids = tuple(
        _string(state.get("id"), "invalid_compiled_workflow_policy", 128)
        for state in states
    )
    task_ids = {
        _string(task.get("id"), "invalid_compiled_workflow_policy", 128)
        for task in tasks
    }
    if len(set(state_ids)) != len(state_ids) or len(task_ids) != len(tasks):
        raise WorkflowOntologyError("invalid_compiled_workflow_policy")
    terminal_ids = tuple(
        state_id
        for state_id, state in zip(state_ids, states)
        if state.get("type") == "terminal"
    )
    expected_index = {}
    ordered_transitions = sorted(
        transitions,
        key=lambda transition: (
            transition.get("priority"),
            transition.get("id"),
        ),
    )
    for transition in ordered_transitions:
        transition_id = _string(
            transition.get("id"), "invalid_compiled_workflow_policy", 128
        )
        from_state = _string(
            transition.get("fromStateId"), "invalid_compiled_workflow_policy", 128
        )
        task_id = _string(
            transition.get("taskId"), "invalid_compiled_workflow_policy", 128
        )
        priority = transition.get("priority")
        if (
            type(priority) is not int
            or from_state not in state_ids
            or task_id not in task_ids
        ):
            raise WorkflowOntologyError("invalid_compiled_workflow_policy")
        key = "%s:%s" % (from_state, task_id)
        expected_index.setdefault(key, []).append(transition_id)

    policy_projection = {
        "allowedConditionPaths": spec.get("allowedConditionPaths"),
        "states": spec.get("states"),
        "tasks": spec.get("tasks"),
        "transitions": spec.get("transitions"),
        "facts": spec.get("facts"),
        "evidenceRequirements": spec.get("evidenceRequirements"),
        "obligations": spec.get("obligations"),
        "controls": spec.get("controls"),
    }
    if (
        canonical_digest(spec) != ontology_digest
        or canonical_digest(policy_projection) != policy_digest
        or embedded_sector_digest != sector_digest
        or canonical_digest(sector_snapshot) != sector_digest
        or compiled.get("ontologyDigest") != ontology_digest
        or compiled.get("policyDigest") != policy_digest
        or compiled.get("sectorSnapshotDigest") != sector_digest
        or _string_tuple(compiled.get("stateIds"), "invalid_compiled_workflow_policy")
        != state_ids
        or _string_tuple(
            compiled.get("terminalStateIds"), "invalid_compiled_workflow_policy"
        )
        != terminal_ids
        or compiled.get("transitionIdsByStateAndTask") != expected_index
    ):
        raise WorkflowOntologyError("workflow_policy_digest_mismatch")

    return RuntimeWorkflowOntology(
        ontology_id=ontology_id,
        version=version,
        mode=mode,
        ontology_digest=ontology_digest,
        policy_digest=policy_digest,
        sector_snapshot_digest=sector_digest,
        compiled_policy=json.loads(
            json.dumps(
                compiled,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            )
        ),
    )


def _unique(values) -> Tuple[str, ...]:
    return tuple(sorted(set(values)))


def _instant(value: Any) -> Optional[float]:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    # JavaScript Date.parse, used by the platform evaluator, has millisecond
    # precision. Truncate (do not round) so both runtimes classify boundary
    # freshness identically.
    return int(parsed.timestamp() * 1000) / 1000


def _resolve_path(path: str, runtime_input: Mapping[str, Any]):
    segments = path.split(".")
    root = segments.pop(0)
    if root in {"request", "state", "actor"}:
        current = runtime_input.get(root)
    elif root == "fact":
        facts = runtime_input.get("facts")
        if not isinstance(facts, Mapping) or not segments:
            return False, None
        fact = facts.get(segments.pop(0))
        if not isinstance(fact, Mapping):
            return False, None
        current = fact.get("value")
    else:
        return False, None
    for segment in segments:
        if not isinstance(current, Mapping) or segment not in current:
            return False, None
        current = current[segment]
    return True, current


def _evaluate_condition(condition: Mapping[str, Any], runtime_input):
    operation = condition.get("op")
    if operation == "all":
        values = [
            _evaluate_condition(child, runtime_input)
            for child in condition.get("args", [])
        ]
        if False in values:
            return False
        return None if None in values else True
    if operation == "any":
        values = [
            _evaluate_condition(child, runtime_input)
            for child in condition.get("args", [])
        ]
        if True in values:
            return True
        return None if None in values else False
    if operation == "not":
        value = _evaluate_condition(condition.get("arg", {}), runtime_input)
        return None if value is None else not value
    found, value = _resolve_path(condition.get("path", ""), runtime_input)
    if operation == "exists":
        return found
    if not found:
        return None
    if operation == "eq":
        return type(value) is type(condition.get("value")) and value == condition.get(
            "value"
        )
    if operation == "neq":
        return not (
            type(value) is type(condition.get("value"))
            and value == condition.get("value")
        )
    if operation == "in":
        return any(
            type(value) is type(candidate) and value == candidate
            for candidate in condition.get("values", [])
        )
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not isfinite(value)
    ):
        return None
    threshold = condition.get("value")
    if isinstance(threshold, bool) or not isinstance(threshold, (int, float)):
        return None
    if operation == "gt":
        return value > threshold
    if operation == "gte":
        return value >= threshold
    if operation == "lt":
        return value < threshold
    if operation == "lte":
        return value <= threshold
    return None


def _decision(runtime_input, recommended, **fields) -> WorkflowPolicyDecision:
    return WorkflowPolicyDecision(
        recommended_outcome=recommended,
        applied_outcome=(
            "allow"
            if runtime_input.get("mode") == "observe" or recommended == "allow"
            else "deny"
        ),
        reason_codes=_unique(fields.get("reason_codes", ())),
        matched_control_ids=_unique(fields.get("matched_control_ids", ())),
        missing_fact_ids=_unique(fields.get("missing_fact_ids", ())),
        stale_fact_ids=_unique(fields.get("stale_fact_ids", ())),
        obligation_ids=_unique(fields.get("obligation_ids", ())),
        evidence_requirement_ids=_unique(fields.get("evidence_requirement_ids", ())),
        proposed_transition_id=fields.get("proposed_transition_id"),
        proposed_state=fields.get("proposed_state"),
        counterfactual_reason_codes=_unique(
            fields.get("counterfactual_reason_codes", ())
        ),
    )


def _control_targets(control, task_id, transition_id) -> bool:
    target = control.get("target", {})
    task_ids = target.get("taskIds", [])
    transition_ids = target.get("transitionIds", [])
    return (
        (not task_ids and not transition_ids)
        or task_id in task_ids
        or transition_id in transition_ids
    )


def _classify_facts(spec, runtime_input, fact_ids, now):
    definitions = {item.get("id"): item for item in spec.get("facts", [])}
    facts = runtime_input.get("facts", {})
    missing = []
    stale = []
    for fact_id in _unique(fact_ids):
        definition = definitions.get(fact_id)
        fact = facts.get(fact_id) if isinstance(facts, Mapping) else None
        if (
            not isinstance(definition, Mapping)
            or not isinstance(fact, Mapping)
            or fact.get("authoritative") is not True
            or fact.get("source") != definition.get("authoritativeSource")
        ):
            missing.append(fact_id)
            continue
        observed = _instant(fact.get("observedAt"))
        max_age = definition.get("maxAgeSeconds")
        if (
            observed is None
            or observed > now
            or (type(max_age) is int and observed + max_age < now)
        ):
            stale.append(fact_id)
    return _unique(missing), _unique(stale)


def _valid_approvals(approvals, control, runtime_input):
    requirement = control.get("approval")
    if not isinstance(requirement, Mapping):
        return (), False
    now = _instant(runtime_input.get("now"))
    actor = runtime_input.get("actor", {})
    valid = {}
    separation_violation = False
    for approval in approvals:
        if not isinstance(approval, Mapping):
            continue
        roles = approval.get("roleIds", [])
        if requirement.get("distinctFromActor") and approval.get(
            "actorRef"
        ) == actor.get("opaqueRef"):
            separation_violation = True
            continue
        if not any(role in requirement.get("roleIds", []) for role in roles):
            continue
        if any(role in roles for role in requirement.get("distinctFromRoleIds", [])):
            separation_violation = True
            continue
        duties = control.get("separationOfDuties")
        if (
            isinstance(duties, Mapping)
            and any(
                role in actor.get("roleIds", [])
                for role in duties.get("actorRoleIds", [])
            )
            and not any(role in roles for role in duties.get("approverRoleIds", []))
        ):
            separation_violation = True
            continue
        approved_at = _instant(approval.get("approvedAt"))
        expires_at = (
            _instant(approval.get("expiresAt"))
            if approval.get("expiresAt") is not None
            else None
        )
        expires_after = requirement.get("expiresAfterSeconds")
        if (
            now is None
            or approved_at is None
            or approved_at > now
            or (
                approval.get("expiresAt") is not None
                and (expires_at is None or expires_at <= now)
            )
            or (type(expires_after) is int and approved_at + expires_after <= now)
        ):
            continue
        valid[approval.get("actorRef")] = approval
    return tuple(valid.values()), separation_violation


def evaluate_workflow_policy(
    compiled_policy: Mapping[str, Any],
    runtime_input: Mapping[str, Any],
) -> WorkflowPolicyDecision:
    """Evaluate the closed AST with the same precedence as the TS compiler."""

    spec = compiled_policy["spec"]
    now = _instant(runtime_input.get("now"))
    if now is None:
        return _decision(
            runtime_input,
            "deny",
            reason_codes=("invalid_evaluation_time",),
            counterfactual_reason_codes=("supply_valid_evaluation_time",),
        )
    state = runtime_input.get("state", {})
    request = runtime_input.get("request", {})
    current_state = state.get("current")
    task_id = request.get("taskId")
    if current_state not in compiled_policy.get("stateIds", []):
        return _decision(
            runtime_input,
            "deny",
            reason_codes=("invalid_current_state",),
            counterfactual_reason_codes=("use_declared_current_state",),
        )
    task = next(
        (item for item in spec.get("tasks", []) if item.get("id") == task_id),
        None,
    )
    if task is None:
        return _decision(
            runtime_input,
            "deny",
            reason_codes=("unknown_task",),
            counterfactual_reason_codes=("use_declared_task",),
        )
    key = "%s:%s" % (current_state, task_id)
    transition_ids = compiled_policy.get("transitionIdsByStateAndTask", {}).get(key, [])
    transitions = [
        item
        for transition_id in transition_ids
        for item in spec.get("transitions", [])
        if item.get("id") == transition_id
    ]
    requested_transition = request.get("transitionId")
    if requested_transition:
        transitions = [
            item for item in transitions if item.get("id") == requested_transition
        ]
    if not transitions:
        return _decision(
            runtime_input,
            "deny",
            reason_codes=("invalid_state_task_transition",),
            counterfactual_reason_codes=(
                "request_transition_allowed_from_current_state",
            ),
        )
    evaluated = [
        (
            transition,
            _evaluate_condition(transition["condition"], runtime_input)
            if "condition" in transition
            else True,
        )
        for transition in transitions
    ]
    matched = [transition for transition, truth in evaluated if truth is True]
    if len(matched) > 1:
        return _decision(
            runtime_input,
            "deny",
            reason_codes=("ambiguous_transition_mapping",),
            counterfactual_reason_codes=("supply_facts_matching_exactly_one_branch",),
        )
    if not matched:
        unknown = any(truth is None for _, truth in evaluated)
        missing, stale = _classify_facts(
            spec,
            runtime_input,
            tuple(task.get("requiredFactIds", ()))
            + tuple(
                fact_id
                for transition, _ in evaluated
                for fact_id in transition.get("requiredFactIds", ())
            ),
            now,
        )
        return _decision(
            runtime_input,
            "indeterminate" if unknown else "deny",
            reason_codes=(
                "transition_condition_indeterminate"
                if unknown
                else "transition_condition_false",
            ),
            missing_fact_ids=missing,
            stale_fact_ids=stale,
            counterfactual_reason_codes=(
                "supply_authoritative_branch_facts"
                if unknown
                else "satisfy_transition_condition",
            ),
        )

    transition = matched[0]
    controls = [
        control
        for control in spec.get("controls", [])
        if _control_targets(control, task_id, transition["id"])
    ]
    evaluated_controls = [
        (
            control,
            _evaluate_condition(control["condition"], runtime_input)
            if "condition" in control
            else True,
        )
        for control in controls
    ]
    matched_controls = [
        control for control, truth in evaluated_controls if truth is True
    ]
    indeterminate_controls = [
        control for control, truth in evaluated_controls if truth is None
    ]
    deny_controls = [
        control for control in matched_controls if control.get("effect") == "deny"
    ]
    if deny_controls:
        return _decision(
            runtime_input,
            "deny",
            reason_codes=(item["reasonCode"] for item in deny_controls),
            matched_control_ids=(item["id"] for item in deny_controls),
            counterfactual_reason_codes=(
                "avoid_%s" % item["reasonCode"] for item in deny_controls
            ),
        )
    for control in matched_controls:
        idempotency = control.get("idempotency")
        if not isinstance(idempotency, Mapping):
            continue
        found, value = _resolve_path(idempotency.get("keyPath", ""), runtime_input)
        if not found or not isinstance(value, str) or not value:
            return _decision(
                runtime_input,
                "indeterminate",
                reason_codes=("missing_idempotency_key",),
                matched_control_ids=(control["id"],),
                counterfactual_reason_codes=("supply_reserved_idempotency_key",),
            )
        if value in runtime_input.get("usedIdempotencyKeys", []):
            return _decision(
                runtime_input,
                "deny",
                reason_codes=("duplicate_idempotency_key",),
                matched_control_ids=(control["id"],),
                counterfactual_reason_codes=("use_new_reserved_idempotency_key",),
            )
    required_facts = (
        tuple(task.get("requiredFactIds", ()))
        + tuple(transition.get("requiredFactIds", ()))
        + tuple(
            fact_id
            for control in matched_controls
            for fact_id in control.get("requiredFactIds", ())
        )
    )
    missing, stale = _classify_facts(spec, runtime_input, required_facts, now)
    if missing or stale or indeterminate_controls:
        return _decision(
            runtime_input,
            "indeterminate",
            reason_codes=(
                (("missing_authoritative_facts",) if missing else ())
                + (("stale_authoritative_facts",) if stale else ())
                + (
                    ("control_condition_indeterminate",)
                    if indeterminate_controls
                    else ()
                )
            ),
            matched_control_ids=tuple(
                item["id"] for item in matched_controls + indeterminate_controls
            ),
            missing_fact_ids=missing,
            stale_fact_ids=stale,
            counterfactual_reason_codes=(
                (("supply_missing_authoritative_facts",) if missing else ())
                + (("refresh_stale_authoritative_facts",) if stale else ())
                + (
                    ("supply_control_condition_facts",)
                    if indeterminate_controls
                    else ()
                )
            ),
        )
    evidence_definitions = {
        item.get("id"): item for item in spec.get("evidenceRequirements", [])
    }
    evidence_ids = _unique(
        tuple(task.get("evidenceRequirementIds", ()))
        + tuple(
            evidence_id
            for control in matched_controls
            for evidence_id in control.get("evidenceRequirementIds", ())
        )
    )
    evidence_ids = tuple(
        evidence_id
        for evidence_id in evidence_ids
        if evidence_definitions.get(evidence_id, {}).get("requiredBefore")
        != "state_commit"
        or runtime_input.get("phase") == "state_commit"
    )
    supplied_evidence = set(runtime_input.get("evidenceRefs", []))
    missing_evidence = tuple(
        evidence_id
        for evidence_id in evidence_ids
        if evidence_id not in supplied_evidence
    )
    if missing_evidence:
        return _decision(
            runtime_input,
            "indeterminate",
            reason_codes=("missing_required_evidence",),
            matched_control_ids=(item["id"] for item in matched_controls),
            evidence_requirement_ids=missing_evidence,
            counterfactual_reason_codes=("supply_required_evidence_references",),
        )
    unsatisfied = []
    for control in matched_controls:
        if control.get("effect") != "require_approval":
            continue
        approvals, separation = _valid_approvals(
            runtime_input.get("approvals", []), control, runtime_input
        )
        if separation:
            return _decision(
                runtime_input,
                "deny",
                reason_codes=("separation_of_duties_violation",),
                matched_control_ids=(control["id"],),
                counterfactual_reason_codes=("use_distinct_authorized_approver",),
            )
        requirement = control.get("approval", {})
        minimum = requirement.get("minApprovals")
        if type(minimum) is not int or len(approvals) < minimum:
            unsatisfied.append(control)
    if unsatisfied:
        return _decision(
            runtime_input,
            "require_approval",
            reason_codes=(item["reasonCode"] for item in unsatisfied),
            matched_control_ids=(item["id"] for item in unsatisfied),
            counterfactual_reason_codes=("supply_valid_distinct_approval",),
        )
    obligations = _unique(
        tuple(transition.get("obligationIds", ()))
        + tuple(
            obligation_id
            for control in matched_controls
            for obligation_id in control.get("obligationIds", ())
        )
    )
    return _decision(
        runtime_input,
        "allow",
        reason_codes=("allowed_with_obligations" if obligations else "allowed",),
        matched_control_ids=(item["id"] for item in matched_controls),
        obligation_ids=obligations,
        evidence_requirement_ids=evidence_ids,
        proposed_transition_id=transition["id"],
        proposed_state=transition["toStateId"],
    )
