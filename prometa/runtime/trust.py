"""Fail-closed verification for Orchestra runtime-admission artifacts.

This module is the first, deliberately narrow runtime capability in the SDK.
It verifies bundle integrity and independent promotion authorization at a
tenant admission boundary; it does not execute an agent or put the Orchestra
control plane in the tenant's synchronous request path.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, FrozenSet, Iterable, Mapping, MutableSet, Optional, Tuple


ENVELOPE_VERSION = 1
ENVELOPE_CANONICALIZATION = "signed-payload-json-v1"
PROMOTION_ATTESTATION_VERSION = 1
PROMOTION_ATTESTATION_TYPE = "orchestra.promotion-attestation"
PROMOTION_ATTESTATION_CANONICALIZATION = "signed-payload-json-v1"
MAX_PROMOTION_APPROVALS = 10
SUPPORTED_TARGET_ENVIRONMENTS = frozenset({"dev", "test", "staging", "prod"})


class BundleVerificationError(ValueError):
    """A fail-closed artifact rejection with a stable machine-readable code."""

    def __init__(self, code: str, message: Optional[str] = None) -> None:
        self.code = code
        super().__init__(message or code.replace("_", " "))


@dataclass(frozen=True)
class BundleTrustEntry:
    """One tenant-controlled Ed25519 trust-store entry.

    The transport bundle's embedded public key is intentionally absent from
    this type. Trust is provisioned out of band and selected by issuer/key ID.
    """

    issuer: str
    key_id: str
    public_key_spki_der_base64: str
    allowed_org_ids: Optional[FrozenSet[str]] = None
    allowed_audiences: Optional[FrozenSet[str]] = None
    allowed_environments: Optional[FrozenSet[str]] = None
    active_from: Optional[datetime] = None
    retired_at: Optional[datetime] = None


class BundleTrustStore:
    """In-memory trust-store snapshot suitable for offline verification."""

    def __init__(self, entries: Iterable[BundleTrustEntry]) -> None:
        indexed: Dict[Tuple[str, str], BundleTrustEntry] = {}
        for entry in entries:
            if not entry.issuer or not entry.key_id:
                raise ValueError("trust entries require issuer and key_id")
            key = (entry.issuer, entry.key_id)
            if key in indexed:
                raise ValueError("duplicate trust entry: %s/%s" % key)
            indexed[key] = entry
        self._entries = indexed

    def resolve(self, issuer: str, key_id: str) -> BundleTrustEntry:
        try:
            return self._entries[(issuer, key_id)]
        except KeyError as exc:
            raise BundleVerificationError(
                "unknown_signing_key",
                "No trusted bundle key for issuer=%r key_id=%r" % (issuer, key_id),
            ) from exc


@dataclass(frozen=True)
class VerifiedBundle:
    """Content and claims parsed only from successfully verified bytes."""

    claims: Mapping[str, Any]
    content: Mapping[str, Any]
    signed_payload: str
    trust_entry: BundleTrustEntry

    @property
    def artifact_digest(self) -> str:
        return str(self.claims["artifactDigest"])

    @property
    def jti(self) -> str:
        return str(self.claims["jti"])


@dataclass(frozen=True)
class VerifiedPromotionAttestation:
    """Claims parsed only after purpose/type, trust, and signature checks."""

    claims: Mapping[str, Any]
    signed_payload: str
    trust_entry: BundleTrustEntry

    @property
    def attestation_id(self) -> str:
        return str(self.claims["subject"]).removeprefix("promotion-attestation:")

    @property
    def artifact_digest(self) -> str:
        return str(self.claims["artifactDigest"])

    @property
    def jti(self) -> str:
        return str(self.claims["jti"])


def _reject_duplicate_keys(pairs: Iterable[Tuple[str, Any]]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key: %s" % key)
        result[key] = value
    return result


def _reject_json_constant(value: str) -> None:
    raise ValueError("non-finite JSON number: %s" % value)


def _parse_json_object(value: str, code: str) -> Dict[str, Any]:
    try:
        parsed = json.loads(
            value,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_json_constant,
        )
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise BundleVerificationError(code, "Invalid JSON artifact bytes") from exc
    if not isinstance(parsed, dict):
        raise BundleVerificationError(code, "Artifact JSON must be an object")
    return parsed


def _digest_json(value: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _required_string(value: Mapping[str, Any], key: str, code: str) -> str:
    candidate = value.get(key)
    if not isinstance(candidate, str) or not candidate:
        raise BundleVerificationError(code, "Missing or invalid %s" % key)
    return candidate


def _parse_instant(value: Mapping[str, Any], key: str) -> datetime:
    raw = _required_string(value, key, "invalid_time_claim")
    normalized = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise BundleVerificationError(
            "invalid_time_claim", "Invalid ISO-8601 %s" % key
        ) from exc
    if parsed.tzinfo is None:
        raise BundleVerificationError(
            "invalid_time_claim", "%s must include a timezone" % key
        )
    return parsed.astimezone(timezone.utc)


def _as_utc(value: Optional[datetime]) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        raise BundleVerificationError("invalid_now", "now must be timezone-aware")
    return current.astimezone(timezone.utc)


def _entry_time(value: Optional[datetime], field: str) -> Optional[datetime]:
    if value is None:
        return None
    if value.tzinfo is None:
        raise BundleVerificationError(
            "invalid_trust_entry", "%s must be timezone-aware" % field
        )
    return value.astimezone(timezone.utc)


def _decode_base64(value: str, code: str) -> bytes:
    try:
        return base64.b64decode("".join(value.split()), validate=True)
    except (binascii.Error, ValueError) as exc:
        raise BundleVerificationError(code, "Invalid base64 data") from exc


def _verify_ed25519(
    entry: BundleTrustEntry,
    signed_payload: str,
    signature_base64: str,
) -> None:
    try:
        from cryptography.exceptions import InvalidSignature
        from cryptography.hazmat.primitives.asymmetric.ed25519 import (
            Ed25519PublicKey,
        )
        from cryptography.hazmat.primitives.serialization import load_der_public_key
    except ImportError as exc:  # pragma: no cover - exercised by core-only CI smoke
        raise BundleVerificationError(
            "runtime_dependency_missing",
            "Install prometa-sdk[runtime] to verify runtime artifacts",
        ) from exc

    key_der = _decode_base64(
        entry.public_key_spki_der_base64, "invalid_trust_key"
    )
    signature = _decode_base64(signature_base64, "malformed_signature")
    try:
        public_key = load_der_public_key(key_der)
    except (TypeError, ValueError) as exc:
        raise BundleVerificationError("invalid_trust_key") from exc
    if not isinstance(public_key, Ed25519PublicKey):
        raise BundleVerificationError("invalid_trust_key", "Trusted key is not Ed25519")
    try:
        public_key.verify(signature, signed_payload.encode("utf-8"))
    except InvalidSignature as exc:
        raise BundleVerificationError("invalid_signature") from exc


def _validate_trust_constraints(
    entry: BundleTrustEntry,
    claims: Mapping[str, Any],
    current: datetime,
    skew: timedelta,
) -> None:
    active_from = _entry_time(entry.active_from, "active_from")
    retired_at = _entry_time(entry.retired_at, "retired_at")
    if active_from is not None and current + skew < active_from:
        raise BundleVerificationError("signing_key_not_active")
    if retired_at is not None and current >= retired_at:
        raise BundleVerificationError("signing_key_retired")

    constrained = (
        (entry.allowed_org_ids, "orgId", "signing_key_org_denied"),
        (entry.allowed_audiences, "audience", "signing_key_audience_denied"),
        (
            entry.allowed_environments,
            "targetEnvironment",
            "signing_key_environment_denied",
        ),
    )
    for allowed, claim_name, error_code in constrained:
        if allowed is not None and claims.get(claim_name) not in allowed:
            raise BundleVerificationError(error_code)


def verify_bundle_envelope(
    bundle: Mapping[str, Any],
    trust_store: BundleTrustStore,
    *,
    expected_org_id: str,
    expected_audience: str,
    expected_environment: str,
    now: Optional[datetime] = None,
    revoked_key_ids: Iterable[str] = (),
    revoked_jtis: Iterable[str] = (),
    seen_jtis: Optional[MutableSet[str]] = None,
    max_clock_skew_seconds: int = 60,
    max_signed_payload_bytes: int = 8 * 1024 * 1024,
    enforce_offline_lease: bool = True,
    require_deployable: bool = True,
) -> VerifiedBundle:
    """Verify and admit one bundle envelope.

    ``seen_jtis`` is an optional caller-owned replay store. It is updated only
    after every verification step succeeds; callers that need cross-process
    atomic replay protection should wrap this verifier with a durable store.
    """

    if not expected_org_id or not expected_audience or not expected_environment:
        raise BundleVerificationError("missing_expected_binding")
    if expected_environment not in SUPPORTED_TARGET_ENVIRONMENTS:
        raise BundleVerificationError("unsupported_expected_environment")
    if type(max_clock_skew_seconds) is not int or max_clock_skew_seconds < 0:
        raise BundleVerificationError("invalid_clock_skew")
    if type(max_signed_payload_bytes) is not int or max_signed_payload_bytes < 1:
        raise BundleVerificationError("invalid_payload_limit")

    if bundle.get("signed") is not True:
        raise BundleVerificationError("unsigned_bundle")
    if bundle.get("algorithm") != "ed25519":
        raise BundleVerificationError("unsupported_algorithm")
    if (
        type(bundle.get("envelopeVersion")) is not int
        or bundle.get("envelopeVersion") != ENVELOPE_VERSION
    ):
        raise BundleVerificationError("unsupported_envelope_version")
    if bundle.get("envelopeCanonicalization") != ENVELOPE_CANONICALIZATION:
        raise BundleVerificationError("unsupported_envelope_canonicalization")

    signed_payload = _required_string(
        bundle, "signedPayload", "missing_signed_payload"
    )
    envelope_signature = _required_string(
        bundle, "envelopeSignature", "missing_signature"
    )
    if len(signed_payload.encode("utf-8")) > max_signed_payload_bytes:
        raise BundleVerificationError("signed_payload_too_large")

    # These transport fields are untrusted selectors only. The resolved key
    # verifies the payload before any signed JSON is parsed, and the signed
    # issuer/key claims must match the selectors afterward.
    transport_issuer = _required_string(bundle, "issuer", "missing_issuer")
    transport_key_id = _required_string(bundle, "keyId", "missing_key_id")
    if transport_key_id in frozenset(revoked_key_ids):
        raise BundleVerificationError("revoked_signing_key")

    trust_entry = trust_store.resolve(transport_issuer, transport_key_id)
    _verify_ed25519(trust_entry, signed_payload, envelope_signature)
    claims = _parse_json_object(signed_payload, "malformed_signed_payload")

    if (
        type(claims.get("envelopeVersion")) is not int
        or claims.get("envelopeVersion") != ENVELOPE_VERSION
    ):
        raise BundleVerificationError("unsupported_envelope_version")
    issuer = _required_string(claims, "issuer", "invalid_claims")
    key_id = _required_string(claims, "keyId", "invalid_claims")
    org_id = _required_string(claims, "orgId", "invalid_claims")
    audience = _required_string(claims, "audience", "invalid_claims")
    environment = _required_string(
        claims, "targetEnvironment", "invalid_claims"
    )
    subject = _required_string(claims, "subject", "invalid_claims")
    jti = _required_string(claims, "jti", "invalid_claims")
    artifact_digest = _required_string(
        claims, "artifactDigest", "invalid_claims"
    )
    content_canonical = _required_string(
        claims, "contentCanonical", "invalid_claims"
    )

    if issuer != transport_issuer or key_id != transport_key_id:
        raise BundleVerificationError("trust_selector_mismatch")
    if bundle.get("artifactDigest") != artifact_digest:
        raise BundleVerificationError("transport_digest_mismatch")
    if org_id != expected_org_id:
        raise BundleVerificationError("wrong_org")
    if audience != expected_audience:
        raise BundleVerificationError("wrong_audience")
    if environment != expected_environment:
        raise BundleVerificationError("wrong_environment")
    if environment not in SUPPORTED_TARGET_ENVIRONMENTS:
        raise BundleVerificationError("unsupported_target_environment")

    current = _as_utc(now)
    skew = timedelta(seconds=max_clock_skew_seconds)
    _validate_trust_constraints(trust_entry, claims, current, skew)

    issued_at = _parse_instant(claims, "issuedAt")
    not_before = _parse_instant(claims, "notBefore")
    expires_at = _parse_instant(claims, "expiresAt")
    offline_lease_expires_at = _parse_instant(claims, "offlineLeaseExpiresAt")
    if not (issued_at <= not_before < expires_at):
        raise BundleVerificationError("invalid_validity_window")
    if offline_lease_expires_at < not_before or offline_lease_expires_at > expires_at:
        raise BundleVerificationError("invalid_offline_lease")
    if current + skew < not_before:
        raise BundleVerificationError("not_yet_valid")
    if current >= expires_at:
        raise BundleVerificationError("expired_bundle")
    if enforce_offline_lease and current >= offline_lease_expires_at:
        raise BundleVerificationError("offline_lease_expired")

    revoked_jti_set = frozenset(revoked_jtis)
    if jti in revoked_jti_set:
        raise BundleVerificationError("revoked_bundle")
    if seen_jtis is not None and jti in seen_jtis:
        raise BundleVerificationError("replayed_bundle")

    expected_digest = "sha256:" + hashlib.sha256(
        content_canonical.encode("utf-8")
    ).hexdigest()
    if not hmac.compare_digest(expected_digest, artifact_digest):
        raise BundleVerificationError("artifact_digest_mismatch")

    content = _parse_json_object(content_canonical, "malformed_bundle_content")
    if content.get("schemaVersion") != 1:
        raise BundleVerificationError("unsupported_bundle_schema")
    manifest = content.get("manifest")
    if not isinstance(manifest, dict):
        raise BundleVerificationError("invalid_manifest")
    manifest_id = _required_string(manifest, "id", "invalid_manifest")
    manifest_version = manifest.get("version")
    if type(manifest_version) is not int or manifest_version < 1:
        raise BundleVerificationError("invalid_manifest")
    expected_subject = "agent-manifest:%s:v%s" % (manifest_id, manifest_version)
    if subject != expected_subject:
        raise BundleVerificationError("subject_mismatch")
    if require_deployable and manifest.get("deployable") is not True:
        raise BundleVerificationError("bundle_not_deployable")
    if "content" in bundle and bundle.get("content") != content:
        raise BundleVerificationError("transport_content_mismatch")

    if seen_jtis is not None:
        seen_jtis.add(jti)
    return VerifiedBundle(
        claims=claims,
        content=content,
        signed_payload=signed_payload,
        trust_entry=trust_entry,
    )


def _expected_gate_stage(environment: str) -> str:
    if environment == "dev":
        return "dev"
    if environment in {"test", "staging"}:
        return "test"
    return "prod"


def _require_sha256_digest(claims: Mapping[str, Any], key: str) -> str:
    value = _required_string(claims, key, "invalid_claims")
    if len(value) != 71 or not value.startswith("sha256:"):
        raise BundleVerificationError("invalid_claims", "Invalid %s" % key)
    try:
        int(value[7:], 16)
    except ValueError as exc:
        raise BundleVerificationError("invalid_claims", "Invalid %s" % key) from exc
    if value[7:] != value[7:].lower():
        raise BundleVerificationError("invalid_claims", "Invalid %s" % key)
    return value


def verify_promotion_attestation(
    attestation: Mapping[str, Any],
    trust_store: BundleTrustStore,
    *,
    expected_org_id: str,
    expected_audience: str,
    expected_environment: str,
    expected_artifact_digest: str,
    expected_release_id: str,
    expected_deployment_id: str,
    expected_runtime: str,
    minimum_approvals: int = 0,
    now: Optional[datetime] = None,
    revoked_key_ids: Iterable[str] = (),
    revoked_jtis: Iterable[str] = (),
    revoked_attestation_ids: Iterable[str] = (),
    seen_jtis: Optional[MutableSet[str]] = None,
    max_clock_skew_seconds: int = 60,
    max_signed_payload_bytes: int = 1024 * 1024,
    enforce_offline_lease: bool = True,
) -> VerifiedPromotionAttestation:
    """Verify one signed promotion authorization before runtime admission.

    A valid bundle is deliberately insufficient here. This verifier requires
    the purpose-specific attestation type and all release/environment/runtime
    bindings, then updates an optional replay store only after every check.
    """

    expected_values = (
        expected_org_id,
        expected_audience,
        expected_environment,
        expected_artifact_digest,
        expected_release_id,
        expected_deployment_id,
        expected_runtime,
    )
    if any(not value for value in expected_values):
        raise BundleVerificationError("missing_expected_binding")
    if expected_environment not in SUPPORTED_TARGET_ENVIRONMENTS:
        raise BundleVerificationError("unsupported_expected_environment")
    if (
        type(minimum_approvals) is not int
        or minimum_approvals < 0
        or minimum_approvals > MAX_PROMOTION_APPROVALS
    ):
        raise BundleVerificationError("invalid_minimum_approvals")
    if type(max_clock_skew_seconds) is not int or max_clock_skew_seconds < 0:
        raise BundleVerificationError("invalid_clock_skew")
    if type(max_signed_payload_bytes) is not int or max_signed_payload_bytes < 1:
        raise BundleVerificationError("invalid_payload_limit")

    if attestation.get("signed") is not True:
        raise BundleVerificationError("unsigned_attestation")
    if attestation.get("algorithm") != "ed25519":
        raise BundleVerificationError("unsupported_algorithm")
    if (
        type(attestation.get("attestationVersion")) is not int
        or attestation.get("attestationVersion") != PROMOTION_ATTESTATION_VERSION
    ):
        raise BundleVerificationError("unsupported_attestation_version")
    if (
        attestation.get("canonicalization")
        != PROMOTION_ATTESTATION_CANONICALIZATION
    ):
        raise BundleVerificationError("unsupported_attestation_canonicalization")

    signed_payload = _required_string(
        attestation, "signedPayload", "missing_signed_payload"
    )
    signature = _required_string(attestation, "signature", "missing_signature")
    if len(signed_payload.encode("utf-8")) > max_signed_payload_bytes:
        raise BundleVerificationError("signed_payload_too_large")

    transport_id = _required_string(
        attestation, "attestationId", "missing_attestation_id"
    )
    transport_issuer = _required_string(attestation, "issuer", "missing_issuer")
    transport_key_id = _required_string(attestation, "keyId", "missing_key_id")
    if transport_key_id in frozenset(revoked_key_ids):
        raise BundleVerificationError("revoked_signing_key")

    trust_entry = trust_store.resolve(transport_issuer, transport_key_id)
    _verify_ed25519(trust_entry, signed_payload, signature)
    claims = _parse_json_object(signed_payload, "malformed_signed_payload")

    if claims.get("artifactType") != PROMOTION_ATTESTATION_TYPE:
        raise BundleVerificationError("wrong_artifact_type")
    if (
        type(claims.get("attestationVersion")) is not int
        or claims.get("attestationVersion") != PROMOTION_ATTESTATION_VERSION
    ):
        raise BundleVerificationError("unsupported_attestation_version")

    issuer = _required_string(claims, "issuer", "invalid_claims")
    key_id = _required_string(claims, "keyId", "invalid_claims")
    subject = _required_string(claims, "subject", "invalid_claims")
    org_id = _required_string(claims, "orgId", "invalid_claims")
    audience = _required_string(claims, "audience", "invalid_claims")
    environment = _required_string(
        claims, "targetEnvironment", "invalid_claims"
    )
    artifact_id = _required_string(claims, "artifactId", "invalid_claims")
    artifact_digest = _require_sha256_digest(claims, "artifactDigest")
    manifest_id = _required_string(claims, "manifestId", "invalid_claims")
    manifest_version = claims.get("manifestVersion")
    agent_id = _required_string(claims, "agentId", "invalid_claims")
    decision_id = _required_string(claims, "decisionId", "invalid_claims")
    gate_stage = _required_string(claims, "gateStage", "invalid_claims")
    policy_digest = _require_sha256_digest(claims, "policySetDigest")
    evidence_digest = _require_sha256_digest(claims, "evidenceDigest")
    requested_runtime = _required_string(
        claims, "requestedRuntime", "invalid_claims"
    )
    release_id = _required_string(claims, "releaseId", "invalid_claims")
    deployment_id = _required_string(claims, "deploymentId", "invalid_claims")
    jti = _required_string(claims, "jti", "invalid_claims")
    revocation_ref = _required_string(claims, "revocationRef", "invalid_claims")

    if issuer != transport_issuer or key_id != transport_key_id:
        raise BundleVerificationError("trust_selector_mismatch")
    if subject != "promotion-attestation:%s" % transport_id:
        raise BundleVerificationError("subject_mismatch")
    if revocation_ref != "urn:prometa:promotion-attestation:%s" % transport_id:
        raise BundleVerificationError("revocation_ref_mismatch")
    if org_id != expected_org_id:
        raise BundleVerificationError("wrong_org")
    if audience != expected_audience:
        raise BundleVerificationError("wrong_audience")
    if environment != expected_environment:
        raise BundleVerificationError("wrong_environment")
    if environment not in SUPPORTED_TARGET_ENVIRONMENTS:
        raise BundleVerificationError("unsupported_target_environment")
    if artifact_digest != expected_artifact_digest:
        raise BundleVerificationError("wrong_artifact_digest")
    if release_id != expected_release_id:
        raise BundleVerificationError("wrong_release")
    if deployment_id != expected_deployment_id:
        raise BundleVerificationError("wrong_deployment")
    if requested_runtime != expected_runtime:
        raise BundleVerificationError("wrong_runtime")
    if claims.get("decisionAllow") is not True:
        raise BundleVerificationError("gate_not_allowed")
    if gate_stage != _expected_gate_stage(environment):
        raise BundleVerificationError("gate_stage_mismatch")
    if type(manifest_version) is not int or manifest_version < 1:
        raise BundleVerificationError("invalid_claims")

    # Required non-empty identities/digests are read above even when the
    # caller does not independently bind every one of them yet.
    _ = (artifact_id, manifest_id, agent_id, decision_id, policy_digest)
    _ = (evidence_digest, revocation_ref)

    current = _as_utc(now)
    skew = timedelta(seconds=max_clock_skew_seconds)
    _validate_trust_constraints(trust_entry, claims, current, skew)

    decision_evaluated_at = _parse_instant(claims, "decisionEvaluatedAt")
    decision_valid_until = _parse_instant(claims, "decisionValidUntil")
    issued_at = _parse_instant(claims, "issuedAt")
    not_before = _parse_instant(claims, "notBefore")
    expires_at = _parse_instant(claims, "expiresAt")
    offline_lease_expires_at = _parse_instant(claims, "offlineLeaseExpiresAt")
    if not (
        decision_evaluated_at <= issued_at <= not_before < expires_at
        <= decision_valid_until
    ):
        raise BundleVerificationError("invalid_validity_window")
    if offline_lease_expires_at < not_before or offline_lease_expires_at > expires_at:
        raise BundleVerificationError("invalid_offline_lease")
    if current + skew < not_before:
        raise BundleVerificationError("not_yet_valid")
    if current >= expires_at:
        raise BundleVerificationError("expired_attestation")
    if enforce_offline_lease and current >= offline_lease_expires_at:
        raise BundleVerificationError("offline_lease_expired")

    approval_requirement = claims.get("approvalRequirement")
    if approval_requirement is None:
        signed_minimum_approvals = 0
    else:
        if not isinstance(approval_requirement, dict):
            raise BundleVerificationError("invalid_approvals")
        signed_minimum_approvals = approval_requirement.get("minimum")
        if (
            type(signed_minimum_approvals) is not int
            or signed_minimum_approvals < 0
            or signed_minimum_approvals > MAX_PROMOTION_APPROVALS
        ):
            raise BundleVerificationError("invalid_approvals")
        _required_string(approval_requirement, "policy", "invalid_approvals")

    approvals = claims.get("approvals")
    if not isinstance(approvals, list) or len(approvals) > MAX_PROMOTION_APPROVALS:
        raise BundleVerificationError("invalid_approvals")
    approval_scope = {
        "artifactId": artifact_id,
        "artifactDigest": artifact_digest,
        "decisionId": decision_id,
        "targetEnvironment": environment,
        "agentId": agent_id,
        "requestedRuntime": requested_runtime,
        "releaseId": release_id,
        "deploymentId": deployment_id,
    }
    expected_scope_digest = _digest_json(approval_scope)
    approval_ids = set()
    approval_identities = set()
    for approval in approvals:
        if not isinstance(approval, dict):
            raise BundleVerificationError("invalid_approvals")
        approval_id = _required_string(approval, "approvalId", "invalid_approvals")
        identity = _required_string(approval, "identity", "invalid_approvals")
        method = _required_string(approval, "method", "invalid_approvals")
        _required_string(approval, "role", "invalid_approvals")
        scope_digest = _require_sha256_digest(approval, "scopeDigest")
        approved_at = _parse_instant(approval, "approvedAt")
        approval_expires_at = _parse_instant(approval, "expiresAt")
        if (
            approved_at < decision_evaluated_at
            or approved_at > issued_at
            or approval_expires_at <= approved_at
            or approval_expires_at < expires_at
            or approval_expires_at > decision_valid_until
        ):
            raise BundleVerificationError("invalid_approvals")
        if not identity.startswith("user:") or identity == "user:":
            raise BundleVerificationError("invalid_approvals")
        if method != "prometa-session":
            raise BundleVerificationError("invalid_approvals")
        if scope_digest != expected_scope_digest:
            raise BundleVerificationError("approval_scope_mismatch")
        if approval_id in approval_ids or identity in approval_identities:
            raise BundleVerificationError("duplicate_approval_identity")
        approval_ids.add(approval_id)
        approval_identities.add(identity)

    required_approvals = max(signed_minimum_approvals, minimum_approvals)
    if len(approval_identities) < required_approvals:
        raise BundleVerificationError("insufficient_approvals")

    if transport_id in frozenset(revoked_attestation_ids):
        raise BundleVerificationError("revoked_attestation")
    if jti in frozenset(revoked_jtis):
        raise BundleVerificationError("revoked_attestation")
    if seen_jtis is not None and jti in seen_jtis:
        raise BundleVerificationError("replayed_attestation")

    mirror = attestation.get("authorization")
    if mirror is not None:
        if not isinstance(mirror, dict):
            raise BundleVerificationError("transport_authorization_mismatch")
        expected_mirror = {
            "artifactId": artifact_id,
            "artifactDigest": artifact_digest,
            "decisionId": decision_id,
            "releaseId": release_id,
            "deploymentId": deployment_id,
            "targetEnvironment": environment,
            "requestedRuntime": requested_runtime,
            "expiresAt": claims.get("expiresAt"),
            "offlineLeaseExpiresAt": claims.get("offlineLeaseExpiresAt"),
        }
        if mirror != expected_mirror:
            raise BundleVerificationError("transport_authorization_mismatch")

    if seen_jtis is not None:
        seen_jtis.add(jti)
    return VerifiedPromotionAttestation(
        claims=claims,
        signed_payload=signed_payload,
        trust_entry=trust_entry,
    )


__all__ = [
    "BundleTrustEntry",
    "BundleTrustStore",
    "BundleVerificationError",
    "VerifiedBundle",
    "VerifiedPromotionAttestation",
    "verify_bundle_envelope",
    "verify_promotion_attestation",
]
