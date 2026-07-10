"""Optional tenant-runtime contracts.

Install ``prometa-sdk[runtime]`` for cryptographic artifact verification. The
receipt builder/client remain standard-library-only, and the default
``prometa-sdk`` installation remains dependency-free and telemetry-first.
"""

from .trust import (
    BundleTrustEntry,
    BundleTrustStore,
    BundleVerificationError,
    VerifiedBundle,
    VerifiedPromotionAttestation,
    verify_bundle_envelope,
    verify_promotion_attestation,
)
from .receipts import (
    RuntimeReceiptClient,
    RuntimeReceiptError,
    RuntimeReceiptSubmissionError,
    build_runtime_receipt,
)

__all__ = [
    "BundleTrustEntry",
    "BundleTrustStore",
    "BundleVerificationError",
    "VerifiedBundle",
    "VerifiedPromotionAttestation",
    "verify_bundle_envelope",
    "verify_promotion_attestation",
    "RuntimeReceiptClient",
    "RuntimeReceiptError",
    "RuntimeReceiptSubmissionError",
    "build_runtime_receipt",
]
