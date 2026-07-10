"""Optional tenant-runtime contracts.

Install ``prometa-sdk[runtime]`` before using this package. The default
``prometa-sdk`` installation remains dependency-free and telemetry-first.
"""

from .trust import (
    BundleTrustEntry,
    BundleTrustStore,
    BundleVerificationError,
    VerifiedBundle,
    verify_bundle_envelope,
)

__all__ = [
    "BundleTrustEntry",
    "BundleTrustStore",
    "BundleVerificationError",
    "VerifiedBundle",
    "verify_bundle_envelope",
]
