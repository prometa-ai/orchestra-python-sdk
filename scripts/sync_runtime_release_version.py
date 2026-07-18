#!/usr/bin/env python3
"""Synchronize the SDK version across tenant-runtime release assets."""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Optional


SEMVER = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")


def _replace_exact(
    root: Path,
    relative_path: str,
    pattern: str,
    replacement: str,
    *,
    expected: int = 1,
) -> None:
    path = root / relative_path
    original = path.read_text(encoding="utf-8")
    updated, count = re.subn(pattern, replacement, original, flags=re.MULTILINE)
    if count != expected:
        raise ValueError(
            f"{relative_path}: expected {expected} version match(es), found {count}"
        )
    if updated != original:
        path.write_text(updated, encoding="utf-8")


def sync_runtime_release_version(
    root: Path, version: str, chart_version: Optional[str] = None
) -> None:
    if not SEMVER.fullmatch(version):
        raise ValueError(f"version must be MAJOR.MINOR.PATCH: {version}")
    if chart_version is not None and not SEMVER.fullmatch(chart_version):
        raise ValueError(f"chart version must be MAJOR.MINOR.PATCH: {chart_version}")

    _replace_exact(
        root,
        "pyproject.toml",
        r'^version = "[0-9]+\.[0-9]+\.[0-9]+"$',
        f'version = "{version}"',
    )
    _replace_exact(
        root,
        "prometa/__init__.py",
        r'^__version__ = "[0-9]+\.[0-9]+\.[0-9]+"$',
        f'__version__ = "{version}"',
    )
    _replace_exact(
        root,
        "deploy/reference-runtime/chart/Chart.yaml",
        r'^appVersion: "[0-9]+\.[0-9]+\.[0-9]+"$',
        f'appVersion: "{version}"',
    )
    if chart_version is not None:
        _replace_exact(
            root,
            "deploy/reference-runtime/chart/Chart.yaml",
            r"^version: [0-9]+\.[0-9]+\.[0-9]+$",
            f"version: {chart_version}",
        )

    package_pattern = (
        r'"prometa-sdk\[runtime-host,runtime-mcp\]=='
        r'[0-9]+\.[0-9]+\.[0-9]+"'
    )
    for dockerfile in (
        "deploy/reference-runtime/Dockerfile",
        "deploy/reference-runtime/Dockerfile.ubi",
    ):
        _replace_exact(
            root,
            dockerfile,
            r"^ARG IMAGE_VERSION=[0-9]+\.[0-9]+\.[0-9]+$",
            f"ARG IMAGE_VERSION={version}",
        )
        _replace_exact(
            root,
            dockerfile,
            package_pattern,
            f'"prometa-sdk[runtime-host,runtime-mcp]=={version}"',
        )

    _replace_exact(
        root,
        "deploy/reference-runtime/compose.yaml",
        r"prometa-runtime-host:[0-9]+\.[0-9]+\.[0-9]+",
        f"prometa-runtime-host:{version}",
        expected=2,
    )
    for profile in (
        "deploy/reference-runtime/topology-profiles.json",
        "deploy/reference-runtime/topology-profiles.mcp.json",
    ):
        _replace_exact(
            root,
            profile,
            r'^(\s*)"runtimeVersion": "[0-9]+\.[0-9]+\.[0-9]+",$',
            rf'\1"runtimeVersion": "{version}",',
        )
        if chart_version is not None:
            _replace_exact(
                root,
                profile,
                r'^(\s*)"chartVersion": "[0-9]+\.[0-9]+\.[0-9]+",$',
                rf'\1"chartVersion": "{chart_version}",',
            )
    for example in (
        "deploy/reference-runtime/config.example.json",
        "deploy/reference-runtime/config.mcp.example.json",
        "deploy/reference-runtime/config.pull.example.json",
    ):
        _replace_exact(
            root,
            example,
            r'^(\s*)"runtimeVersion": "[0-9]+\.[0-9]+\.[0-9]+",$',
            rf'\1"runtimeVersion": "{version}",',
        )
    for values_file in (
        "deploy/reference-runtime/chart/values.production.example.yaml",
        "deploy/reference-runtime/chart/values.mcp.example.yaml",
    ):
        _replace_exact(
            root,
            values_file,
            r'^(\s*)tag: "[0-9]+\.[0-9]+\.[0-9]+"$',
            rf'\1tag: "{version}"',
        )
    _replace_exact(
        root,
        "examples/runtime_kernel_quickstart.py",
        r'(add_argument\("--runtime-version", default=")'
        r"[0-9]+\.[0-9]+\.[0-9]+"
        r'("\))',
        rf"\g<1>{version}\g<2>",
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("version", help="Release version in MAJOR.MINOR.PATCH form")
    parser.add_argument(
        "--chart-version",
        help="Independent Helm chart version in MAJOR.MINOR.PATCH form",
    )
    parser.add_argument(
        "--repository-root",
        type=Path,
        default=Path(__file__).resolve().parent.parent,
    )
    args = parser.parse_args()
    sync_runtime_release_version(
        args.repository_root.resolve(), args.version, args.chart_version
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
