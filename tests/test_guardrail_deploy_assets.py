"""The shipped guardrail-service deployment assets, checked against the loaders."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from prometa.guardrail import (
    GuardrailHostError,
    GuardrailService,
    load_guardrail_api_keys,
    load_guardrail_host_config,
)


ROOT = Path(__file__).parent.parent
ASSETS = ROOT / "deploy/guardrail-service"


def _project_version() -> str:
    project = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    match = re.search(r'^version = "([^"]+)"$', project, re.MULTILINE)
    assert match is not None
    return match.group(1)


def _example_keys_path(tmp_path, key: str) -> Path:
    document = json.loads((ASSETS / "api-keys.example.json").read_text(encoding="utf-8"))
    document[0]["key"] = key
    path = tmp_path / "api-keys.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


def test_the_example_config_loads_through_the_shipped_loader(tmp_path) -> None:
    document = json.loads(
        (ASSETS / "config.example.json").read_text(encoding="utf-8")
    )
    keys_path = _example_keys_path(tmp_path, "8f2c" * 12)
    document["apiKeysFile"] = str(keys_path)
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(document), encoding="utf-8")

    config = load_guardrail_host_config(config_path)
    keys = load_guardrail_api_keys(config.api_keys_file)
    service = GuardrailService(config.profiles, default_profile=config.default_profile)

    assert sorted(config.profiles) == ["prod-strict", "staging-observe"]
    assert config.profiles["prod-strict"].fail_mode == "closed"
    assert config.profiles["staging-observe"].fail_mode == "open"
    assert keys[0].tenant == "acme-prod"
    assert sorted(service.profiles) == ["prod-strict", "staging-observe"]


def test_every_shipped_profile_declares_guardrails_and_serves_a_request(tmp_path) -> None:
    """A profile declaring none can serve nothing, so it is not a usable default."""

    document = json.loads((ASSETS / "config.example.json").read_text(encoding="utf-8"))
    keys_path = _example_keys_path(tmp_path, "8f2c" * 12)
    document["apiKeysFile"] = str(keys_path)
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(document), encoding="utf-8")
    config = load_guardrail_host_config(config_path)
    service = GuardrailService(config.profiles, default_profile=config.default_profile)

    for profile_id, profile in config.profiles.items():
        assert profile.guardrails, profile_id
        response = service.evaluate(
            {
                "contractVersion": 1,
                "requestId": "deploy-" + profile_id,
                "stage": "llm_output",
                "profile": profile_id,
                "budgetMs": 40,
                # Name-only, as the engine sends it.
                "payload": {"kind": "text", "text": "aws key AKIAIOSFODNN7EXAMPLE"},
                "guardrails": [
                    {"name": name, "guardrailType": None, "onViolation": None}
                    for name in profile.guardrails
                ],
                "subject": {"tenant": "acme-prod"},
                "tool": None,
            }
        )
        assert response.status == 200, (profile_id, response.body)
        assert "secret-egress" in response.body["evaluatedGuardrails"]

    assert config.profiles["prod-strict"].guardrails["secret-egress"].on_violation == "block"
    assert config.profiles["staging-observe"].guardrails["secret-egress"].on_violation == "log"


def test_the_shipped_example_key_is_refused_verbatim(tmp_path) -> None:
    keys_path = tmp_path / "api-keys.json"
    keys_path.write_text(
        (ASSETS / "api-keys.example.json").read_text(encoding="utf-8"),
        encoding="utf-8",
    )

    with pytest.raises(GuardrailHostError) as error:
        load_guardrail_api_keys(keys_path)

    assert error.value.code == "guardrail_api_key_placeholder"


def test_the_chart_and_image_agree_on_the_port_and_the_probes() -> None:
    values = (ASSETS / "chart/values.yaml").read_text(encoding="utf-8")
    deployment = (ASSETS / "chart/templates/deployment.yaml").read_text(
        encoding="utf-8"
    )
    dockerfile = (ASSETS / "Dockerfile").read_text(encoding="utf-8")

    assert "containerPort: 8080" in values
    assert "path: /readyz" in deployment
    assert "path: /healthz" in deployment
    assert "EXPOSE 8080" in dockerfile
    assert 'ENTRYPOINT ["prometa-guardrail-service"]' in dockerfile


def test_the_guardrail_deploy_assets_follow_the_package_version() -> None:
    version = _project_version()
    chart = (ASSETS / "chart/Chart.yaml").read_text(encoding="utf-8")
    dockerfile = (ASSETS / "Dockerfile").read_text(encoding="utf-8")
    ubi = (ASSETS / "Dockerfile.ubi").read_text(encoding="utf-8")

    assert 'appVersion: "%s"' % version in chart
    assert "IMAGE_VERSION=%s" % version in dockerfile
    assert "IMAGE_VERSION=%s" % version in ubi


def test_the_service_image_never_installs_a_control_plane_client() -> None:
    dockerfile = (ASSETS / "Dockerfile").read_text(encoding="utf-8")
    ubi = (ASSETS / "Dockerfile.ubi").read_text(encoding="utf-8")

    for text in (dockerfile, ubi):
        assert "[guardrail-service]" in text
        assert "runtime-host" not in text
        assert "psycopg" not in text
