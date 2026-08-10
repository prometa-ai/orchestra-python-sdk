"""Cross-repository model invocation identity v2 contract lock."""

from __future__ import annotations

import hashlib
import json
import re
from itertools import product
from pathlib import Path


FIXTURE = Path(__file__).parent / "fixtures" / "prometa-model-usage-v2.schema.json"
EXPECTED_SHA256 = "845f830df424f1626717e60a5dbd05e01187f84e2e96223527cceda521f3d55a"


def test_model_invocation_identity_v2_fixture_checksum_and_wire_names() -> None:
    content = FIXTURE.read_bytes()
    assert hashlib.sha256(content).hexdigest() == EXPECTED_SHA256

    contract = json.loads(content)
    assert contract["x-prometa-identity-order"] == [
        "usage_record_id",
        "engine_request_id",
        "runtime_request_id",
        "model_invocation_id",
        "model_attempt_id",
    ]
    mapping = contract["x-prometa-header-mapping"]
    assert mapping["inbound-x-request-id-alias"] is None
    assert mapping["request"] == {
        "x-orchestra-runtime-request-id": "runtime_request_id",
        "x-orchestra-model-invocation-id": "model_invocation_id",
        "x-orchestra-model-attempt-id": "model_attempt_id",
    }
    assert mapping["response"] == {
        "x-request-id": "engine_request_id",
        "x-orchestra-usage-record-id": "usage_record_id",
    }
    external_identity = contract["$defs"]["externalIdentity"]
    assert external_identity == {
        "type": "string",
        "minLength": 1,
        "maxLength": 256,
        "pattern": (
            "^(?!(?:[Nn][Uu][Ll][Ll]|[Nn][Oo][Nn][Ee]|[Nn][Ii][Ll]|"
            "[Uu][Nn][Dd][Ee][Ff][Ii][Nn][Ee][Dd])$)[!-~]+$"
        ),
    }
    external_pattern = re.compile(external_identity["pattern"])
    for sentinel in ("null", "none", "nil", "undefined"):
        for characters in product(
            *((character.lower(), character.upper()) for character in sentinel)
        ):
            assert external_pattern.fullmatch("".join(characters)) is None
    for ordinary in ("!", "~" * 256, "nullx", "none-", "nil0", "undefined_"):
        assert external_pattern.fullmatch(ordinary) is not None
    assert contract["properties"]["engine_request_id"]["pattern"] == (
        "^req_[0-9a-f]{32}$"
    )
    assert contract["properties"]["usage_record_id"]["pattern"] == (
        "^usage_[0-9a-f]{32}$"
    )
    assert contract["properties"]["fallback_from_model"]["type"] == [
        "string",
        "null",
    ]
    assert contract["additionalProperties"] is False
    assert contract["x-prometa-delivery"] == {
        "mode": "best-effort-buffered",
        "dedupeField": "usage_record_id",
        "redeliveryWindow": {
            "condition": "drain-cancelled-after-sink-acceptance-uncertain",
            "retainsDedupeValue": True,
        },
    }
    assert len(contract["allOf"]) == 2
    attempt_dependency = contract["allOf"][1]["then"]["properties"]
    assert attempt_dependency["runtime_request_id"] == {"type": "string"}
    assert attempt_dependency["model_invocation_id"] == {"type": "string"}
