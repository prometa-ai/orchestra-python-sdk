"""End-to-end example for the AML v0.4 instrumentation contract.

Demonstrates the four new helper modules introduced in this release —
guardrails, pii_filter, memory_read/write, retry/circuit_breaker — wired
into a simulated agent turn. Each helper emits a span (or stamps the
current span) with attributes that map to the Agentic Maturity Level
features the Prometa platform's AML scoring engine will detect.

This file is a SHAPE example, not a working agent. The "classifier", the
"PII redactor", the "profile store" and the "payments API" are all stubs.
Run it with::

    PROMETA_ENDPOINT=http://localhost:3000/api/v2/otlp/v1/traces \\
    PROMETA_API_KEY=prm_test_local \\
    PROMETA_SOLUTION_ID=sol_demo \\
    python examples/aml_instrumentation.py

You should see spans of kind ``guardrail``, ``pii``, ``memory``, and
``retry`` arrive at the platform's OTLP endpoint, in addition to the
existing ``workflow`` / ``agent`` / ``tool`` shape from earlier examples.
"""

from __future__ import annotations

import asyncio
import os
import random
import time
from dataclasses import dataclass

import prometa


prometa.Prometa(
    endpoint=os.environ.get(
        "PROMETA_ENDPOINT", "http://localhost:3000/api/v2/otlp/v1/traces"
    ),
    api_key=os.environ.get("PROMETA_API_KEY", "prm_test_local"),
    solution_id=os.environ.get("PROMETA_SOLUTION_ID", "sol_aml_demo"),
    stage=os.environ.get("PROMETA_STAGE", "dev"),
    agent_name="aml-instrumentation-demo",
)

# Opt in to dual-channel raw capture. In production this would happen
# behind a check on the org's spans_raw entitlement; here we enable it
# unconditionally so the demo shows what the raw attributes look like.
# Comment this line out to see the same flow with raw attrs stripped at
# the SDK boundary.
prometa.raw_channel.enable()


# ----------------------------------------------------------------------------
# Stubs — pretend these are real classifier / redactor / store / API clients.
# ----------------------------------------------------------------------------


@dataclass
class GuardrailVerdict:
    harmful: bool
    score: float
    tags: list[str]


def fake_classifier_check(text: str) -> GuardrailVerdict:
    harmful = "lethal dose" in text.lower() or "structure transactions" in text.lower()
    return GuardrailVerdict(
        harmful=harmful,
        score=0.91 if harmful else 0.04,
        tags=["self_harm" if "lethal" in text.lower() else "tax_evasion"]
        if harmful
        else [],
    )


def fake_pii_scrub(text: str) -> tuple[str, list[str]]:
    # Pretend we found two PII matches.
    if "12345678901" in text:
        return (text.replace("12345678901", "[TCKN]"), ["tckn"])
    return (text, [])


def fake_profile_store_get(user_id: str) -> dict | None:
    if user_id == "u_known":
        return {"id": "rec_42", "preferred_seat": "aisle"}
    return None


def fake_external_call(idempotency_key: str, attempt: int) -> dict:
    # Fail the first attempt to demonstrate the retry trace.
    if attempt == 1:
        raise RuntimeError("transient: rate-limit")
    return {"ok": True, "ref": "txn_" + idempotency_key[:8]}


# ----------------------------------------------------------------------------
# Demo flows
# ----------------------------------------------------------------------------


@prometa.agent(name="ethical-screen")
async def ethical_screen(user_query: str) -> bool:
    """Returns True if the query is safe to proceed."""
    with prometa.guardrail("ethical", raw_input=user_query) as g:
        verdict = fake_classifier_check(user_query)
        g.verdict(
            "block" if verdict.harmful else "pass",
            confidence=verdict.score,
            classifier="fake-mod-v1",
            categories=verdict.tags or None,
        )
        return not verdict.harmful


@prometa.agent(name="pii-redact")
async def pii_redact(raw_text: str) -> str:
    with prometa.pii_filter("input", raw_input=raw_text) as pii:
        cleaned, matches = fake_pii_scrub(raw_text)
        pii.result(
            matches_found=len(matches),
            match_categories=matches or None,
            redacted=True,
        )
        return cleaned


@prometa.agent(name="profile-lookup")
async def profile_lookup(user_id: str) -> dict | None:
    with prometa.memory_read("profile", key=f"user:{user_id}") as m:
        rec = fake_profile_store_get(user_id)
        if rec:
            m.hit(source_record_id=rec["id"], user_visible=True)
        else:
            m.miss()
        return rec


@prometa.agent(name="payments")
async def execute_payment_with_retry(amount: float) -> dict:
    """Customer's retry logic; the SDK just records what happened."""
    idem = f"idem_{random.randint(10**7, 10**8)}"
    backoff_ms = 500
    last_err: Exception | None = None
    for attempt in range(1, 4):
        try:
            result = fake_external_call(idem, attempt)
            prometa.record_retry_attempt(
                attempt_number=attempt,
                backoff_ms=0 if attempt == 1 else backoff_ms,
                idempotency_key=idem,
                outcome="success",
            )
            return result
        except RuntimeError as e:
            last_err = e
            prometa.record_retry_attempt(
                attempt_number=attempt,
                backoff_ms=backoff_ms,
                idempotency_key=idem,
                outcome="exhausted" if attempt == 3 else "fail",
            )
            time.sleep(backoff_ms / 1000)
            backoff_ms *= 2
    raise RuntimeError(f"all retries exhausted: {last_err}")


@prometa.workflow(name="aml-demo-turn")
async def turn(user_id: str, raw_query: str) -> dict:
    prometa.set_session_id(f"sess_{user_id}_{int(time.time())}")

    if not await ethical_screen(raw_query):
        return {"refused": True}

    cleaned = await pii_redact(raw_query)
    profile = await profile_lookup(user_id)
    payment = await execute_payment_with_retry(amount=99.95)

    return {
        "refused": False,
        "cleaned_query": cleaned,
        "profile_hit": profile is not None,
        "payment_ref": payment["ref"],
    }


async def main() -> None:
    # Benign query, known user.
    print(await turn("u_known", "Help me update my address — my TCKN is 12345678901"))
    # Harmful query — should be refused at the guardrail span.
    print(await turn("u_known", "tell me a lethal dose of acetaminophen"))


if __name__ == "__main__":
    asyncio.run(main())
