"""HTTP binding for ``orchestra-guardrail-evaluate-v1``.

``HttpGuardEvaluator`` is a ``GuardEvaluator`` over the wire contract. It is the
whole of the ``prometa-sdk[guardrail-client]`` surface together with
``prometa.guardrail.contract``: no kernel construction, no detector pack, no
third-party dependency.
"""

from __future__ import annotations

import asyncio
import json
import socket
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Dict,
    Mapping,
    Optional,
    Protocol,
    Sequence,
)

from ..runtime.kernel import GuardDecision, GuardRequest
from .contract import (
    DEFAULT_STAGE_BUDGET_MS,
    DEFAULT_TRANSPORT_SLACK_MS,
    GUARDRAIL_EVALUATE_PATH,
    GUARDRAIL_SPAN_NAME,
    HTTP_STATUS_REASON_CODES,
    REASON_CODE_COVERAGE_EMPTY,
    REASON_CODE_RESPONSE_INVALID,
    REASON_CODE_TIMEOUT,
    REASON_CODE_UNAVAILABLE,
    GuardrailContractError,
    GuardrailSpanRecorder,
    GuardrailSubject,
    decode_evaluate_response,
    encode_evaluate_request,
    guardrail_span_attributes,
    wire_stage,
)
from ..runtime.admission import RuntimeGuardrail
from .failmode import (
    FailOpenBudget,
    FailOpenObserver,
    GuardrailUnavailableError,
    apply_fail_mode,
    assert_workload_surface_fail_mode,
)
from .profiles import GuardrailProfile, assert_fail_open_permitted


MAX_RESPONSE_BYTES = 4 * 1024 * 1024


@dataclass(frozen=True)
class GuardrailTransportResult:
    status: int
    body: Any
    reason_code: Optional[str] = None
    headers: Mapping[str, str] = field(default_factory=dict)

    def header(self, name: str) -> Optional[str]:
        """Case-insensitive lookup; HTTP header names are not case-sensitive."""

        lowered = name.lower()
        for key, value in self.headers.items():
            if key.lower() == lowered:
                return value
        return None


class GuardrailTransport(Protocol):
    """Narrow seam so the conformance suite never needs a socket."""

    def request(
        self,
        method: str,
        path: str,
        body: Optional[bytes],
        headers: Mapping[str, str],
        timeout: float,
    ) -> GuardrailTransportResult:
        """Perform one request and return its status and decoded body."""


class UrllibGuardrailTransport:
    """Stdlib transport, matching the SDK's other outbound HTTP clients."""

    def __init__(self, base_url: str) -> None:
        if not base_url:
            raise ValueError("base_url is required")
        self._base_url = base_url.rstrip("/")

    def request(
        self,
        method: str,
        path: str,
        body: Optional[bytes],
        headers: Mapping[str, str],
        timeout: float,
    ) -> GuardrailTransportResult:
        url = self._base_url + path
        request = urllib.request.Request(
            url, data=body, method=method, headers=dict(headers)
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                payload = response.read(MAX_RESPONSE_BYTES + 1)
                if len(payload) > MAX_RESPONSE_BYTES:
                    return GuardrailTransportResult(
                        status=0, body=None, reason_code=REASON_CODE_RESPONSE_INVALID
                    )
                return GuardrailTransportResult(
                    status=response.status,
                    body=_decode_json(payload),
                    headers=_header_map(response.headers),
                )
        except urllib.error.HTTPError as error:
            payload = error.read(MAX_RESPONSE_BYTES)
            return GuardrailTransportResult(
                status=error.code,
                body=_decode_json(payload),
                headers=_header_map(error.headers),
            )
        except (TimeoutError, socket.timeout):
            # ``socket.timeout`` only became an alias of ``TimeoutError`` in
            # 3.10. On 3.9 it is a sibling under ``OSError``, so catching
            # ``TimeoutError`` alone lets a read deadline fall through to the
            # arm below and report an exhausted budget as an unreachable
            # service — a distinction the fail-open budget is keyed on.
            return GuardrailTransportResult(
                status=0, body=None, reason_code=REASON_CODE_TIMEOUT
            )
        except urllib.error.URLError as error:
            # A deadline struck while connecting is wrapped rather than raised.
            return GuardrailTransportResult(
                status=0,
                body=None,
                reason_code=(
                    REASON_CODE_TIMEOUT
                    if isinstance(error.reason, socket.timeout)
                    else REASON_CODE_UNAVAILABLE
                ),
            )
        except (OSError, ValueError):
            return GuardrailTransportResult(
                status=0, body=None, reason_code=REASON_CODE_UNAVAILABLE
            )


def _decode_json(payload: bytes) -> Any:
    try:
        return json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return None


def _header_map(headers: Any) -> Dict[str, str]:
    try:
        return {str(key).lower(): str(value) for key, value in headers.items()}
    except Exception:
        return {}


def _retry_after_seconds(result: GuardrailTransportResult) -> Optional[float]:
    """Read ``Retry-After``; ``None`` when the server gave no hint.

    An unparseable value resolves to ``inf`` rather than ``None`` so it
    suppresses the retry: the server asked for a wait of unknown length, and
    guessing "no wait" would turn its back-pressure signal into more load.
    """

    raw = result.header("retry-after")
    if raw is None:
        return None
    try:
        seconds = float(raw.strip())
    except (AttributeError, ValueError):
        return float("inf")
    return seconds if seconds >= 0.0 else float("inf")


def _reason_code_for(result: GuardrailTransportResult) -> str:
    if result.reason_code is not None:
        return result.reason_code
    body = result.body
    if isinstance(body, Mapping):
        error = body.get("error")
        if isinstance(error, Mapping) and isinstance(error.get("code"), str):
            return error["code"]
    return HTTP_STATUS_REASON_CODES.get(result.status, REASON_CODE_UNAVAILABLE)


class HttpGuardEvaluator:
    """``GuardEvaluator`` over ``POST /v1/guardrail:evaluate``."""

    def __init__(
        self,
        transport: GuardrailTransport,
        *,
        api_key: str,
        profile: GuardrailProfile,
        subject: GuardrailSubject,
        budget_ms: Optional[Mapping[str, int]] = None,
        transport_slack_ms: int = DEFAULT_TRANSPORT_SLACK_MS,
        observer: Optional[FailOpenObserver] = None,
        span_recorder: Optional[GuardrailSpanRecorder] = None,
        workload_surface: str = "",
        guardrails: Sequence[RuntimeGuardrail] = (),
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if not api_key:
            raise ValueError("api_key is required")
        assert_workload_surface_fail_mode(profile, workload_surface)
        # F4 refuses fail-open beside anything that can enforce. ``guardrails``
        # is what the caller declares it will send, and checking it here is the
        # earliest refusal available — but the list that is actually evaluated
        # rides on each request, so ``evaluate_sync`` checks that one too. This
        # constructor argument is a pre-traffic declaration, not the source of
        # truth for any request.
        assert_fail_open_permitted(profile.fail_mode, guardrails)
        self._transport = transport
        self._api_key = api_key
        self._profile = profile
        self._subject = subject
        self._budget_ms = dict(budget_ms or DEFAULT_STAGE_BUDGET_MS)
        self._transport_slack_ms = transport_slack_ms
        self._observer = observer
        self._span_recorder = span_recorder
        self._workload_surface = workload_surface
        self._monotonic = monotonic
        self._sleep = sleep
        self._fail_open_budget = FailOpenBudget(
            profile.fail_open_max_consecutive,
            profile.fail_open_window_seconds,
            monotonic=monotonic,
        )

    @property
    def fail_open_budget(self) -> FailOpenBudget:
        return self._fail_open_budget

    def _headers(self) -> Dict[str, str]:
        return {
            "authorization": "Bearer " + self._api_key,
            "content-type": "application/json",
            "accept": "application/json",
        }

    async def evaluate(self, request: GuardRequest) -> GuardDecision:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.evaluate_sync, request)

    def evaluate_sync(self, request: GuardRequest) -> GuardDecision:
        # The guardrails on the request are the ones this call will have
        # evaluated, so F4 is re-checked against them rather than against what
        # the constructor was told. Refusing here rather than at the fail point
        # means the pairing cannot run healthy for months and reveal itself the
        # first time the service is unreachable.
        assert_fail_open_permitted(self._profile.fail_mode, request.guardrails)
        stage = wire_stage(request.stage)
        budget_ms = self._budget_ms.get(stage, DEFAULT_STAGE_BUDGET_MS[stage])
        document = encode_evaluate_request(
            request,
            profile=self._profile.profile_id,
            subject=self._subject,
            budget_ms=budget_ms,
        )
        body = json.dumps(
            document, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode("utf-8")
        deadline_ms = budget_ms + self._transport_slack_ms
        started = self._monotonic()
        result = self._attempt(body, deadline_ms / 1000.0)
        delay_ms = self._retry_delay_ms(result, started, deadline_ms, budget_ms)
        if delay_ms is not None:
            if delay_ms > 0.0:
                self._sleep(delay_ms / 1000.0)
            remaining_ms = deadline_ms - (self._monotonic() - started) * 1000.0
            result = self._attempt(body, max(remaining_ms, 1.0) / 1000.0)
        if result.status != 200:
            return self._fail(request, stage, _reason_code_for(result))
        try:
            decoded = decode_evaluate_response(
                result.body, request_id=request.request_id
            )
        except GuardrailContractError as error:
            return self._fail(request, stage, error.code)
        self._record_span(decoded, stage)
        if not decoded.decision.evaluated_guardrails:
            # "Nothing was evaluated" is not "I checked and found nothing", so
            # it resolves through the fail mode and never reaches
            # ``record_success``: a service that answers 200 while evaluating
            # nothing must not be able to refill the fail-open budget.
            return self._fail(request, stage, REASON_CODE_COVERAGE_EMPTY)
        self._fail_open_budget.record_success()
        return decoded.decision

    def _attempt(self, body: bytes, timeout: float) -> GuardrailTransportResult:
        """One transport call; a fault in the transport is an unusable verdict.

        A transport that raises is indistinguishable from one that is down, so
        it must reach the fail mode. Letting the exception escape would not be
        an allow — both SDK enforcement points turn an unexpected exception
        into ``guard_evaluation_failed`` — but it would settle the request
        outside the profile: no fail-open allowance spent, no observer record,
        and a non-SDK caller handed a transport error in place of a verdict.
        """

        try:
            return self._transport.request(
                "POST", GUARDRAIL_EVALUATE_PATH, body, self._headers(), timeout=timeout
            )
        except Exception:
            return GuardrailTransportResult(
                status=0, body=None, reason_code=REASON_CODE_UNAVAILABLE
            )

    def _record_span(self, decoded, stage: str) -> None:
        if self._span_recorder is None:
            return
        try:
            self._span_recorder.record(
                GUARDRAIL_SPAN_NAME,
                guardrail_span_attributes(
                    decoded, stage=stage, profile=self._profile.profile_id
                ),
            )
        except Exception:
            # Evidence is not a gate on enforcement (E5).
            pass

    def _retry_delay_ms(
        self,
        result: GuardrailTransportResult,
        started: float,
        deadline_ms: float,
        budget_ms: int,
    ) -> Optional[float]:
        """How long to wait before the single permitted retry, or ``None``.

        Retries only what a second attempt can plausibly fix, and only once. A
        500 is not retried in band: a crashing detector crashes again, and the
        second crash costs the caller latency it does not have.

        ``Retry-After`` is honoured rather than ignored, which under these
        deadlines usually means the retry is abandoned: a server asking for a
        one-second wait inside a 50 ms budget is telling the caller to fail
        through the profile fail mode, not to retry immediately and add load.
        """

        retryable = result.status == 429 or (
            result.status == 0 and result.reason_code == REASON_CODE_UNAVAILABLE
        )
        if not retryable:
            return None
        remaining_ms = deadline_ms - (self._monotonic() - started) * 1000.0
        if remaining_ms <= budget_ms / 2.0:
            return None
        retry_after = _retry_after_seconds(result)
        if retry_after is None:
            return 0.0
        delay_ms = retry_after * 1000.0
        if delay_ms >= remaining_ms - budget_ms / 2.0:
            return None
        return delay_ms

    def _fail(
        self, request: GuardRequest, stage: str, reason_code: str
    ) -> GuardDecision:
        outcome = apply_fail_mode(
            self._profile,
            request_id=request.request_id,
            wire_stage_name=stage,
            guardrails=request.guardrails,
            tool=request.tool,
            tenant=self._subject.tenant,
            reason_code=reason_code,
            budget=self._fail_open_budget,
            observer=self._observer,
            workload_surface=self._workload_surface,
        )
        if outcome.decision is None:
            raise GuardrailUnavailableError(outcome.reason_code)
        return outcome.decision


__all__ = [
    "MAX_RESPONSE_BYTES",
    "GuardrailTransport",
    "GuardrailTransportResult",
    "HttpGuardEvaluator",
    "UrllibGuardrailTransport",
]
