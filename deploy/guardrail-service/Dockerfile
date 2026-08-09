ARG PYTHON_IMAGE=python:3.12-slim@sha256:57cd7c3a7a273101a6485ba99423ee568157882804b1124b4dd04266317710de
FROM ${PYTHON_IMAGE} AS builder

WORKDIR /src
COPY pyproject.toml README.md ./
COPY prometa ./prometa
RUN python -m pip wheel --no-cache-dir --wheel-dir /wheels ".[guardrail-service]"

FROM ${PYTHON_IMAGE}

ARG VCS_REF=unknown
ARG IMAGE_VERSION=0.20.1

LABEL org.opencontainers.image.title="Prometa reference guardrail service" \
      org.opencontainers.image.description="Tenant-deployed orchestra-guardrail-evaluate-v1 evaluator" \
      org.opencontainers.image.source="https://github.com/prometa-ai/orchestra-python-sdk" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.version="${IMAGE_VERSION}" \
      io.prometa.image.variant="debian"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080 \
    PROMETA_GUARDRAIL_CONFIG=/etc/prometa-guardrail/config.json

RUN groupadd --gid 10001 guardrail \
    && useradd --uid 10001 --gid guardrail --create-home --shell /usr/sbin/nologin guardrail
COPY --from=builder /wheels /wheels
RUN python -m pip install --no-cache-dir --no-index --find-links=/wheels \
      "prometa-sdk[guardrail-service]==0.20.1" \
    && rm -rf /wheels

USER 10001:10001
EXPOSE 8080

# The probe below is the container asking itself over loopback, which is why
# it works under a NetworkPolicy with empty egress: the evaluator resolves no
# names and opens no outbound connection, because the whole detector pack is
# compiled from the image and the profile document.
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
  CMD ["python", "-c", "import os,urllib.request;urllib.request.urlopen('http://127.0.0.1:%s/healthz' % os.environ.get('PORT','8080'),timeout=2).read()"]

ENTRYPOINT ["prometa-guardrail-service"]
