ARG PYTHON_IMAGE=python:3.12-slim@sha256:57cd7c3a7a273101a6485ba99423ee568157882804b1124b4dd04266317710de
FROM ${PYTHON_IMAGE} AS builder

WORKDIR /src
COPY pyproject.toml README.md ./
COPY prometa ./prometa
RUN python -m pip wheel --no-cache-dir --wheel-dir /wheels ".[runtime-host,runtime-mcp]"

FROM ${PYTHON_IMAGE}

ARG VCS_REF=unknown
ARG IMAGE_VERSION=0.18.2

LABEL org.opencontainers.image.title="Prometa reference tenant runtime" \
      org.opencontainers.image.description="Tenant-deployed host for signed Orchestra runtime releases" \
      org.opencontainers.image.source="https://github.com/prometa-ai/orchestra-python-sdk" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.version="${IMAGE_VERSION}" \
      io.prometa.image.variant="debian"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080 \
    PROMETA_RUNTIME_CONFIG=/etc/prometa-runtime/config.json

RUN groupadd --gid 10001 runtime \
    && useradd --uid 10001 --gid runtime --create-home --shell /usr/sbin/nologin runtime
COPY --from=builder /wheels /wheels
RUN python -m pip install --no-cache-dir --no-index --find-links=/wheels \
      "prometa-sdk[runtime-host,runtime-mcp]==0.18.2" \
    && rm -rf /wheels

USER 10001:10001
EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
  CMD ["python", "-c", "import os,ssl,urllib.request;t=os.environ.get('PROMETA_RUNTIME_SERVER_TLS_CERT_FILE');c=ssl.create_default_context(cafile=os.environ.get('PROMETA_RUNTIME_PROBE_TLS_CA_FILE') or t) if t else None;t and setattr(c,'check_hostname',False);p=os.environ.get('PROMETA_RUNTIME_PROBE_TLS_CERT_FILE');p and c.load_cert_chain(p,os.environ['PROMETA_RUNTIME_PROBE_TLS_KEY_FILE']);urllib.request.urlopen(('%s://127.0.0.1:%s/healthz' % ('https' if t else 'http',os.environ.get('PORT','8080'))),timeout=2,context=c).read()"]

ENTRYPOINT ["prometa-runtime-host"]
