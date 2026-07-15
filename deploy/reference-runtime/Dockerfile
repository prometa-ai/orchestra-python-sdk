ARG PYTHON_IMAGE=python:3.12-slim
FROM ${PYTHON_IMAGE} AS builder

WORKDIR /src
COPY pyproject.toml README.md ./
COPY prometa ./prometa
RUN python -m pip wheel --no-cache-dir --wheel-dir /wheels ".[runtime-host,runtime-mcp]"

FROM ${PYTHON_IMAGE}

LABEL org.opencontainers.image.title="Prometa reference tenant runtime" \
      org.opencontainers.image.description="Tenant-deployed host for signed Orchestra runtime releases" \
      org.opencontainers.image.version="0.18.0"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080 \
    PROMETA_RUNTIME_CONFIG=/etc/prometa-runtime/config.json

RUN groupadd --gid 10001 runtime \
    && useradd --uid 10001 --gid runtime --create-home --shell /usr/sbin/nologin runtime
COPY --from=builder /wheels /wheels
RUN python -m pip install --no-cache-dir --no-index --find-links=/wheels \
      "prometa-sdk[runtime-host,runtime-mcp]==0.18.0" \
    && rm -rf /wheels

USER 10001:10001
EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
  CMD ["python", "-c", "import os,urllib.request; urllib.request.urlopen('http://127.0.0.1:%s/healthz' % os.environ.get('PORT','8080'), timeout=2).read()"]

ENTRYPOINT ["prometa-runtime-host"]
