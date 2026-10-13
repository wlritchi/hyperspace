# syntax=docker/dockerfile:1
#
# The Hyperspace server image. The server is a PEP 723 uv script; its
# dependencies come from the committed lock (server/hyperspace-server.py.lock)
# into one venv, so the running container never reaches a package index.
#
# The admin scripts (scripts/) are in the image too, so that admin work is
# `kubectl exec <pod> -- python /app/hyperspace/scripts/<script>.py ...`.
#
# CI builds and pushes releases (.forgejo/workflows/build.yaml). To build by
# hand, from the repository root:
#   DOCKER_BUILDKIT=1 docker build -t hyperspace:<tag> \
#     --build-arg VERSION=<version> \
#     --build-arg VCS_REF="$(git rev-parse --short HEAD)" \
#     --build-arg BUILD_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)" .

FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS build
WORKDIR /build
ENV UV_LINK_MODE=copy UV_COMPILE_BYTECODE=1
# The lock refers to ../protocol relative to the script, so run from server/
# and copy the protocol package beside it. --frozen fails the build if the
# lock is missing or stale. --no-editable installs a copy of the protocol.
COPY protocol/ ./protocol/
COPY server/hyperspace-server.py server/hyperspace-server.py.lock ./server/
WORKDIR /build/server
RUN --mount=type=cache,target=/root/.cache/uv \
    uv export --script hyperspace-server.py --frozen --no-editable -o requirements.txt && \
    uv venv /opt/venv && \
    uv pip install --python /opt/venv -r requirements.txt

FROM python:3.12-slim-bookworm AS runtime
ARG VERSION=0.0.0
ARG VCS_REF=unknown
ARG BUILD_DATE=unknown
LABEL org.opencontainers.image.title="hyperspace" \
      org.opencontainers.image.description="Append-only backup store for coding-agent transcripts, with session teleport" \
      org.opencontainers.image.source="https://github.com/wlritchi/hyperspace" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.created="${BUILD_DATE}"
RUN export DEBIAN_FRONTEND=noninteractive && \
    apt-get update && \
    apt-get install -y --no-install-recommends tini && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
# A fixed uid and gid, so that a Kubernetes PVC is writable with
# securityContext.fsGroup: 10001. USER is numeric, so that a runAsNonRoot
# admission check can verify it.
RUN groupadd -g 10001 app && useradd -u 10001 -g 10001 -M -s /usr/sbin/nologin app
COPY --from=build --chown=app:app /opt/venv /opt/venv
# Keep the server/, lib/, scripts/ layout: the scripts add ../lib to sys.path.
COPY --chown=app:app server/hyperspace-server.py /app/hyperspace/server/
COPY --chown=app:app lib/ /app/hyperspace/lib/
COPY --chown=app:app scripts/ /app/hyperspace/scripts/
RUN install -d -o app -g app /data
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    HYPERSPACE_LISTEN_HOST=0.0.0.0 \
    HYPERSPACE_PORT=8139 \
    HYPERSPACE_DATA_DIR=/data
USER 10001:10001
EXPOSE 8139
VOLUME ["/data"]
# Plain python: the dependencies are in the venv, so the PEP 723 header is
# not used at run time.
ENTRYPOINT ["tini", "--"]
CMD ["python", "/app/hyperspace/server/hyperspace-server.py"]
