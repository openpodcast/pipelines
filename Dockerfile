# syntax=docker/dockerfile:1
# Pin to linux/amd64 as we deploy to x86_64 servers
FROM --platform=linux/amd64 python:3.11-slim-bullseye

ARG COMMIT_SHA
ENV COMMIT_SHA=${COMMIT_SHA}

# Install cron and gpg for decrypting data fetched from db
RUN apt-get update \
    && apt-get install -y --no-install-recommends cron gpg \
    && rm -rf /var/lib/apt/lists/*

# Install uv (pinned)
COPY --from=astral/uv:0.8.24 /uv /uvx /usr/local/bin/

# uv settings:
# - copy installed files (compatible with COPY --from layouts)
# - don't try to download a Python (we already have 3.11 from the base image)
# - install dependencies into /usr/local instead of a project-local .venv,
#   so the per-source pipeline subprocesses (which are launched with plain
#   `python -m job`) pick them up without needing a venv activation.
ENV UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/usr/local

WORKDIR /app

# Install Python deps first for layer caching: only the lock + project
# metadata, without the project source.
COPY connector_manager/pyproject.toml \
     connector_manager/uv.lock \
     connector_manager/README.md \
     ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

# Now copy the application code. The per-source pipeline directories are
# launched as subprocesses by manager.worker against the same Python
# interpreter, so all connectors are installed via
# connector_manager/pyproject.toml above.
COPY connector_manager/manager manager
COPY spotify spotify
COPY apple apple
COPY anchor anchor
COPY podigee podigee

COPY entrypoint.sh entrypoint.sh
RUN chmod +x ./entrypoint.sh

CMD ["/app/entrypoint.sh"]
