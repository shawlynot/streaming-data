FROM ghcr.io/astral-sh/uv:python3.14-trixie-slim

LABEL org.opencontainers.image.source=https://github.com/shawlynot/streaming-data

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv python install 3.14
RUN uv sync --locked --no-install-project
ENV PATH="/app/.venv/bin:$PATH"

COPY src/ ./src/

WORKDIR /app/src