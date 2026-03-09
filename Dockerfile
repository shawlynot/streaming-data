FROM ghcr.io/astral-sh/uv:python3.14-trixie-slim

LABEL org.opencontainers.image.source=https://github.com/shawlynot/streaming-data

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends g++ cmake libboost-dev && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock ./
COPY CMakeLists.txt ./
ENV PATH="/app/.venv/bin:$PATH"
COPY src/ ./src/
RUN uv sync --locked
