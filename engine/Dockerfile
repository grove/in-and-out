FROM python:3.13-slim AS builder
WORKDIR /app
RUN pip install uv
COPY pyproject.toml uv.lock ./
COPY engine/pyproject.toml engine/pyproject.toml
COPY engine/src/ engine/src/
RUN uv sync --frozen --no-dev --no-editable --package inandout

FROM python:3.13-slim AS runtime
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY engine/src/ src/
COPY engine/migrations/ migrations/
COPY engine/alembic.ini .
ENV PATH="/app/.venv/bin:$PATH"
EXPOSE 9090
CMD ["inandout", "ingest", "run", "--config", "/config/ingestion.yaml"]
