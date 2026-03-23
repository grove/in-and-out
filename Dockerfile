FROM python:3.13-slim AS builder
WORKDIR /app
RUN pip install uv
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-editable

FROM python:3.13-slim AS runtime
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY src/ src/
COPY migrations/ migrations/
COPY alembic.ini .
ENV PATH="/app/.venv/bin:$PATH"
EXPOSE 9090
CMD ["inandout", "ingest", "run", "--config", "/config/ingestion.yaml"]
