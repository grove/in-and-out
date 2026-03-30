FROM python:3.13-slim AS builder
WORKDIR /app
RUN pip install uv
COPY pyproject.toml uv.lock ./
COPY simulator/pyproject.toml simulator/pyproject.toml
COPY simulator/src/ simulator/src/
RUN uv sync --frozen --no-dev --no-editable --package inandout-simulator

FROM python:3.13-slim AS runtime
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY simulator/src/ src/
COPY schemas/ /schemas/
ENV PATH="/app/.venv/bin:$PATH"
ENV INANDOUT_SCHEMAS_DIR=/schemas
EXPOSE 6100
CMD ["inandout-simulator", "--listen", "0.0.0.0:6100"]
