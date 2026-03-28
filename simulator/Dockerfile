FROM python:3.13-slim AS builder
WORKDIR /app
RUN pip install uv
COPY pyproject.toml uv.lock ./
COPY simulator/pyproject.toml simulator/pyproject.toml
RUN uv sync --frozen --no-dev --no-editable --package inandout-simulator

FROM python:3.13-slim AS runtime
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY simulator/src/ src/
ENV PATH="/app/.venv/bin:$PATH"
EXPOSE 6100
CMD ["inandout-simulator", "--listen", "0.0.0.0:6100"]
