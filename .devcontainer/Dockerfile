FROM mcr.microsoft.com/devcontainers/python:3.13

# Install uv (fast Python package manager)
RUN pip install --quiet uv

# Install just (task runner)
RUN curl -sSf https://just.systems/install.sh | bash -s -- --to /usr/local/bin
