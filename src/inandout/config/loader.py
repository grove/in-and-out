"""YAML connector config loader.

Loads a connector YAML file, parses it with PyYAML, and validates it
against the ConnectorFileConfig Pydantic model.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

import yaml
from pydantic import ValidationError

from inandout.config.connector import ConnectorFileConfig


def load_connector(path: str | Path) -> ConnectorFileConfig:
    """Load and validate a connector config file.

    Args:
        path: Path to the connector YAML file.

    Returns:
        Validated ConnectorFileConfig instance.

    Raises:
        FileNotFoundError: If the file does not exist.
        yaml.YAMLError: If the file is not valid YAML.
        ValidationError: If the config fails Pydantic validation.
    """
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    return ConnectorFileConfig.model_validate(raw)


def load_connector_from_string(content: str) -> ConnectorFileConfig:
    """Load and validate a connector config from a YAML string."""
    raw = yaml.safe_load(content)
    return ConnectorFileConfig.model_validate(raw)


_ENV_VAR_PATTERN = re.compile(r'\$\{([A-Z][A-Z0-9_]*)\}')


def _interpolate_env_vars(text: str) -> str:
    """Replace ${UPPER_CASE_VAR} patterns with values from os.environ.

    Non-uppercase patterns (like ${runtime.x}) are left unchanged.
    Raises EnvironmentError if a referenced env var is not set.
    """
    errors: list[str] = []

    def replacer(m: re.Match) -> str:  # type: ignore[type-arg]
        var_name = m.group(1)
        value = os.environ.get(var_name)
        if value is None:
            errors.append(var_name)
            return m.group(0)
        return value

    result = _ENV_VAR_PATTERN.sub(replacer, text)
    if errors:
        raise EnvironmentError(
            f"Environment variable(s) not set: {', '.join(errors)}"
        )
    return result


def load_ingestion_tool_config(path: str | Path) -> "IngestionToolConfig":
    """Load and validate an ingestion tool config file (ingestion.yaml)."""
    from inandout.config.tool import IngestionToolConfig

    text = Path(path).read_text(encoding="utf-8")
    text = _interpolate_env_vars(text)
    raw = yaml.safe_load(text)
    return IngestionToolConfig.model_validate(raw)


def load_writeback_tool_config(path: str | Path) -> "WritebackToolConfig":
    """Load and validate a writeback tool config file (writeback.yaml)."""
    from inandout.config.tool import WritebackToolConfig

    text = Path(path).read_text(encoding="utf-8")
    text = _interpolate_env_vars(text)
    raw = yaml.safe_load(text)
    return WritebackToolConfig.model_validate(raw)
