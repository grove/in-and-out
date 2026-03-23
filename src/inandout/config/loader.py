"""YAML connector config loader.

Loads a connector YAML file, parses it with PyYAML, and validates it
against the ConnectorFileConfig Pydantic model.
"""

from __future__ import annotations

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
