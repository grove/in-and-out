"""Duration string parser. Converts '30s', '5m', '1h', '90d' → float seconds."""
from __future__ import annotations
import re

_PATTERN = re.compile(r'^(\d+(?:\.\d+)?)\s*([smhd])$')
_MULTIPLIERS = {'s': 1.0, 'm': 60.0, 'h': 3600.0, 'd': 86400.0}

def parse_duration(s: str) -> float:
    m = _PATTERN.match(s.strip())
    if not m:
        raise ValueError(f"Invalid duration string: {s!r}. Expected e.g. '30s', '5m', '1h'.")
    return float(m.group(1)) * _MULTIPLIERS[m.group(2)]
