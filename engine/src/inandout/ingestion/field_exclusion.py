"""Field exclusion utility for glob-style pattern matching (T1 #21)."""
from __future__ import annotations

import fnmatch
from typing import Any


def apply_field_exclusions(
    record: dict[str, Any],
    exclude_patterns: list[str],
) -> dict[str, Any]:
    """
    Remove fields from a record that match glob-style exclusion patterns.
    
    Patterns support standard glob syntax:
    - * matches any sequence of characters
    - ? matches any single character
    - [seq] matches any character in seq
    - [!seq] matches any character not in seq
    
    Examples:
    - "*.internal_*" excludes "user.internal_id", "order.internal_status"
    - "_*" excludes all fields starting with underscore
    - "*_temp" excludes all fields ending with "_temp"
    
    Args:
        record: Dictionary to filter
        exclude_patterns: List of glob patterns to exclude
    
    Returns:
        Filtered dictionary with matching fields removed
    """
    if not exclude_patterns:
        return record
    
    filtered: dict[str, Any] = {}
    
    for key, value in record.items():
        # Check if key matches any exclusion pattern
        excluded = False
        for pattern in exclude_patterns:
            if fnmatch.fnmatch(key, pattern):
                excluded = True
                break
        
        if not excluded:
            # For nested dicts, recursively apply exclusions
            if isinstance(value, dict):
                filtered[key] = apply_field_exclusions(value, exclude_patterns)
            else:
                filtered[key] = value
    
    return filtered
