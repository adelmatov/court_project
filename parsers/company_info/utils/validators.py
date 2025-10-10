"""
Validation utilities
"""

import re
from typing import Optional


BIN_PATTERN = re.compile(r'^[0-9]{12}$')


def validate_bin(bin_value: Optional[str]) -> bool:
    """
    Validate BIN format.
    
    Args:
        bin_value: BIN string to validate
    
    Returns:
        True if valid, False otherwise
    """
    if not bin_value:
        return False
    
    return bool(BIN_PATTERN.match(bin_value))


def normalize_bin(bin_value: str) -> str:
    """
    Normalize BIN (strip whitespace).
    
    Args:
        bin_value: BIN string
    
    Returns:
        Normalized BIN
    """
    return bin_value.strip() if bin_value else ''