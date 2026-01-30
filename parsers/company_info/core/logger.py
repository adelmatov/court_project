"""
Logging configuration
"""

import logging
import sys
from typing import Optional

from .config import LOG_CONFIG


def setup_logger(
    name: str,
    log_file: Optional[str] = None,
    level: str = 'INFO'
) -> logging.Logger:
    """
    Setup logger with console handler only.
    
    File logging is handled by orchestrator.
    
    Args:
        name: Logger name
        log_file: Ignored (kept for backwards compatibility)
        level: Log level (DEBUG, INFO, WARNING, ERROR)
    
    Returns:
        Configured logger instance
    """
    
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))
    
    # Remove existing handlers
    logger.handlers.clear()
    logger.propagate = False
    
    # Console handler only
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, level.upper()))
    console_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    return logger


# Default logger (console only)
logger = setup_logger(
    'company_parser',
    level=LOG_CONFIG['level']
)