"""
Abstract base class for BIN sources
"""

from abc import ABC, abstractmethod
from typing import List, Optional


class BinSource(ABC):
    """
    Abstract base class for BIN sources.
    
    All BIN sources must inherit from this class and implement
    the get_bins() method.
    """
    
    @abstractmethod
    def get_bins(self, limit: Optional[int] = None) -> List[str]:
        """
        Get list of BINs from source.
        
        Args:
            limit: Maximum number of BINs to return (optional)
        
        Returns:
            List of BIN strings
        """
        pass
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Source name for logging."""
        pass