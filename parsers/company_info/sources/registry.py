"""
Source Registry - Register and retrieve BIN sources
"""

from typing import Dict, Type, Optional

from .base import BinSource
from .qamqor_source import QamqorBinSource
from ..core.logger import logger


class SourceRegistry:
    """
    Registry for BIN sources.
    
    Usage:
        registry = SourceRegistry()
        source = registry.get('qamqor')
        bins = source.get_bins()
    """
    
    def __init__(self):
        self._sources: Dict[str, Type[BinSource]] = {}
        self._register_default_sources()
    
    def _register_default_sources(self):
        """Register built-in sources."""
        self.register('qamqor', QamqorBinSource)
    
    def register(self, name: str, source_class: Type[BinSource]):
        """
        Register a new BIN source.
        
        Args:
            name: Source identifier
            source_class: BinSource subclass
        """
        if not issubclass(source_class, BinSource):
            raise TypeError(f"{source_class} must inherit from BinSource")
        
        self._sources[name] = source_class
        logger.debug(f"Registered BIN source: {name}")
    
    def get(self, name: str) -> BinSource:
        """
        Get source instance by name.
        
        Args:
            name: Source identifier
        
        Returns:
            BinSource instance
        
        Raises:
            ValueError: If source not found
        """
        if name not in self._sources:
            available = ', '.join(self._sources.keys())
            raise ValueError(
                f"Unknown source '{name}'. "
                f"Available sources: {available}"
            )
        
        return self._sources[name]()
    
    def list_sources(self) -> list:
        """Get list of registered source names."""
        return list(self._sources.keys())


# Global registry instance
registry = SourceRegistry()