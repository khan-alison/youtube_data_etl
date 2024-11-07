from typing import Dict, Any, Callable
from helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)


class TransformationRegistry:
    """Registry for transformation functions."""
    _transformations: Dict[str, Callable] = {}

    @classmethod
    def register(cls, name: str = None):
        """Decorator to register a transformation function."""
        def decorator(func: Callable):
            nonlocal name
            if name is None:
                name = func.__name__
            cls._transformations[name] = func
            logger.info(f"Registered transformation: {name}")
            return func
        return decorator

    @classmethod
    def get_transformation(cls, name: str) -> Callable:
        """Get a transformation function by name"""
        if name not in cls._transformations:
            raise ValueError(
                f"Transformation function '{name}' not found from registry.")
        return cls._transformations[name]

    @classmethod
    def list_transformations(cls) -> list:
        """List all registered transformation functions"""
        return list(cls._transformations.keys())
