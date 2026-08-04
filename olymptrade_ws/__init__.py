# olymptrade_ws/__init__.py
# Export minimo para evitar import circular (client → api → package).
from .core.client import OlympTradeClient

__all__ = ["OlympTradeClient"]
