# olymptrade_ws/__init__.py
# Cliente WS nao oficial (vendorizado). Import direto do core — evita side-effects de main.py.
from .core.client import OlympTradeClient
from .api.balance import BalanceAPI
from .api.market import MarketAPI
from .api.trade import TradeAPI

__all__ = [
    "OlympTradeClient",
    "BalanceAPI",
    "MarketAPI",
    "TradeAPI",
]
