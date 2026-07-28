"""Compatibilidade: coletor 1m removido — alias para o coletor diario (D1).

Evita crash no deploy se app.py antigo ainda importar collector_1m.
"""

from bot.ohlc_collector_1d import collector_1d

collector_1m = collector_1d
