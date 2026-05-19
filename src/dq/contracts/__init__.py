"""
Data contracts — single source of truth for schema + DQ rules per table.

Layout:
    contracts/
        silver/
            fact_*.yml      ← consumed by conform_fact + GX gate
            dim_*.yml       ← consumed by conform_dim + GX gate
        gold/
            *.yml           ← consumed by GX gate

Public API:
    load_contract(layer, table)       — parse one contract file
    list_contracts(layer)             — discover all contracts under a layer
    to_hard_rules_case(contract)      — CASE expr for split_quarantine()
    to_gx_suite(contract)             — GX 1.x ExpectationSuite (in-memory)

Soda Core support was removed after empirical evidence that
soda-core-spark-df 3.3.7 and 3.5.6 both fail on Databricks serverless
(client="2") with distinct Spark Connect / session-detach errors. GX 1.x
works cleanly across silver facts, silver dims, and gold marts, so the
single-runner approach won out.
"""

from .loader import Contract, list_contracts, load_contract
from .to_gx import to_gx_suite
from .to_hard_rules import to_hard_rules_case

__all__ = [
    "Contract",
    "list_contracts",
    "load_contract",
    "to_gx_suite",
    "to_hard_rules_case",
]
