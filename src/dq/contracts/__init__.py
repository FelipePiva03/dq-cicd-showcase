"""
Data contracts — single source of truth for schema + DQ rules per table.

Layout:
    contracts/
        silver/
            fact_*.yml      ← consumed by conform_fact + GX gate
            dim_*.yml       ← consumed by conform_dim + Soda gate
        gold/
            *.yml           ← consumed by Soda gate

Public API:
    load_contract(layer, table)       — parse one contract file
    list_contracts(layer)             — discover all contracts under a layer
    to_hard_rules_case(contract)      — CASE expr for split_quarantine()
    to_gx_suite(contract)             — GX 1.x ExpectationSuite (in-memory)
    to_soda_yaml(contracts)           — SodaCL YAML string (multi-table scan)
"""

from .loader import Contract, list_contracts, load_contract
from .to_gx import to_gx_suite
from .to_hard_rules import to_hard_rules_case
from .to_soda import to_soda_yaml

__all__ = [
    "Contract",
    "list_contracts",
    "load_contract",
    "to_gx_suite",
    "to_hard_rules_case",
    "to_soda_yaml",
]
