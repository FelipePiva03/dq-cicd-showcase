"""Contract → CASE expression for the quarantine split in conform pipelines."""

from __future__ import annotations

from .loader import Contract


def to_hard_rules_case(contract: Contract) -> str:
    """Builds the SQL CASE expression evaluated by split_quarantine().

    Returns the literal `'NULL'` string if the contract has no hard_rules —
    that's a valid Spark expression that evaluates to NULL for every row,
    so split_quarantine() puts all rows in `valid`. Tables without
    structural rules (notably gold marts) are expected to not have a
    quarantine flow at all, but this keeps the API uniform.
    """
    if not contract.hard_rules:
        return "CAST(NULL AS STRING)"

    whens = "\n".join(
        f"            WHEN {hr.condition} THEN '{hr.reason}'" for hr in contract.hard_rules
    )
    return f"""
        CASE
{whens}
            ELSE NULL
        END
    """
