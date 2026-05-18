"""Contract → in-memory GX 1.x ExpectationSuite."""

from __future__ import annotations

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.core.expectation_suite import ExpectationSuite

from .loader import Contract


def to_gx_suite(contract: Contract) -> ExpectationSuite:
    """Generate a GX 1.x ExpectationSuite from a contract's expectations.

    The suite name follows `{catalog_schema}_{table}_suite` so it's
    self-describing in the GX context. `meta.severity` and `meta.rationale`
    are preserved on every expectation so the tiered runner can read them.

    GX 1.x requires an active context before instantiating Expectations;
    callers must ensure one exists (the runner does, via gx.get_context).
    """
    # Ensure a GX context exists (idempotent — returns the active one if any).
    gx.get_context(mode="ephemeral")

    suite_name = f"{contract.catalog_schema}_{contract.table}_suite"
    suite = ExpectationSuite(
        name=suite_name,
        meta={"source_contract": f"contracts/{contract.catalog_schema}/{contract.table}.yml"},
    )

    for exp in contract.expectations:
        cls = getattr(gxe, exp.type, None)
        if cls is None:
            raise ValueError(
                f"unknown GX expectation class: {exp.type!r} "
                f"(in contract {contract.catalog_schema}/{contract.table}, id={exp.id!r})"
            )
        expectation = cls(
            **exp.kwargs,
            meta={"severity": exp.severity, "rationale": exp.rationale, "id": exp.id},
        )
        suite.add_expectation(expectation)

    return suite
