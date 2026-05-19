"""Contract → in-memory GX 1.x ExpectationSuite.

GX imports happen *inside* `to_gx_suite()` (not at module top) so that
non-GX consumers of `dq.contracts` (e.g. the silver pipelines, which use
`to_hard_rules_case`) don't need great_expectations installed in their
environment. This keeps the Databricks `default` env lean — only the
`gx` env carries the great_expectations dependency.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .loader import Contract

if TYPE_CHECKING:  # only for type hints — never imported at runtime
    from great_expectations.core.expectation_suite import ExpectationSuite


def to_gx_suite(contract: Contract) -> ExpectationSuite:
    """Generate a GX 1.x ExpectationSuite from a contract's expectations.

    The suite name follows `{catalog_schema}_{table}_suite` so it's
    self-describing in the GX context. `meta.severity` and `meta.rationale`
    are preserved on every expectation so the tiered runner can read them.

    GX 1.x requires an active context before instantiating Expectations;
    this function creates an ephemeral one if none is active.
    """
    # Lazy imports — only paid by callers that actually use this adapter.
    import great_expectations as gx
    import great_expectations.expectations as gxe
    from great_expectations.core.expectation_suite import ExpectationSuite

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
