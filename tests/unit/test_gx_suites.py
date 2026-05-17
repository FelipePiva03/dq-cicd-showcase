"""Unit tests for GX suites — every expectation must declare severity."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.dq.great_expectations.run_checkpoint import FACT_TABLES, load_suite


SUITE_DIR = Path("src/dq/great_expectations/expectations")
VALID_SEVERITIES = {"critical", "warning"}


@pytest.mark.parametrize("table", FACT_TABLES)
def test_each_fact_has_a_suite_file(table):
    path = SUITE_DIR / f"silver_fact_{table}_suite.json"
    assert path.exists(), f"missing suite file for {table}: {path}"


@pytest.mark.parametrize("table", FACT_TABLES)
def test_suite_loads_into_gx_object(table):
    suite = load_suite(table)
    assert suite.name == f"silver_fact_{table}_suite"
    assert len(suite.expectations) >= 1


@pytest.mark.parametrize("table", FACT_TABLES)
def test_every_expectation_declares_severity(table):
    """Every expectation must have meta.severity set, and the value must be
    a known severity level. Untagged expectations would default to warning
    silently — explicit > implicit."""
    path = SUITE_DIR / f"silver_fact_{table}_suite.json"
    data = json.loads(path.read_text(encoding="utf-8"))

    for i, exp in enumerate(data["expectations"]):
        meta = exp.get("meta", {})
        assert "severity" in meta, (
            f"{table}[{i}] {exp['type']} is missing meta.severity"
        )
        assert meta["severity"] in VALID_SEVERITIES, (
            f"{table}[{i}] severity={meta['severity']!r} — must be one of {VALID_SEVERITIES}"
        )


@pytest.mark.parametrize("table", FACT_TABLES)
def test_every_expectation_has_rationale(table):
    """meta.rationale is required — forces the author to explain WHY a
    rule earns critical or warning. Self-documenting suites."""
    path = SUITE_DIR / f"silver_fact_{table}_suite.json"
    data = json.loads(path.read_text(encoding="utf-8"))

    for i, exp in enumerate(data["expectations"]):
        rationale = exp.get("meta", {}).get("rationale", "")
        assert rationale.strip(), (
            f"{table}[{i}] {exp['type']} is missing meta.rationale"
        )
