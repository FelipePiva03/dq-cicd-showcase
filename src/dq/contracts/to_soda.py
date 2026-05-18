"""Contract → SodaCL YAML string for soda-core scans.

The Soda runner registers each contract's `metadata.table` as a temp view of
the same name (e.g. `dim_customer`), so checks reference views by table name.

Severity mapping:
  - severity: critical → bare check (failing the scan)
  - severity: warning  → emits `warn: when > 0` (or row_count's equivalent)
    so the scan still passes but a warning is recorded
"""

from __future__ import annotations

from .loader import Contract, Expectation


def to_soda_yaml(contracts: list[Contract]) -> str:
    """Emit one SodaCL YAML covering every table in `contracts`."""
    blocks: list[str] = []
    for c in contracts:
        block = [f"checks for {c.table}:"]
        for exp in c.expectations:
            block.extend(_emit(exp, table=c.table))
        blocks.append("\n".join(block))
    return "\n\n".join(blocks) + "\n"


def _emit(exp: Expectation, *, table: str) -> list[str]:
    """Translate one expectation to SodaCL lines (already indented)."""
    t = exp.type
    k = exp.kwargs

    if t == "ExpectColumnValuesToBeUnique":
        return _check_with_severity(
            head=f"duplicate_count({k['column']}) = 0",
            name=exp.rationale,
            extras=[],
            severity=exp.severity,
        )

    if t == "ExpectCompoundColumnsToBeUnique":
        cols = ", ".join(k["column_list"])
        return _check_with_severity(
            head=f"duplicate_count({cols}) = 0",
            name=exp.rationale,
            extras=[],
            severity=exp.severity,
        )

    if t == "ExpectColumnValuesToBeInSet":
        values = "[" + ", ".join(str(v) for v in k["value_set"]) + "]"
        return _check_with_severity(
            head=f"invalid_count({k['column']}) = 0",
            name=exp.rationale,
            extras=[f"valid values: {values}"],
            severity=exp.severity,
        )

    if t == "ExpectColumnValuesToBeBetween":
        mostly = k.get("mostly")
        extras: list[str] = []
        if "min_value" in k:
            extras.append(f"valid min: {k['min_value']}")
        if "max_value" in k:
            extras.append(f"valid max: {k['max_value']}")
        if mostly is not None:
            threshold = round((1 - mostly) * 100, 6)
            head = f"invalid_percent({k['column']}) < {threshold}"
        else:
            head = f"invalid_count({k['column']}) = 0"
        return _check_with_severity(
            head=head, name=exp.rationale, extras=extras, severity=exp.severity
        )

    if t == "ExpectColumnValuesToNotBeNull":
        mostly = k.get("mostly")
        if mostly is not None:
            threshold = round((1 - mostly) * 100, 6)
            head = f"missing_percent({k['column']}) < {threshold}"
        else:
            head = f"missing_count({k['column']}) = 0"
        return _check_with_severity(head=head, name=exp.rationale, extras=[], severity=exp.severity)

    if t == "ExpectTableRowCountToBeBetween":
        return _emit_row_count(k, name=exp.rationale, severity=exp.severity)

    if t == "ExpectColumnPairValuesAToBeGreaterThanB":
        return _emit_cross_column(k, table=table, name=exp.rationale, severity=exp.severity)

    raise ValueError(f"unsupported expectation type for Soda adapter: {t!r}")


def _check_with_severity(*, head: str, name: str, extras: list[str], severity: str) -> list[str]:
    """Emit a bare check (critical) or a check with `warn: when > 0` (warning).

    Soda's pattern for tiering: keep the check expression the same, add
    `warn: when > 0` so any non-zero count/percent triggers a warning but
    the scan does not fail.
    """
    lines = [f"  - {head}:", f"      name: {name}"]
    for extra in extras:
        lines.append(f"      {extra}")
    if severity == "warning":
        lines.append("      warn: when > 0")
    return lines


def _emit_row_count(k: dict, *, name: str, severity: str) -> list[str]:
    """row_count needs different warn syntax — it's a single value, not a count of bad rows."""
    has_min = "min_value" in k
    has_max = "max_value" in k

    if severity == "critical":
        if has_min and has_max:
            head = f"row_count between {k['min_value']} and {k['max_value']}"
        elif has_min:
            head = "row_count > 0" if k["min_value"] == 1 else f"row_count >= {k['min_value']}"
        else:
            head = f"row_count <= {k['max_value']}"
        return [f"  - {head}:", f"      name: {name}"]

    # severity: warning — use Soda's `warn: when not between/>=/<=` block
    if has_min and has_max:
        cond = f"not between {k['min_value']} and {k['max_value']}"
    elif has_min:
        cond = f"< {k['min_value']}"
    else:
        cond = f"> {k['max_value']}"
    return [
        "  - row_count:",
        f"      name: {name}",
        f"      warn: when {cond}",
    ]


def _emit_cross_column(k: dict, *, table: str, name: str, severity: str) -> list[str]:
    """ExpectColumnPairValuesAToBeGreaterThanB → Soda `failed rows` with SQL."""
    a, b = k["column_A"], k["column_B"]
    cmp = "<" if k.get("or_equal") else "<="
    where = f"{a} {cmp} {b}"
    if k.get("ignore_row_if") == "either_value_is_missing":
        where += f" AND {a} IS NOT NULL AND {b} IS NOT NULL"

    block_key = "warn" if severity == "warning" else "fail"
    return [
        "  - failed rows:",
        f"      name: {name}",
        f"      {block_key} query: |",
        f"        SELECT * FROM {table} WHERE {where}",
    ]
