# Great Expectations vs Soda Core — A Field Comparison

> Both tools validate the **same six business rules** on `silver.orders` from
> the Olist dataset. This document captures my findings after implementing
> them side by side as Databricks job tasks in this repository.

## Why this comparison exists

When picking a Data Quality tool for a real platform, the choice usually
comes down to **trade-offs**, not features on paper. Marketing sites all
claim "complete coverage" — so I built the same suite twice and measured.

## Setup

| | Great Expectations | Soda Core |
|---|---|---|
| Version | `0.18.19` | `3.3.7` (`soda-core-spark-df`) |
| Spark adapter | Runtime DataFrame | `add_spark_session` |
| Result persistence | Custom action → Delta | Custom action → Delta |
| Code lives in | `src/dq/great_expectations/` | `src/dq/soda/` |

## The six checks (same rules, both sides)

| # | Rule | GX expectation | Soda check |
|---|---|---|---|
| 1 | `order_id` not null | `expect_column_values_to_not_be_null` | `missing_count(order_id) = 0` |
| 2 | `order_id` unique | `expect_column_values_to_be_unique` | `duplicate_count(order_id) = 0` |
| 3 | `order_status` ∈ allowed set | `expect_column_values_to_be_in_set` | `invalid_count(order_status) = 0` w/ `valid values` |
| 4 | `days_to_delivery` ∈ [0,90] (99%) | `expect_column_values_to_be_between` w/ `mostly` | `invalid_percent < 1` w/ valid min/max |
| 5 | Delivery date ≥ purchase date | `expect_column_pair_values_a_to_be_greater_than_b` | `failed rows` with custom SQL |
| 6 | Row count plausibility | `expect_table_row_count_to_be_between` | `row_count between 1 and 1000000` |

## Findings (to fill in after running both)

### Verbosity — lines of code to express the suite
- GX: _N lines_ (JSON suite + checkpoint YAML + Python runner ≈ XX LoC)
- Soda: _N lines_ (single YAML ≈ XX LoC)
- **Winner:** _Soda / GX / tie_

### Readability for a non-engineer (PM, analyst)
- GX: …
- Soda: …
- **Winner:** _Soda / GX / tie_

### Expressiveness — how easily I expressed check #5 (cross-column)
- GX: …
- Soda: …
- **Winner:** _Soda / GX / tie_

### Observability / Data Docs
- GX: native HTML Data Docs site, great for sharing with stakeholders
- Soda: Soda Cloud (paid) or roll-your-own dashboard from `dq.run_results`
- **Winner:** _Soda / GX / tie_

### Spark integration friction
- GX: …
- Soda: …
- **Winner:** _Soda / GX / tie_

### Community & momentum (as of <date>)
- GX: …
- Soda: …

## My verdict

> _Fill in: which tool I'd pick if I had to recommend one tomorrow at CNH,
> and what would change my mind. Include the "why" — the meta-skill the
> portfolio is signaling is judgment, not just "ran two tools"._

## What I'd do differently next time

> _Things I learned the hard way during implementation._
