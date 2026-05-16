# Architecture

> Deeper rationale for the design decisions summarized in the README.

## Topology

Four jobs, three cadences, two DQ tools — one medallion.

```
                ┌──────────────────────┐
                │  data/raw/ (CSVs)    │   committed for the showcase
                └──────────┬───────────┘
                           │
       ┌───────────────────┴──────────────────────┐
       │                                          │
       ▼                                          ▼
┌──────────────────┐                  ┌──────────────────────┐
│ producer_events  │                  │  /Volumes/.../raw/   │
│ replay temporal  │                  │  dims/ (master data) │
│ by purchase_ts   │                  └──────────┬───────────┘
└────────┬─────────┘                             │
         │ /Volumes/landing/events/{table}/      │
         ▼                                       ▼
┌──────────────────┐                  ┌──────────────────────┐
│ streaming_events │                  │  batch_dimensions    │
│ trigger=         │                  │ schedule: daily 04h  │
│   availableNow   │                  │ COPY INTO + MERGE    │
│ every 5 min      │                  │ SCD Type 1           │
│                  │                  │                      │
│ bronze.fact_*    │                  │ bronze.dim_*         │
│ silver.fact_*    │                  │ silver.dim_*         │
│ 🛡️ GX gate       │                  │ 🛡️ Soda gate         │
└────────┬─────────┘                  └──────────┬───────────┘
         │                                       │
         └──────────────────┬────────────────────┘
                            ▼
                  ┌──────────────────┐
                  │ gold_aggregations│  schedule: nightly 05h
                  │ silver→gold      │  joins fact × dim
                  │ 🛡️ Soda gate     │  business-rule checks
                  └──────────────────┘
```

## Why 4 jobs (not 1)

| Concern | One job | Four jobs |
|---|---|---|
| Cadence | Locked together | Streaming/daily/nightly each on their own clock |
| Failure surface | One break stalls everything | Broken dim refresh doesn't block order ingestion |
| Compute | Long-running cluster pays for idle batch | Streaming cluster + ephemeral job clusters for batch |
| UI lineage | One linear graph | Multiple panels — accepted cost |

Cadence is the deciding factor. A daily dim refresh wedged into a streaming
job either runs too often (wasted compute) or holds back the stream
(latency regression). Splitting them is honest design.

## Layer responsibilities

### Bronze — raw, immutable, append-only
Schema-on-read via Auto Loader (events) or `COPY INTO` (dimensions).
Data is preserved **exactly as it arrived**, plus two observability columns
(`_ingestion_ts`, `_source_file`). Bronze is replayable: if Silver logic
changes, we recompute from Bronze without re-ingesting from source.

### Silver — full dimensional model
Kimball-style star schema. Each natural-key collision resolved via MERGE,
each business type cast, each duplicate removed. **This is where the DQ
gates bite hardest** — once data passes the gate, downstream consumers
can trust it.

| Kind | Tables | Source | Update pattern |
|---|---|---|---|
| Fact | `silver.fact_orders`, `silver.fact_order_items`, `silver.fact_payments`, `silver.fact_reviews` | streaming events | MERGE on natural key via `foreachBatch` |
| Dim | `silver.dim_customer`, `silver.dim_product`, `silver.dim_seller`, `silver.dim_geolocation`, `silver.dim_category` | batch dimensions | MERGE SCD Type 1 (overwrite) |

**Why SCD1, not SCD2?** Olist is a static historical export — there is no
real "history of changes" to track. SCD1 is the honest choice for this
dataset. The SCD2 pattern is documented in [`docs/DABS_GUIDE.md`](DABS_GUIDE.md)
as a swap-in for production scenarios.

### Gold — agg-by-purpose marts
One table per analytical question. Wide, denormalized, BI-friendly.
Examples:

| Mart | Question |
|---|---|
| `gold.daily_revenue` | Revenue / order count by purchase date |
| `gold.delivery_performance` | On-time rate, avg delay, by state |
| `gold.top_categories` | Revenue & unit volume by product category |
| `gold.customer_lifetime_value` | First-purchase, last-purchase, total spend per customer |

Soda checks here protect business semantics (no negative revenue,
delivery_performance.on_time_rate between 0 and 1, etc).

## Event replay strategy

The producer reads the 4 event CSVs (`orders`, `order_items`,
`order_payments`, `order_reviews`), orders all rows by
`order_purchase_timestamp`, and drops them into
`/Volumes/{catalog}/landing/events/{table}/` in time-windowed micro-batches
(default: 1 real day = 1 wall-clock minute, configurable).

This produces realistic late-arriving data:
- `order_items` typically lands with its parent `orders`
- `order_payments` lands shortly after
- `order_reviews` can land days later

That late arrival is the **whole reason streaming exists** in this project.
A batch job that just reads all 4 CSVs once would never exercise
`foreachBatch` MERGE semantics or watermarks.

## DQ layers — quarantine, gates, and what each catches

Three concentric defenses, each with a single job:

| Layer | Catches | Failure mode |
|---|---|---|
| **Quarantine (silver pre-MERGE)** | Structural integrity — null PK, null FK to required dim, value unparseable after `try_cast` | Bad rows routed to `quarantine.fact_*` with `_quarantine_reason`; silver stays clean; pipeline continues |
| **GX gate (silver post-MERGE)** | Business semantics — `days_to_delivery >= 0`, `review_score BETWEEN 1 AND 5`, status enums | Suite fails → downstream gold blocked; silver retains the run, GX results persisted to `dq.run_results` |
| **Soda gate (gold post-marts)** | Aggregate invariants — `avg_revenue between 0 and 1e7`, `on_time_rate ∈ [0,1]` | Mart fails → consumers (BI, alerts) see stale data, not bad data |

**Why split hard vs soft rules?** Hard rules protect silver's *shape*
(typed columns, MERGE keys present); soft rules protect downstream
*meaning* (revenue makes sense, scores in range). Conflating them in one
gate forces a binary choice: either fail everything on a single null
order_id, or let silver accept structurally broken rows. The split lets
us keep ingesting while quarantining noise.

**GX vs Soda — why both?** GX's Python expressiveness fits the
structural+regex-heavy silver checks; Soda's YAML is ergonomic for the
business-rule gold checks an analyst can read and edit. The split is
deliberate so the comparison in [`docs/COMPARISON.md`](COMPARISON.md)
reflects real usage, not synthetic toy checks.

### Known source data quirk: Olist CSV escaping

The Olist `order_reviews` CSV escapes embedded quotes inconsistently
(`""` in some rows, `"""` in others). Even with `multiLine=true` +
`escape='"'`, the CSV parser misaligns columns on those rows — comment
text leaks into `review_id` and `order_id`. **This is not a pipeline
bug, it's source-data reality.** Quarantine catches every misparsed row
via `review_score_unparseable` or `order_id_null`. Silver only ever sees
well-formed records. A "data engineer hero fix" (pre-cleaning the CSV
in the producer) would just hide the problem from the dashboard later.

## Idempotency story

| Layer | Mechanism |
|---|---|
| Bronze (events) | Auto Loader checkpoint — exactly-once file processing |
| Bronze (dims) | `COPY INTO` — exactly-once file load |
| Silver (facts) | MERGE on natural key via `foreachBatch` — idempotent upsert |
| Silver (dims) | MERGE SCD1 — last-write-wins, deterministic |
| Gold | Full overwrite from Silver — trivially idempotent |
| DQ tasks | Stateless, append run results to `dq.run_results` |

Every layer can be re-run without producing duplicates or partial state.

## Why streaming + `availableNow=True`

The pipeline is streaming-native (`readStream`, `writeStream`,
`foreachBatch`) but triggers with `availableNow=True` for cost control on
Free Trial. Swapping to `processingTime("1 minute")` makes it truly
continuous — same code, one parameter.

The job is then **scheduled** every 5 minutes by the Databricks job
scheduler, which gives the same continuous-ingestion semantics at a
fraction of the cost.

## What's deliberately out of scope

- Real-time SLAs (this is a portfolio — not on-call)
- PII handling (Olist is anonymized at source)
- SCD Type 2 (dataset is static; pattern documented but not implemented)
- Cost optimization beyond `availableNow` (would warrant photon, AQE tuning)
- Lineage tooling (Unity Catalog provides enough for the showcase)
- CDC from a transactional source (Olist is a snapshot, not a CDC feed)
