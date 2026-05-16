# Architecture

> Deeper rationale for the design decisions summarized in the README.

## Layer responsibilities

### Bronze — raw, immutable, append-only
Schema-on-read via Auto Loader. We keep the data **exactly as it arrived**,
plus two observability columns (`_ingestion_ts`, `_source_file`). This layer
is replayable — if Silver logic changes, we recompute from Bronze without
re-ingesting from source.

### Silver — conformed, deduped, business-typed
Type-cast, normalized, deduplicated by natural key. MERGE upserts make
re-runs safe. **This is the layer DQ guards most aggressively** — once data
passes the GX gate, downstream consumers can trust it.

### Gold — agg-by-purpose
One table per analytical question. Wide, denormalized, BI-friendly. Soda
checks here protect business semantics (no negative revenue, etc).

## Why streaming + `availableNow=True`

The pipeline is streaming-native (`readStream`, `writeStream`,
`foreachBatch`) but triggers with `availableNow=True` for cost control on
Free Trial. Swapping to `processingTime("1 minute")` makes it truly
continuous — same code, one parameter.

## DQ placement

| Gate | Where | Why |
|---|---|---|
| GX on Silver | After conform + MERGE | Last chance to block bad data before it enters the source of truth |
| Soda on Gold | After aggregations | Business-rule layer; YAML readable by analysts |

## Idempotency story

- **Bronze**: Auto Loader checkpoint = exactly-once file processing
- **Silver**: MERGE on natural key = idempotent upsert
- **Gold**: full overwrite from Silver = trivially idempotent
- **DQ tasks**: stateless, append run results to `dq.run_results`

## What's deliberately out of scope

- Real-time SLAs (this is a portfolio — not on-call)
- PII handling (Olist is anonymized at source)
- Cost optimization beyond `availableNow` (would warrant photon, AQE tuning)
- Lineage tooling (Unity Catalog provides enough for the showcase)
