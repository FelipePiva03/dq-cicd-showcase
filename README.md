# 🛡️ DQ + CI/CD Showcase — Streaming Medallion on Databricks

![CI](https://github.com/FelipePiva03/dq-cicd-showcase/actions/workflows/ci.yml/badge.svg)
![Deploy dev](https://github.com/FelipePiva03/dq-cicd-showcase/actions/workflows/deploy-dev.yml/badge.svg)
![Python](https://img.shields.io/badge/python-3.11-blue)
![Databricks](https://img.shields.io/badge/Databricks-DABs-orange)
![License](https://img.shields.io/badge/license-MIT-green)

> A streaming Medallion pipeline that ingests Brazilian e-commerce data
> (Olist), with **Data Quality gates** between every layer and a full
> **CI/CD lifecycle** powered by **Databricks Asset Bundles** + GitHub Actions.
>
> Uses **Great Expectations** (1.x) as the single DQ gate across silver
> facts, silver dimensions, and gold marts — with tiered critical/warning
> severity driven by per-table contracts.

---

## ✨ What this project demonstrates

| Capability | Where to look |
|---|---|
| **Structured Streaming + Auto Loader** | [`src/pipelines/bronze/ingest_events.py`](src/pipelines/bronze/ingest_events.py) |
| **`foreachBatch` + MERGE INTO Delta (facts)** | [`src/pipelines/silver/conform_fact.py`](src/pipelines/silver/conform_fact.py) |
| **SCD1 MERGE on dimensions** | [`src/pipelines/silver/conform_dim.py`](src/pipelines/silver/conform_dim.py) |
| **Quarantine pre-MERGE for hard violations** | [`src/pipelines/silver/conform_fact.py`](src/pipelines/silver/conform_fact.py) (`HARD_RULES`, `split_quarantine`) |
| **Databricks Asset Bundles (dev + prod targets)** | [`databricks.yml`](databricks.yml), [`resources/jobs/`](resources/jobs/) |
| **GX gate (silver facts + dims + gold marts, tiered critical/warning)** | [`src/dq/great_expectations/`](src/dq/great_expectations/) |
| **Contract-driven DQ (one YAML per table)** | [`contracts/`](contracts/), [`src/dq/contracts/`](src/dq/contracts/) |
| **3-stage CI/CD on GitHub Actions** | [`.github/workflows/`](.github/workflows/) |
| **Pytest unit + integration tests with chispa** | [`tests/`](tests/) |

---

## 🏗️ Architecture

```
        ┌──────────────────────┐
        │  Olist CSVs (Kaggle) │
        └──────────┬───────────┘
                   │ producer (temporal replay)
                   ▼
        ┌──────────────────────┐
        │ /Volumes/landing/... │
        └──────────┬───────────┘
                   │ Auto Loader (events) / COPY INTO (dims)
                   ▼
        ┌────────────────────────────────┐
        │  bronze.fact_*   bronze.dim_*  │  append-only Delta
        └──────────┬─────────────┬───────┘
                   │             │
   readStream      │             │  batch MERGE (SCD1)
   foreachBatch    │             │
   MERGE           ▼             ▼
        ┌──────────────────┐ ┌──────────────────┐
        │ 🛡️ quarantine   │ │                  │
        │   (HARD_RULES)  │ │  silver.dim_*    │
        └──────────┬──────┘ └─────────┬────────┘
                   ▼                  │
        ┌──────────────────┐          │
        │  silver.fact_*   │          │
        └──────────┬───────┘          │
                   │                  │
        ┌──────────┴───────┐          │
        │ 🛡️ GX gate       │ tiered  │
        │ (critical/warn)  │          │
        └──────────┬───────┘          │
                   │  ┌───────────────┘
                   │  │  🛡️ GX gate (dims)
                   ▼  ▼
        ┌────────────────────────┐
        │  gold.daily_orders     │
        │  gold.delivery_perf    │
        └──────────┬─────────────┘
                   │
        ┌──────────┴───────┐
        │ 🛡️ GX gate       │  gold mart invariants
        └──────────┬───────┘
                   ▼
        ┌────────────────────────┐
        │   dq.run_results       │  scan results (Delta append)
        └────────────────────────┘
```

DQ gates are **separate job tasks** with `depends_on`, so the failure
surface in the Databricks UI tells you *exactly* which gate broke.

---

## 🔁 CI/CD lifecycle

```
PR ─────► CI workflow ─────► merge to develop ─────► deploy-dev workflow
                                                            │
                                                            ▼
                                                   bundle deploy --target dev
                                                            │
                                                            ▼
                                                   smoke-run gx_validation
                                                            │
                                                            ▼
                                                  tag v*.*.* ─────► deploy-prod
                                                                         │
                                                                         ▼
                                                            (manual approval)
                                                                         │
                                                                         ▼
                                                            bundle deploy --target prod
```

| Workflow | Triggers | What runs |
|---|---|---|
| [`ci.yml`](.github/workflows/ci.yml) | PR + push to `develop` | Ruff lint/format, pytest unit + integration, `bundle validate` |
| [`deploy-dev.yml`](.github/workflows/deploy-dev.yml) | push to `develop` | `bundle deploy --target dev` + smoke run |
| [`deploy-prod.yml`](.github/workflows/deploy-prod.yml) | tag `v*.*.*` | `bundle deploy --target prod` (manual approval) |

---

## 🚀 Getting started

### Prerequisites
- Python 3.11+
- Java 17 (for local Spark)
- A Databricks workspace (Free Trial works)
- The Databricks CLI v0.205+: `pip install databricks-cli` or `brew install databricks/tap/databricks`

### Local setup

```bash
git clone https://github.com/FelipePiva03/dq-cicd-showcase
cd dq-cicd-showcase
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev,spark]"
pytest tests/ -v
```

### Bundle commands

```bash
# Authenticate with the workspace
databricks configure --token

# Validate config
databricks bundle validate --target dev

# Deploy jobs to dev
databricks bundle deploy --target dev

# Run a specific job
databricks bundle run streaming_events --target dev
```

### Loading the dataset

1. Grab the Olist dataset from
   [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).
2. Upload to a Unity Catalog volume:
   ```
   /Volumes/dq_showcase_dev/source/raw/olist_orders_dataset.csv
   ```
3. Trigger the producer job:
   ```bash
   databricks bundle run olist_producer --target dev
   ```

---

## 🤔 Why these choices

| Decision | Why |
|---|---|
| Structured Streaming over batch | Mirrors real-world ingestion + exercises Auto Loader semantics |
| Auto Loader instead of Kafka | Kafka was overkill for a static dataset replay; Auto Loader is the Databricks-native pattern |
| `foreachBatch` + MERGE on Silver | Order status updates require idempotent upserts, not appends |
| DQ as separate job tasks | Failure points are visible in the run timeline, not buried in logs |
| GX everywhere (single runner) | Soda Core was tried first for dims/gold but failed on Databricks serverless (`client: "2"`) — both 3.3.7 and 3.5.6 raised Spark-Connect/session-detach errors mid-scan. GX 1.x runs cleanly across facts, dims, and marts; the contract layer abstracts the runner so the swap was a one-file rewrite |
| Quarantine pre-MERGE, gates post-MERGE | Hard structural failures shouldn't poison silver; soft business failures should block downstream |
| DABs instead of "deploy notebooks" | IaC, multi-env for free, source-of-truth in Git |

---

## 📚 Further reading

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — three-layer DQ defense and deeper rationale
- [`docs/DABS_GUIDE.md`](docs/DABS_GUIDE.md) — DABs configuration walkthrough

---

## 👤 Author

**Felipe Piva** — Data Engineer at CNH Industrial (Curitiba, Brazil)
[LinkedIn](https://linkedin.com/in/felipe-piva-developer) · [GitHub](https://github.com/FelipePiva03)
