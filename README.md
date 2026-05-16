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
> Built to compare **Great Expectations vs. Soda Core** side by side on the
> same business rules — see [`docs/COMPARISON.md`](docs/COMPARISON.md).

---

## ✨ What this project demonstrates

| Capability | Where to look |
|---|---|
| **Structured Streaming + Auto Loader** | [`src/pipelines/bronze/ingest_orders.py`](src/pipelines/bronze/ingest_orders.py) |
| **`foreachBatch` + MERGE INTO Delta** | [`src/pipelines/silver/transform_orders.py`](src/pipelines/silver/transform_orders.py) |
| **Databricks Asset Bundles (dev + prod targets)** | [`databricks.yml`](databricks.yml), [`resources/jobs/`](resources/jobs/) |
| **GX as a DQ gate task** | [`src/dq/great_expectations/`](src/dq/great_expectations/) |
| **Soda Core as a DQ gate task** | [`src/dq/soda/`](src/dq/soda/) |
| **3-stage CI/CD on GitHub Actions** | [`.github/workflows/`](.github/workflows/) |
| **Pytest unit + integration tests with chispa** | [`tests/`](tests/) |
| **Side-by-side GX vs Soda comparison** | [`docs/COMPARISON.md`](docs/COMPARISON.md) |

---

## 🏗️ Architecture

```
                  ┌────────────────────────┐
                  │  Olist CSVs (Kaggle)   │
                  └───────────┬────────────┘
                              │  producer job (replay)
                              ▼
                  ┌────────────────────────┐
                  │  /Volumes/landing/...  │
                  └───────────┬────────────┘
                              │  Auto Loader (cloudFiles)
                              ▼
                  ┌────────────────────────┐
                  │   bronze.orders        │   append-only Delta
                  └───────────┬────────────┘
                              │  readStream + foreachBatch MERGE
                              ▼
                  ┌────────────────────────┐
                  │   silver.orders        │   conformed, deduped
                  └───────────┬────────────┘
                              │
                ┌─────────────┴─────────────┐
                │  🛡️ GX checkpoint (gate)  │   ← fails the job on regression
                └─────────────┬─────────────┘
                              ▼
                  ┌────────────────────────┐
                  │   gold.daily_orders    │
                  │   gold.delivery_perf   │
                  └───────────┬────────────┘
                              │
                ┌─────────────┴─────────────┐
                │  🛡️ Soda scan (gate)      │   ← fails the job on regression
                └─────────────┬─────────────┘
                              ▼
                  ┌────────────────────────┐
                  │   dq.run_results       │   ← powers DQ dashboard
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
databricks bundle run streaming_pipeline --target dev
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
| GX **and** Soda | Comparison is the point of the showcase; trade-offs are the takeaway |
| DABs instead of "deploy notebooks" | IaC, multi-env for free, source-of-truth in Git |

---

## 📚 Further reading

- [`docs/COMPARISON.md`](docs/COMPARISON.md) — the GX vs Soda field comparison
- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — deeper architectural decisions
- [`docs/DABS_GUIDE.md`](docs/DABS_GUIDE.md) — DABs configuration walkthrough

---

## 👤 Author

**Felipe Piva** — Data Engineer at CNH Industrial (Curitiba, Brazil)
[LinkedIn](https://linkedin.com/in/felipe-piva-developer) · [GitHub](https://github.com/FelipePiva03)
