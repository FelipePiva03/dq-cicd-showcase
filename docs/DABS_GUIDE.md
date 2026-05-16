# Databricks Asset Bundles — Walkthrough

> What this repo demonstrates about DABs, and why each piece is there.

## Why DABs at all

Before DABs, deploying Databricks code meant:
- Notebooks pushed via the workspace UI or `databricks workspace import`
- Jobs created manually or via JSON dumps from the API
- "Which environment has the latest version of this job?" answered by hope

DABs collapses all of that into **declarative YAML, source-controlled, deployed via CLI**.
It's the Terraform of Databricks: same model, same workflow.

## The four building blocks in this repo

### 1. `databricks.yml` — the bundle root

Two key sections:

- **`include:`** points to the modular job/resource YAMLs under `resources/`.
  Keeping each job in its own file scales better than one mega-config.
- **`targets:`** declares the deployable environments. Each target can
  override `workspace`, `variables`, `run_as`, etc.

### 2. `resources/jobs/*.yml` — declarative jobs

Each YAML is a complete job spec: tasks, dependencies, cluster config,
notifications. The `${var.catalog}` syntax pulls from `databricks.yml`
variables, which lets the **same job definition deploy to dev or prod**
just by passing `--target`.

### 3. The CLI commands you'll actually use

| Command | When |
|---|---|
| `databricks bundle validate --target X` | Pre-commit, in CI on every PR |
| `databricks bundle deploy --target X` | After merge to develop/main |
| `databricks bundle run <job_key> --target X` | Trigger a deployed job |
| `databricks bundle destroy --target X` | Tear down (e.g. cost cleanup) |

### 4. `mode: development` vs `mode: production`

| | `development` | `production` |
|---|---|---|
| Resource name prefix | `[username] [dev]` auto-prefixed | none |
| Schedules / triggers | paused by default | run as declared |
| Run-as | current user | usually a service principal |
| Concurrent runs | limited | as declared |

This is what gives us "free" environment isolation. Two engineers deploying
to `dev` won't clobber each other's jobs.

## Free Trial caveats

- No service principal available — `run_as` falls back to current user
- Unity Catalog setup needed before first deploy (one-time admin step)
- 14-day timer = capture screenshots/GIFs of successful runs while you can

## What I'd add for a real prod setup

- Service principal via `run_as`
- Bundle deployment from `develop` blocked unless tests pass (already in CI)
- Bundle history retained via Git tags (already done)
- Workspace permissions configured via `permissions:` blocks
- Cluster policies referenced via `policy_id` instead of inline `new_cluster`
