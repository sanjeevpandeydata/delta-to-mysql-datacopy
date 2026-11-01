# Delta Lake to MySQL — Optimized Data Copy Pipeline

![Databricks](https://img.shields.io/badge/Databricks-Spark-FF3621)
![Delta Lake](https://img.shields.io/badge/Delta-Lake-00ADD8)
![MySQL](https://img.shields.io/badge/MySQL-8.0-4479A1)
![Python](https://img.shields.io/badge/Python-3.9-blue)
![Serverless](https://img.shields.io/badge/Serverless-Compatible-green)

## The problem

A leads report table lives in Delta Lake (Unity Catalog) and has to be available in MySQL for downstream reporting. The first version of the copy worked, but it did all of its work on the Spark driver: it collected every row to driver memory and inserted it with a single-threaded loop, and it pulled every existing lead ID to the driver to work out what was new. That pattern cannot grow — it risks driver out-of-memory failures and it cannot run on serverless compute, where there is no shared driver state to rely on.

This repository keeps that first version for comparison and ships the optimized version that replaced it: the same two modes, the same 30-column target table, but with the copy distributed across Spark executors.

## What it does

Copies the `leads_detailrpt_daily` table from Delta Lake into MySQL, in two modes:

| Mode | Trigger | Behaviour |
|---|---|---|
| `hist` | Manual / one-time | **TRUNCATE** the MySQL table, then reload the last 2 years, one year at a time |
| `daily` | Scheduled, 00:30 America/New_York in the workflow definition | Load yesterday's records only, skipping lead IDs already present in MySQL |

A readiness gate runs first in the workflow: the copy only proceeds when the source table has data. See [docs/architecture.md](docs/architecture.md) for the full flow, design decisions, and limitations.

## The optimization: v1 → v2

The change is in *where* the work happens, not in what is copied.

| Area | v1 — before | v2 — production |
|---|---|---|
| Insert | `delta_df.collect()` to the driver, then `cursor.executemany()` in a single-threaded loop | `repartition()` + `mapInPandas()`: each executor partition opens its own MySQL connection and inserts its chunk |
| Dedup (daily) | `cursor.fetchall()` → Python `set` → `isin()` filter, all IDs in driver memory | `spark.read.jdbc()` with a date-filtered pushdown query, then a `left_anti` join on the executors |
| Partitioning | None — one driver thread | Dynamic: `count // (chunk_size * 5)`, capped at 20 partitions, so MySQL is not flooded with connections |
| Serverless | No — driver-bound | Yes — no shared driver state in the insert path |

The v1 notebook is kept in this repository, unchanged, as `leads_data_copy_v1_before_optimization.py`, so the before/after comparison can be read side by side. The production notebook is `leads_data_copy_v2_optimized.py`.

**What is not claimed here:** no timed benchmark was supplied with these files, so this README claims no runtime improvement, no row-per-second figure, and no measured memory figure. The improvement claimed is architectural — parallelism and no driver `collect()` — and the "Performance" section of the architecture document states exactly what to measure to prove it.

## Architecture

```
Databricks Workflow: wf_leads_datacopy
  │
  ├─ Task 1: check_data_copy_readiness   (gate check, sets table_count)
  │
  ├─ Task 2: table_count_check           (condition: table_count == "1"?)
  │        └─ false → workflow ends, nothing copied
  │
  └─ Task 3: leads_data_copy_v2_optimized
         │
         ├─ hist  → TRUNCATE MySQL → year-by-year → mapInPandas insert
         └─ daily → JDBC anti-join dedup → mapInPandas insert
                              │
                    Delta Lake (Unity Catalog)
                              │
                       MySQL staging_db
```

Full detail — end-to-end flow for both modes, the v1 vs v2 evidence, design decisions with trade-offs, failure modes, and the deployment checklist — is in [docs/architecture.md](docs/architecture.md).

## Key design choices

- **`mapInPandas` for writes.** Each partition is a pandas DataFrame on an executor. It opens its own MySQL connection, inserts in `chunk_size` batches, commits, and closes. Data never moves to the driver.
- **JDBC anti-join for daily dedup.** Only the existing IDs for the target date are read, and the join runs distributed. The old approach had to hold every existing ID in driver RAM.
- **Year-wise historical reload.** Two years are loaded one year at a time, so each batch stays manageable and progress is visible per year.
- **Condition gate.** An empty source table ends the workflow before the copy task runs, instead of producing a confusing zero-row run.
- **Environment-aware database names.** `staging_db` in production, `staging_db_dev` / `staging_db_qa` in dev/QA, with no code branching — the suffix comes from the `env` parameter.

## How to deploy

1. Upload both notebooks to the Databricks workspace under `/Workspace/projects/leads_datacopy/`. The workflow also references a readiness notebook at `/Workspace/projects/leads_datacopy/check_data_copy_readiness` — that notebook is **not in this repository** and must already exist at that path, or Task 1 will fail. See Limitations.
2. Store the MySQL credentials in the Databricks Secret Scope `adls{env}scope` (`mysqlusername`, `mysqlpassword`, `mysqlhost`, `mysqlport`). No host, username, or password is hardcoded anywhere in this repository.
3. In `workflows/wf_leads_datacopy.yml`, replace `<your-service-principal-id>` in `run_as`, and confirm the warehouse/cluster environment, the `shared_functions` wheel reference (see Limitations), and the schedule. The schedule in the YAML is currently `PAUSED` and the default mode is `hist` — see the safety note below.
4. For daily runs: set `activity_timeframe = daily` and unpause the schedule. For a historical reload: set `activity_timeframe = hist` and trigger manually.

> **Safety — read before the first run.** The workflow default is `hist`, and `hist` **truncates** the MySQL target table before reloading. Confirm the target database and table, and that a reload is intended, before running it against anything other than a dev/QA database.

## Configuration

| Parameter | Default in workflow | Meaning |
|---|---|---|
| `activity_timeframe` | `hist` | `hist` = full reload, `daily` = yesterday only |
| `chunk_size` | `50000` | Batch size inside each partition insert; also drives partition sizing |
| `env` | `dev` | `dev` / `qa` add a suffix to the MySQL database name; production uses the plain name |
| `staging_database` / `leads_table` | `staging_db` / `leads_detailrpt_daily` | Source Delta catalog/table and target MySQL database/table |
| `dataset` | `leads_detailrpt_daily` | Dataset name passed through the workflow |

## Limitations — read openly

- **Readiness notebook not included.** Task 1 (`check_data_copy_readiness`) is referenced by the workflow but is not one of the files supplied for this repository. The workflow cannot run as-is without it.
- **`shared_functions` wheel not included.** The workflow environment references `shared_functions-0.8-py3-none-any.whl`. That wheel is not in this repository and is not imported by either copy notebook. Remove the reference or point it at your own wheel if your workspace does not need it.
- **Daily dedup failure mode.** In v2, if the JDBC read of existing IDs raises *any* exception, the notebook treats all rows as new and inserts them. A MySQL connectivity failure is therefore indistinguishable from an empty target table in that path. Check the run logs after a daily run that inserts an unexpectedly large number of rows.
- **No measured benchmark supplied.** Runtime, throughput, and driver-memory improvements are described by design, not by a measured run in the supplied files. Do not quote a speed-up from this repository without running the comparison described in the architecture document.
- **Date handling.** "Yesterday" is computed as UTC minus 5 hours minus 1 day (a fixed EST offset). It does not adjust for daylight saving time.

## Project structure

```
delta-to-mysql-datacopy/
├── notebooks/
│   ├── leads_data_copy_v1_before_optimization.py  # Before: driver collect() + executemany()
│   └── leads_data_copy_v2_optimized.py            # Production: mapInPandas + JDBC anti-join
├── workflows/
│   └── wf_leads_datacopy.yml                      # Databricks Workflow: gate + copy, paused by default
├── data/
│   └── sample_leads_data.csv                      # Synthetic sample: 10 invented rows, same 30 columns
├── docs/
│   └── architecture.md                            # Improved architecture: flow, v1 vs v2, decisions, limits
├── requirements.txt
├── .gitignore
├── LICENSE
└── README.md
```

Sample data: `data/sample_leads_data.csv` contains 10 synthetic rows with the same 30 columns as the target table, with invented names and 555 phone numbers, for checking the schema and file shape only. It is test data, not real customer data.

## Tech stack

| Component | Technology |
|---|---|
| Source | Delta Lake, Unity Catalog, Databricks |
| Target | MySQL 8.0, `mysql-connector-python` |
| Processing | PySpark, `mapInPandas`, JDBC (`com.mysql.cj.jdbc.Driver`) |
| Orchestration | Databricks Workflows (YAML, condition gate, scheduled) |
| Secrets | Databricks Secret Scope |

## Author

**Sanjeev Kumar Pandey** — Data Engineer → AI Engineer | Azure Databricks | PySpark | RAG | Agentic AI

[LinkedIn](https://www.linkedin.com/in/sanjeev-pandey-7a45831ba) · [GitHub](https://github.com/sanjeevpandeydata)
