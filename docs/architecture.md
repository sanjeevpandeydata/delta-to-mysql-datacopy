# Architecture — Delta Lake to MySQL Data Copy

## Purpose

This document describes how the leads data copy pipeline works, why the optimized version is shaped the way it is, and where it can fail. It is written so a reviewer can check each claim against the code in `notebooks/` and the workflow in `workflows/wf_leads_datacopy.yml`.

The pipeline copies one table, `leads_detailrpt_daily`, from Delta Lake (Unity Catalog) into MySQL. It has two modes — a full historical reload and a daily incremental load — and a readiness gate that stops the workflow when the source table is empty.

> **Status of this document.** This is an improved rewrite of the architecture document supplied with the project. The notebooks and workflow file are unchanged. Where the supplied document stated a performance figure with no measurement behind it, this version says so explicitly instead of repeating the figure as a result (see §8).

---

## 1. System overview

```
┌─────────────────────────────────────────────────────────────────────┐
│  Databricks Workflow: wf_leads_datacopy                            │
│  Schedule: 00:30 America/New_York, currently PAUSED in the YAML     │
│                                                                     │
│  ┌──────────────────────────┐                                       │
│  │ Task 1                   │  Notebook (not in this repo — §7)    │
│  │ check_data_copy_readiness│  Sets task value table_count = "1"/"0"│
│  └────────────┬─────────────┘                                       │
│               ▼                                                     │
│  ┌──────────────────────────┐                                       │
│  │ Task 2                   │  Condition gate                       │
│  │ table_count_check        │  Proceed only if table_count == "1"   │
│  └────────────┬─────────────┘                                       │
│               ▼  (only when true)                                   │
│  ┌──────────────────────────┐                                       │
│  │ Task 3                   │  leads_data_copy_v2_optimized.py     │
│  │ leads_data_copy          │  hist mode  OR  daily mode (§2)       │
│  └──────────────────────────┘                                       │
└─────────────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┴─────────────────────┐
        ▼                                           ▼
 Delta Lake (source)                           MySQL (target)
 Unity Catalog: staging_db.                    staging_db[. _dev/_qa].
   leads_detailrpt_daily                         leads_detailrpt_daily
        │                                           ▲
        └───────────── Spark / JDBC ─────────────────┘
                              │
                    Secret Scope: adls{env}scope
                    (MySQL host, username, password, port)
```

**Reading the diagram.** Data flows left to right in Task 3 only. Tasks 1–2 move no data; they decide whether Task 3 should run at all. Credentials never appear in code or in this document — they are read from the Secret Scope at run time (§6).

---

## 2. End-to-end flow

### 2.1 Common steps (both modes)

1. **Gate.** Task 1 checks the source Delta table and sets `table_count`. Task 2 is a condition task: if `table_count` is not `"1"`, the workflow ends. No copy, no truncate, no wasted compute.
2. **Mode selection.** Task 3 reads the `activity_timeframe` widget parameter: `hist` or `daily`. Anything else runs neither branch and reports success with zero records — there is no explicit error for an unrecognized mode.
3. **Target database.** The MySQL database name is `staging_database`, with `_dev` / `_qa` appended when `env` is `dev` / `qa`. The table name is `leads_table`. Both come from workflow parameters, not from code constants.
4. **Reporting.** Each run prints the pipeline name, records processed, duration in seconds, and `Status: SUCCESS`. There is no status-table write in these notebooks, even though the workflow passes `status_database` / `status_tbl` parameters.

### 2.2 Historical mode (`hist`) — full reload

```
TRUNCATE MySQL target table
  │
  ▼
For each year in [today − 2 years … today]:
  │  SELECT … FROM staging_db.leads_detailrpt_daily
  │  WHERE YEAR(activity_timeframe) = <year>
  ▼
  repartition(num_partitions) → mapInPandas insert (§3.1)
  ▼
MySQL target table
```

- The year loop is deliberate: two or three years in one query would make each batch very large. Year-by-year keeps each batch manageable and makes progress visible per year.
- `TRUNCATE` runs on the driver connection before the loop. This is the destructive step in the pipeline — see the safety note in the README.
- The loop bounds are computed from the run date (`today − 2 years` to the current year), so the window moves with time; it is not a fixed pair of years.

### 2.3 Daily mode (`daily`) — incremental load

```
Target date = yesterday, computed as UTC − 5h − 1 day
  │
  ▼
Delta rows WHERE activity_timeframe = <target date>
  │
  ▼
Existing lead_ids for that date, read from MySQL via spark.read.jdbc
  (date-filtered pushdown query — only that date's IDs are transferred)
  │
  ▼
left_anti JOIN on lead_id → only rows NOT already in MySQL survive
  │
  ▼
repartition(num_partitions) → mapInPandas insert (§3.1)
  ▼
MySQL target table
```

- Dedup is by `lead_id` only, for the target date only. A lead ID that already exists for a *different* date is not filtered out by this step.
- If the JDBC read raises an exception, the notebook treats all rows as new and inserts them (§7, failure modes). That fallback is the single most important behaviour to understand before operating the daily mode.

---

## 3. The optimization: v1 → v2, with evidence

Both notebooks copy the same 30 columns, in the same order, into the same MySQL table. The difference is entirely in where the work executes.

### 3.1 Insert path

| | v1: `leads_data_copy_v1_before_optimization.py` | v2: `leads_data_copy_v2_optimized.py` |
|---|---|---|
| Read | `spark.sql(...)` then `delta_df.collect()` — every row of the year is materialized as Python objects on the driver | `spark.sql(...)` then `distributed_insert()` — the DataFrame stays distributed |
| Write | `cursor.executemany(insert_query, chunk_data)` in a loop on the driver, one shared connection | `df.repartition(n).mapInPandas(insert_partition_to_mysql, schema=StructType([])).collect()` — the trailing `.collect()` only triggers execution; the inserts happen on executors |
| Connection | One driver connection, opened once, used for every batch | One new connection per executor partition, opened inside `insert_partition_to_mysql`, committed and closed per partition |
| Concurrency | Single-threaded | One concurrent MySQL connection per partition, capped (§4.2) |
| Serverless | No — the pattern depends on driver state and a driver-held connection | Yes, by design — no shared state between driver and executors in the insert path (the driver connection is used only for `TRUNCATE`) |

The `mapInPandas` function itself is explicit about this contract in its comments: it runs on each executor partition, converts the pandas partition to tuples, inserts in `chunk_size` batches, commits, closes, and yields an empty DataFrame, which the API requires.

### 3.2 Daily dedup path

| | v1 | v2 |
|---|---|---|
| Existing IDs | `cursor.execute("SELECT DISTINCT lead_id …")`, then `set(row[0] for row in cursor.fetchall())` — all IDs in driver RAM | `spark.read.jdbc(url, table=<date-filtered subquery>, properties=…)` — a distributed DataFrame |
| Filter | `delta_df.filter(~delta_df.lead_id.isin(existing_lead_ids))` — the ID set is serialized back to the executors | `delta_df.join(existing_ids_df, on="lead_id", how="left_anti")` — the join runs on the executors |
| Scaling behaviour | Driver memory grows with the number of existing IDs | Driver memory does not grow with the number of existing IDs; the cost moves to the distributed join |

### 3.3 What did *not* change

- The 30-column insert statement, the year-wise loop, the `TRUNCATE` in historical mode, the yesterday-date computation, and the environment suffix on the database name are the same in both versions. The optimization did not change the data model, the modes, or the target table.
- Both versions still print a summary and report `SUCCESS` even when a mode branch inserts zero rows (for example, an empty year or a fully-duplicated daily date). "SUCCESS" in the logs means the notebook completed, not that rows were inserted — check the printed record count.

---

## 4. Design decisions

### 4.1 `mapInPandas` for distributed MySQL writes

**Decision.** Write through `mapInPandas` with a per-partition connection, rather than through Spark's built-in JDBC writer or a driver loop.

**Why.** The target is MySQL, not a Spark-native sink, and the existing insert statement and batching were already expressed as `executemany()` calls. `mapInPandas` keeps that SQL while moving execution to the executors. Each partition is self-contained: connect, insert, commit, close.

**Trade-off.** Connections are opened per partition rather than pooled, so partition count directly controls peak MySQL connections. That is why partitioning is capped (§4.2), and why a runaway partition count would be a MySQL problem, not just a Spark problem.

### 4.2 Dynamic partition sizing, capped at 20

**Decision.** `num_partitions = min(20, max(1, count // (chunk_size * 5)))`.

**Why.** Partition size should scale with the row count, but unbounded parallelism would open one MySQL connection per partition. The cap bounds that.

**What to notice.** With the workflow default `chunk_size = 50000`, the divisor is 250,000, so fewer than 250,000 rows produce a single partition and the parallelism only engages above that. If most daily runs are well under 250,000 rows, the dynamic sizing is effectively inert for them — lowering `chunk_size` is the lever that changes that, at the cost of smaller batches per insert.

### 4.3 JDBC anti-join for dedup

**Decision.** Read existing IDs as a DataFrame and `left_anti` join, instead of fetching IDs to the driver.

**Why.** It removes the only other driver-memory bottleneck in the daily path and keeps the dedup logic in Spark, where it can be pushed down to the target date.

**Trade-off.** The dedup now depends on the JDBC driver and on MySQL being reachable from the executors at read time, not just from the driver. A JDBC failure is handled by the broad fallback in §7 — read that before operating the pipeline.

### 4.4 Year-wise historical reload

**Decision.** Loop year by year instead of loading the whole 2-year window in one query.

**Why.** Bounded batch size, visible per-year progress, and an easy place to resume after a failure (re-run the same mode; already-loaded years are re-copied after the truncate, so a re-run is a full reload, not a partial resume — see §7).

### 4.5 Condition gate before the copy

**Decision.** A separate readiness task plus a condition task, rather than checking row counts inside the copy notebook.

**Why.** The workflow itself decides whether to run, so an empty source table produces no copy task run at all — no truncate, no zero-row insert, no misleading "SUCCESS with 0 rows" from the copy notebook.

**Cost.** The gate depends on a notebook that is not in this repository (§7). The gate is only as reliable as that notebook's `table_count` value.

### 4.6 Environment-aware database names

**Decision.** Append `_dev` / `_qa` to the MySQL database name from the `env` parameter, with no code branching.

**Why.** Dev and QA writes cannot land in the production database by a code path — only by passing the wrong `env` value, which is a single, reviewable parameter.

### 4.7 Secrets via Secret Scope, never hardcoded

**Decision.** Host, username, password, and port are read with `dbutils.secrets.get()` from `adls{env}scope` at run time.

**Why.** The same notebooks run in dev, QA, and production with different credentials and no code change, and no credential value appears in the repository, the workflow YAML, or the logs.

**Note.** In v2 the secret *values* are read on the driver and then used inside the executor function (via closure) to open per-partition connections. The Secret Scope lookup itself happens once, on the driver; the values travel to executors as part of the function. That is the standard pattern for this approach, but it is worth knowing when reviewing credential handling: rotating a secret requires a fresh run, not a code change.

---

## 5. Data and schema

The target table has 30 columns, in the order used by the `INSERT` statement in both notebooks:

```
site_code, brand, activity_timeframe, lead_id, form_phone,
lead_new_used, lead_datetime, lead_source, cust_name, miles_from_dealer,
model, actionable_time, response_date, response_time, survey_returned,
num_key_drivers, other_dealers_contacted, sale_date, selling_dealer_site,
sale_model, sale_new_used, sale_reported_date, lrt_elapsed, lrt_lead,
lost_sales, leads_open_close, dealer_mailing_name, record_guid,
ingest_date, update_ts
```

- **Key for dedup:** `lead_id`, scoped to one `activity_timeframe` date in daily mode. No other uniqueness constraint is stated in the notebooks.
- **Date column:** `activity_timeframe` is used both as the historical year filter (`YEAR(activity_timeframe)`) and as the daily date filter. Its type and time zone handling in Delta Lake determine whether the year/date filters behave as intended — verify against the source table definition before the first run.
- **Types:** the notebooks pass values through without casting. Column types are therefore whatever Spark infers from Delta Lake and whatever `mysql-connector-python` maps them to in MySQL. A type mismatch surfaces as a MySQL error inside an executor partition, not as a driver-side validation error.

Sample data is included as `data/sample_leads_data.csv`: 10 synthetic rows with the same 30 columns, invented names and 555 phone numbers, for checking the schema and file shape only. It is test data, confirmed synthetic by the author, not real customer data.

---

## 6. Configuration

| Parameter | Where it is set | Default | Notes |
|---|---|---|---|
| `activity_timeframe` | Workflow parameter / widget | `hist` | `hist` truncates and reloads. `daily` loads yesterday only. |
| `chunk_size` | Workflow parameter / widget | `50000` | Insert batch size per partition; also drives partition sizing (§4.2). |
| `env` | Workflow parameter / widget | `dev` | `dev`/`qa` suffix the MySQL database name. |
| `staging_database`, `leads_table` | Workflow parameter / widget | `staging_db`, `leads_detailrpt_daily` | Source Delta and target MySQL names. |
| `dataset`, `appName`, `pipeline_*` | Workflow parameter / widget | See YAML | Passed through for logging/identification; not used to select data. |
| `ops_database`, `ops_table`, `status_database`, `status_tbl` | Workflow parameter | `ops`, `ops_application_metadata`, `ops`, `tbl_run_status` | Accepted by the workflow but not written to by these notebooks. |
| Schedule | Workflow YAML | 00:30 America/New_York, `PAUSED` | Unpause to run daily. The pause status is in the YAML as supplied. |
| `run_as` | Workflow YAML | `<your-service-principal-id>` | Placeholder — replace before deploying. |
| Secret Scope | Databricks | `adls{env}scope` | Keys: `mysqlusername`, `mysqlpassword`, `mysqlhost`, `mysqlport`. |

---

## 7. Failure modes, gaps, and limitations

Read this section before operating the pipeline. Each item is visible in the supplied files.

1. **Readiness notebook is missing from this repository.** The workflow's Task 1 points at `/Workspace/projects/leads_datacopy/check_data_copy_readiness`, and Task 2 depends on its `table_count` task value. That notebook was not supplied with this project. Without it, the workflow as defined cannot run. Either add it at that path or replace Task 1–2 with a check that exists in your workspace.
2. **`shared_functions` wheel is referenced but not included.** The workflow environment lists `/Volumes/uc_dev/default/jars/shared_functions-0.8-py3-none-any.whl`. It is not in this repository and neither copy notebook imports it. Remove the dependency or point it at your own wheel.
3. **Daily dedup fallback is broad.** In v2, *any* exception from the JDBC read is caught and treated as "no existing records — insert everything". A MySQL outage, a wrong host, or a driver error therefore looks exactly like an empty target table. After any daily run that inserts an unexpectedly large number of rows, check whether the JDBC read actually succeeded.
4. **Historical mode is destructive and is the default.** `hist` truncates the target table and the workflow default is `hist`. A mistaken manual trigger against the production database empties the reporting table until the reload finishes. Confirm `env`, database, and table before running `hist` anywhere but dev/QA.
5. **Re-running `hist` is a full reload, not a resume.** A failure part-way through the year loop leaves a partially loaded table (the truncate already happened). Re-running reloads everything from the truncate again; there is no checkpoint that skips completed years.
6. **Unrecognized mode is silent.** If `activity_timeframe` is neither `hist` nor `daily`, neither branch runs and the notebook still prints `Status: SUCCESS` with zero records. Validate the parameter in the workflow or in the notebook before relying on the status line.
7. **Fixed EST offset for "yesterday".** Daily mode computes yesterday as UTC − 5 hours − 1 day. That offset does not change with daylight saving time, while the workflow schedule is in `America/New_York`, which does. Around DST transitions, verify the target date printed in the logs matches the intended business date.
8. **No tests in this repository.** There are no unit or integration tests for the insert function, the partition sizing, or the anti-join. The v1 notebook serves as a reference implementation, not as a test.
9. **No status-table write.** Despite the `status_*` / `ops_*` parameters, neither notebook writes run status to a table. Operational visibility comes from workflow run history and the printed summary only.

---

## 8. Performance — what is known and what is not

**Known from the code.** v2 removes two driver-side operations that v1 performed on every run: collecting all rows to the driver for inserts, and fetching all existing IDs to the driver for dedup. By construction, v2's driver memory does not grow with row count or ID count in those steps, and its inserts run in parallel across up to 20 partitions.

**Not known from the supplied files.** No timed run, row count, duration, driver-memory measurement, or MySQL connection measurement was supplied. In particular, the figure "~400MB driver memory for 50K rows" in the earlier architecture draft and the claim of scaling "to millions of IDs" are **not treated as measured results in this document**, because no benchmark, log, or test behind them was supplied. Do not quote them as results.

**How to measure it.** Run v1 and v2 against the same data volume in the same environment and record, per run: total rows, total duration (printed by both notebooks), peak driver memory from the cluster metrics, and peak MySQL connections. Vary `chunk_size` and observe the partition count printed via the row count and the resulting connection count. Only then add a performance table with measured numbers, labelled with the data volume, environment, and date of the run.

---

## 9. Deployment and operations checklist

- [ ] Readiness notebook exists at `/Workspace/projects/leads_datacopy/check_data_copy_readiness` and sets `table_count` to `"1"` / `"0"`.
- [ ] Secret Scope `adls{env}scope` exists in the workspace that runs the workflow, with all four MySQL keys.
- [ ] `<your-service-principal-id>` in the YAML is replaced, and the service principal can read the Delta table and write to MySQL.
- [ ] The `shared_functions` wheel reference is removed or corrected (§7.2).
- [ ] Target MySQL database/table, `env`, and mode (`hist` vs `daily`) are confirmed — especially before any `hist` run.
- [ ] Schedule is unpaused only when daily mode is intended; otherwise leave it paused and trigger manually.
- [ ] First run is in dev/QA, with row counts checked in MySQL afterwards and the printed duration and record count recorded.
- [ ] After a daily run, check the printed "New records to insert" count against expectations; an unexpectedly large count may indicate the JDBC fallback in §7.3.

---

## 10. File inventory

| Path | What it is | Changed in this repository? |
|---|---|---|
| `notebooks/leads_data_copy_v1_before_optimization.py` | Original driver-side implementation, kept for comparison | No — as supplied |
| `notebooks/leads_data_copy_v2_optimized.py` | Production optimized implementation | No — as supplied |
| `workflows/wf_leads_datacopy.yml` | Databricks Workflow definition (gate + copy) | No — as supplied, including `PAUSED` schedule and placeholder service principal |
| `data/sample_leads_data.csv` | Synthetic sample: 10 invented rows, same 30 columns | No — as supplied; confirmed synthetic test data by the author |
| `docs/architecture.md` | This document | **Yes — rewritten** at the author's request to be professional and checkable; see the status note at the top |
| `README.md` | Project README | **Yes — rewritten** for recruiters, with the same facts and limits as this document |
| `requirements.txt`, `.gitignore`, `LICENSE` | Professional extras | Added — see the README approval note; no code depends on them at run time |

---

## Author

**Sanjeev Kumar Pandey** — Data Engineer → AI Engineer | Azure Databricks | PySpark | RAG | Agentic AI

[LinkedIn](https://www.linkedin.com/in/sanjeev-pandey-7a45831ba) · [GitHub](https://github.com/sanjeevpandeydata)
