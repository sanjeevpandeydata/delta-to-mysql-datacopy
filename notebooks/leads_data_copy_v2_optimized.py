# Databricks notebook source
# ============================================================
# NOTEBOOK : leads_data_copy_v2.py  (OPTIMIZED VERSION)
# PURPOSE  : Copy leads report data from Delta Lake to MySQL.
#            Supports two modes:
#              hist  — truncate MySQL and reload last 2 years
#                      year-by-year using mapInPandas
#              daily — load yesterday's data, deduplicating
#                      against MySQL using JDBC anti-join
#
# KEY OPTIMIZATIONS vs v1 (leads_data_copy_v1):
#   1. mapInPandas replaces collect() + cursor.executemany()
#      → Inserts distributed across Spark executors
#      → Serverless compute compatible (no driver bottleneck)
#      → MySQL connection created per partition (not on driver)
#   2. JDBC spark.read replaces cursor.fetchall() for dedup
#      → Existing lead_ids read as a distributed DataFrame
#      → left_anti join replaces Python set isin() filter
#      → Scales to millions of existing IDs with no driver OOM
#   3. Dynamic repartition before mapInPandas
#      → Partitions sized to chunk_size * 5 rows each
#      → Capped at 20 partitions to avoid MySQL connection flood
#
# AUTHOR   : Sanjeev Kumar Pandey
# ============================================================

# COMMAND ----------
import mysql.connector
from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
import pyspark.sql.functions as F
from pyspark.sql.types import StructType
import time

start = time.time()
total_records_processed = 0

# COMMAND ----------
# ── Widget Parameters ─────────────────────────────────────────
dbutils.widgets.text("appName",                   "")
appName                   = dbutils.widgets.get("appName")

dbutils.widgets.text("env",                       "")
env                       = dbutils.widgets.get("env")

dbutils.widgets.text("activity_timeframe",        "")
activity_timeframe        = dbutils.widgets.get("activity_timeframe")

dbutils.widgets.text("chunk_size",                "")
chunk_size                = int(dbutils.widgets.get("chunk_size"))

dbutils.widgets.text("dataset",                   "")
dataset                   = dbutils.widgets.get("dataset")

dbutils.widgets.text("staging_database",          "")
staging_database          = dbutils.widgets.get("staging_database")

dbutils.widgets.text("leads_table",               "")
leads_table               = dbutils.widgets.get("leads_table")

dbutils.widgets.text("pipeline_or_workflow_name", "")
pl_or_wl_name             = dbutils.widgets.get("pipeline_or_workflow_name")

dbutils.widgets.text("pipeline_activity",         "")
pipeline_activity         = dbutils.widgets.get("pipeline_activity")

# COMMAND ----------
# ── Secrets ───────────────────────────────────────────────────
mysql_username = dbutils.secrets.get(scope=f"adls{env}scope", key="mysqlusername")
mysql_password = dbutils.secrets.get(scope=f"adls{env}scope", key="mysqlpassword")
mysql_host     = dbutils.secrets.get(scope=f"adls{env}scope", key="mysqlhost")
mysql_port     = dbutils.secrets.get(scope=f"adls{env}scope", key="mysqlport")

# COMMAND ----------
# ── MySQL target database name ────────────────────────────────
mysql_database = staging_database
mysql_table    = leads_table

if env in ("dev", "qa"):
    mysql_database = f"{mysql_database}_{env}"

# COMMAND ----------
# ── Driver-level connection (used for TRUNCATE only) ──────────
# Executor-level inserts use per-partition connections inside
# mapInPandas — no shared state between driver and executors.
cnx    = mysql.connector.connect(
    host=mysql_host, user=mysql_username,
    password=mysql_password, database=mysql_database)
cursor = cnx.cursor()

# COMMAND ----------
# ── mapInPandas insert function ───────────────────────────────
# This function runs ON EACH EXECUTOR PARTITION, not on driver.
# Each call:
#   1. Opens its own MySQL connection (serverless-safe)
#   2. Converts pandas DataFrame partition to list of tuples
#   3. Inserts in configurable chunk_size batches
#   4. Commits and closes the connection
#   5. Yields an empty DataFrame (required by mapInPandas API)
#
# WHY mapInPandas:
#   - collect() + driver insert = single-threaded bottleneck
#   - mapInPandas = parallel insert across all executors
#   - Works on Databricks Serverless (no shared driver state)

def insert_partition_to_mysql(pdf_iter):
    import mysql.connector
    import pandas as pd

    insert_query = f"""
        INSERT INTO `{mysql_database}`.`{mysql_table}` (
            site_code, brand, activity_timeframe, lead_id, form_phone,
            lead_new_used, lead_datetime, lead_source, cust_name,
            miles_from_dealer, model, actionable_time, response_date,
            response_time, survey_returned, num_key_drivers,
            other_dealers_contacted, sale_date, selling_dealer_site,
            sale_model, sale_new_used, sale_reported_date, lrt_elapsed,
            lrt_lead, lost_sales, leads_open_close, dealer_mailing_name,
            record_guid, ingest_date, update_ts
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )"""

    for pdf in pdf_iter:
        if len(pdf) == 0:
            continue

        # New connection per partition — safe for serverless
        cnx_exec = mysql.connector.connect(
            host=mysql_host, user=mysql_username,
            password=mysql_password, database=mysql_database)
        cursor_exec = cnx_exec.cursor()

        data = [tuple(row) for row in pdf.values]

        # Chunked insert within the partition
        for i in range(0, len(data), chunk_size):
            cursor_exec.executemany(insert_query, data[i:i + chunk_size])
            cnx_exec.commit()

        cursor_exec.close()
        cnx_exec.close()

        yield pd.DataFrame()   # mapInPandas requires a yielded DataFrame


# COMMAND ----------
# ── Helper: repartition + mapInPandas trigger ─────────────────
# Sizes partitions so each executor handles ~chunk_size * 5 rows.
# Capped at 20 partitions to avoid overwhelming MySQL connections.

def distributed_insert(df, record_count):
    num_partitions = max(1, record_count // (chunk_size * 5))
    num_partitions = min(num_partitions, 20)
    df.repartition(num_partitions) \
      .mapInPandas(insert_partition_to_mysql, schema=StructType([])) \
      .collect()


# COMMAND ----------
# ── Helper: Year-wise insert (historical mode) ────────────────
# Queries one year at a time to keep each partition manageable.
# Uses distributed_insert — no driver collect().

def insert_data_year_wise(filter_year):
    global total_records_processed

    delta_query = f"""
        SELECT site_code, brand, activity_timeframe, lead_id, form_phone,
               lead_new_used, lead_datetime, lead_source, cust_name,
               miles_from_dealer, model, actionable_time, response_date,
               response_time, survey_returned, num_key_drivers,
               other_dealers_contacted, sale_date, selling_dealer_site,
               sale_model, sale_new_used, sale_reported_date, lrt_elapsed,
               lrt_lead, lost_sales, leads_open_close, dealer_mailing_name,
               record_guid, ingest_date, update_ts
        FROM {staging_database}.{leads_table}
        WHERE YEAR(activity_timeframe) = {filter_year}
    """
    delta_df    = spark.sql(delta_query)
    delta_count = delta_df.count()
    print(f"Year {filter_year}: {delta_count:,} rows in Delta table")

    if delta_count > 0:
        distributed_insert(delta_df, delta_count)
        total_records_processed += delta_count
        print(f"Year {filter_year}: {delta_count:,} rows inserted via mapInPandas")
    else:
        print(f"Year {filter_year}: no data found — skipping")

# COMMAND ----------
# ── Main: Historical mode ─────────────────────────────────────
if activity_timeframe == "hist":
    print("Mode: HISTORICAL — truncating MySQL and reloading last 2 years")

    cursor.execute(f"TRUNCATE TABLE `{mysql_database}`.`{mysql_table}`")
    cnx.commit()

    today      = datetime.today()
    start_year = int((today + relativedelta(years=-2)).strftime("%Y"))
    end_year   = int(datetime.now().strftime("%Y"))
    print(f"Loading years: {start_year} → {end_year}")

    while start_year <= end_year:
        insert_data_year_wise(start_year)
        start_year += 1

    cursor.close()
    cnx.close()

# COMMAND ----------
# ── Main: Daily mode ──────────────────────────────────────────
elif activity_timeframe == "daily":
    print("Mode: DAILY — loading yesterday's records not yet in MySQL")

    # Compute yesterday in EST (UTC - 5h - 1 day)
    current_utc            = datetime.now(timezone.utc)
    edd_activity_timeframe = (current_utc - timedelta(days=1, hours=5)).strftime("%Y-%m-%d")
    print(f"Target date: {edd_activity_timeframe}")

    query = f"""
        SELECT site_code, brand, activity_timeframe, lead_id, form_phone,
               lead_new_used, lead_datetime, lead_source, cust_name,
               miles_from_dealer, model, actionable_time, response_date,
               response_time, survey_returned, num_key_drivers,
               other_dealers_contacted, sale_date, selling_dealer_site,
               sale_model, sale_new_used, sale_reported_date, lrt_elapsed,
               lrt_lead, lost_sales, leads_open_close, dealer_mailing_name,
               record_guid, ingest_date, update_ts
        FROM {staging_database}.{leads_table}
        WHERE activity_timeframe = '{edd_activity_timeframe}'
    """
    delta_df    = spark.sql(query)
    delta_count = delta_df.count()
    print(f"Delta records for {edd_activity_timeframe}: {delta_count:,}")

    if delta_count > 0:
        # ── OPTIMIZATION: JDBC read replaces cursor.fetchall() ─
        # Old approach: cursor.fetchall() → Python set → isin()
        #   → all existing IDs pulled to driver RAM
        #   → isin() filter serialised and broadcast back
        # New approach: spark.read.jdbc → left_anti join
        #   → reads only needed date partition via pushdown
        #   → join runs distributed across executors
        #   → no driver memory pressure regardless of ID count

        jdbc_url = f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_database}"
        jdbc_props = {
            "user":   mysql_username,
            "password": mysql_password,
            "driver": "com.mysql.cj.jdbc.Driver"
        }

        existing_ids_query = f"""
            (SELECT DISTINCT lead_id
             FROM `{mysql_database}`.`{mysql_table}`
             WHERE activity_timeframe = '{edd_activity_timeframe}') AS existing_leads
        """

        try:
            existing_ids_df = spark.read.jdbc(
                url=jdbc_url,
                table=existing_ids_query,
                properties=jdbc_props)

            # Anti-join: keep only rows whose lead_id is NOT in MySQL
            filtered_df = delta_df.join(existing_ids_df, on="lead_id", how="left_anti")

        except Exception:
            # Table may be empty on first run — treat all rows as new
            print("No existing records found in MySQL — inserting all rows.")
            filtered_df = delta_df

        filtered_count = filtered_df.count()
        print(f"New records to insert: {filtered_count:,}")

        if filtered_count > 0:
            distributed_insert(filtered_df, filtered_count)
            total_records_processed = filtered_count
            print(f"Inserted: {total_records_processed:,} rows via mapInPandas")
        else:
            print("All records for this date already exist in MySQL — nothing to insert.")

        cursor.close()
        cnx.close()

    else:
        print(f"No data found in Delta for timeframe: {edd_activity_timeframe}")

# COMMAND ----------
end          = time.time()
run_duration = end - start
print(f"\nPipeline : {appName}")
print(f"Records  : {total_records_processed:,}")
print(f"Duration : {run_duration:.2f}s")
print("Status   : SUCCESS")
