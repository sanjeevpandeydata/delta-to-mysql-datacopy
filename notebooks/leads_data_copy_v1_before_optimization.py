# Databricks notebook source
# ============================================================
# NOTEBOOK : leads_data_copy_v1.py  (OLD VERSION — before optimization)
# PURPOSE  : Copy leads report data from Delta Lake to MySQL.
#            Supports two modes:
#              hist  — truncate MySQL and reload last 2 years
#                      year-by-year
#              daily — load yesterday's data, skipping
#                      leadids already present in MySQL
#
# ⚠  LIMITATION: Uses driver.collect() + cursor.executemany()
#    for MySQL inserts. All data pulled to driver node — not
#    suitable for large volumes or serverless compute.
#    See leads_data_copy_v2.py for the optimized version.
#
# AUTHOR   : Sanjeev Kumar Pandey
# ============================================================

# COMMAND ----------
import mysql.connector
from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
import pyspark.sql.functions as F
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
# In dev/qa environments a suffix is appended to the DB name
# so dev and prod MySQL stay isolated.
mysql_database = staging_database
mysql_table    = leads_table

if env in ("dev", "qa"):
    mysql_database = f"{mysql_database}_{env}"

# COMMAND ----------
# ── MySQL connection ──────────────────────────────────────────
cnx = mysql.connector.connect(
    host=mysql_host,
    user=mysql_username,
    password=mysql_password,
    database=mysql_database
)
cursor = cnx.cursor()

# COMMAND ----------
# ── INSERT statement ──────────────────────────────────────────
insert_query = f"""
    INSERT INTO `{mysql_database}`.`{mysql_table}` (
        site_code, brand, activity_timeframe, lead_id, form_phone,
        lead_new_used, lead_datetime, lead_source, cust_name, miles_from_dealer,
        model, actionable_time, response_date, response_time, survey_returned,
        num_key_drivers, other_dealers_contacted, sale_date, selling_dealer_site,
        sale_model, sale_new_used, sale_reported_date, lrt_elapsed, lrt_lead,
        lost_sales, leads_open_close, dealer_mailing_name, record_guid,
        ingest_date, update_ts
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )"""

# COMMAND ----------
# ── Helper: Chunked batch insert ─────────────────────────────
# ⚠ DRIVER-SIDE: entire year's data collected to driver node.
# Acceptable for small volumes but becomes a bottleneck at scale.

def insert_data_in_batches(data):
    inserted_rows = 0
    for start_idx in range(0, len(data), chunk_size):
        end_idx    = min(start_idx + chunk_size, len(data))
        chunk_data = data[start_idx:end_idx]
        cursor.executemany(insert_query, chunk_data)
        cnx.commit()
        inserted_rows += len(chunk_data)
    return inserted_rows

# COMMAND ----------
# ── Helper: Year-wise insert (historical mode) ────────────────
# Queries one year at a time to avoid pulling all years into
# memory at once. Still driver-side via collect().

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

    data_to_insert = [
        (
            row.site_code, row.brand, row.activity_timeframe, row.lead_id,
            row.form_phone, row.lead_new_used, row.lead_datetime,
            row.lead_source, row.cust_name, row.miles_from_dealer, row.model,
            row.actionable_time, row.response_date, row.response_time,
            row.survey_returned, row.num_key_drivers,
            row.other_dealers_contacted, row.sale_date,
            row.selling_dealer_site, row.sale_model, row.sale_new_used,
            row.sale_reported_date, row.lrt_elapsed, row.lrt_lead,
            row.lost_sales, row.leads_open_close, row.dealer_mailing_name,
            row.record_guid, row.ingest_date, row.update_ts
        )
        for row in delta_df.collect()   # ⚠ DRIVER COLLECT
    ]

    year_records            = insert_data_in_batches(data_to_insert)
    total_records_processed += year_records
    print(f"Year {filter_year}: {year_records:,} rows inserted into MySQL")

# COMMAND ----------
# ── Main: Historical mode ─────────────────────────────────────
if activity_timeframe == "hist":
    print("Mode: HISTORICAL — truncating MySQL and reloading last 2 years")

    cursor.execute(f"TRUNCATE TABLE `{mysql_database}`.`{mysql_table}`")

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
    current_utc          = datetime.now(timezone.utc)
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
        # ⚠ DRIVER-SIDE dedup: fetch existing lead IDs via cursor
        cursor.execute(
            f"SELECT DISTINCT lead_id FROM `{mysql_database}`.`{mysql_table}` "
            f"WHERE activity_timeframe = '{edd_activity_timeframe}'"
        )
        existing_lead_ids = set(row[0] for row in cursor.fetchall())

        # Filter out records already in MySQL
        filtered_df    = delta_df.filter(~delta_df.lead_id.isin(existing_lead_ids))
        filtered_count = filtered_df.count()
        print(f"New records to insert: {filtered_count:,}")

        if filtered_count > 0:
            data_to_insert = [
                (
                    row.site_code, row.brand, row.activity_timeframe,
                    row.lead_id, row.form_phone, row.lead_new_used,
                    row.lead_datetime, row.lead_source, row.cust_name,
                    row.miles_from_dealer, row.model, row.actionable_time,
                    row.response_date, row.response_time, row.survey_returned,
                    row.num_key_drivers, row.other_dealers_contacted,
                    row.sale_date, row.selling_dealer_site, row.sale_model,
                    row.sale_new_used, row.sale_reported_date, row.lrt_elapsed,
                    row.lrt_lead, row.lost_sales, row.leads_open_close,
                    row.dealer_mailing_name, row.record_guid,
                    row.ingest_date, row.update_ts
                )
                for row in filtered_df.collect()   # ⚠ DRIVER COLLECT
            ]

            cursor.executemany(insert_query, data_to_insert)
            cnx.commit()
            total_records_processed = len(data_to_insert)
            print(f"Inserted: {total_records_processed:,} rows")

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
