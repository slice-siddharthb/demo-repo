"""
Airflow DAG to alert when a senior citizen is charged for cheque-book orders.

Runbook:
1) Query Spark/Hive tables for charged CHEQUE_BOOK orders.
2) Resolve customer UUID -> CIF -> customer_ind_info.date_of_birth (decrypted).
3) Identify senior citizens (age > 60 at order time, IST).
4) Send Slack alert and upload CSV containing distinct customer_ids.
5) Persist alerted order_ids to a log table to avoid duplicate alerts.
"""

from __future__ import annotations

import os
import shutil
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


def _single_csv_from_spark_dir(spark_output_dir: str, final_csv_path: str) -> None:
    """Move Spark's generated part file into a deterministic CSV path."""
    output_path = Path(spark_output_dir)
    part_files = sorted(output_path.glob("part-*.csv"))
    if not part_files:
        raise AirflowException(f"No Spark CSV part file found in {spark_output_dir}")
    Path(final_csv_path).parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(part_files[0]), final_csv_path)
    shutil.rmtree(spark_output_dir, ignore_errors=True)


def detect_and_alert(**context) -> None:
    """Detect violations, post Slack alert, upload CSV, and log alerted orders."""
    # Import Spark only when task runs (keeps DAG parse lightweight).
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import current_timestamp, lit

    slack_channel_id = Variable.get(
        "SENIOR_CHEQUEBOOK_SLACK_CHANNEL_ID", default_var="C0AG94SB9DW"
    )
    slack_token = Variable.get(
        "SENIOR_CHEQUEBOOK_SLACK_BOT_TOKEN",
        default_var=os.getenv("SENIOR_CHEQUEBOOK_SLACK_BOT_TOKEN", ""),
    )
    if not slack_token:
        raise AirflowException(
            "Missing Slack token. Set Airflow Variable "
            "'SENIOR_CHEQUEBOOK_SLACK_BOT_TOKEN' or env "
            "'SENIOR_CHEQUEBOOK_SLACK_BOT_TOKEN'."
        )

    lookback_hours = int(Variable.get("SENIOR_CHEQUEBOOK_LOOKBACK_HOURS", default_var="24"))
    local_tmp_dir = Variable.get(
        "SENIOR_CHEQUEBOOK_LOCAL_TMP_DIR",
        default_var="/tmp/senior_chequebook_alerts",
    )
    Path(local_tmp_dir).mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder.appName("senior-chequebook-charge-alert")
        .enableHiveSupport()
        .getOrCreate()
    )
    violations_df = None

    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS monitoring")
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS monitoring.senior_cb_alert_log (
                order_id STRING,
                uuid STRING,
                customer_id BIGINT,
                alerted_at TIMESTAMP,
                order_created_at TIMESTAMP,
                dag_run_id STRING
            )
            USING parquet
            """
        )

        query = f"""
    WITH latest_tracking AS (
        SELECT order_id, user_id, status
        FROM (
            SELECT
                order_id,
                user_id,
                status,
                ROW_NUMBER() OVER (
                    PARTITION BY order_id
                    ORDER BY updated_at DESC, created_at DESC
                ) AS rn
            FROM druid_gold.services_trackings
            WHERE user_id IS NOT NULL
              AND TRIM(user_id) <> ''
              AND (COALESCE(__is_deleted, false) = false)
        ) t
        WHERE rn = 1
    ),
    base_orders AS (
        SELECT
            o.order_id,
            o.created_at AS order_created_at,
            from_utc_timestamp(o.created_at, 'Asia/Kolkata') AS order_created_at_ist,
            to_date(from_utc_timestamp(o.created_at, 'Asia/Kolkata')) AS order_date_ist,
            o.payment_eligible
        FROM druid_gold.services_orders o
        WHERE o.type = 'CHEQUE_BOOK'
          AND o.payment_eligible = true
          AND o.created_at >= current_timestamp() - INTERVAL {lookback_hours} HOURS
          AND (COALESCE(o.__is_deleted, false) = false)
    ),
    joined AS (
        SELECT
            b.order_id,
            b.order_created_at,
            b.order_created_at_ist,
            b.order_date_ist,
            b.payment_eligible,
            lt.user_id AS uuid,
            lt.status,
            CAST(c.cif AS BIGINT) AS customer_id,
            decryptFunction(ci.date_of_birth) AS decrypted_dob
        FROM base_orders b
        JOIN latest_tracking lt
          ON lt.order_id = b.order_id
        LEFT JOIN uid_db_gold.customers c
          ON CAST(c.id AS STRING) = CAST(lt.user_id AS STRING)
         AND (COALESCE(c.__is_deleted, false) = false)
        LEFT JOIN bsgcrm_gold_pii.customer_ind_info ci
          ON CAST(c.cif AS BIGINT) = ci.customer_id
         AND (COALESCE(ci.__is_deleted, false) = false)
    ),
    with_dob AS (
        SELECT
            order_id,
            order_created_at,
            order_created_at_ist,
            order_date_ist,
            payment_eligible,
            uuid,
            status,
            customer_id,
            COALESCE(
                to_date(decrypted_dob, 'yyyy-MM-dd'),
                to_date(decrypted_dob, 'dd-MM-yyyy'),
                to_date(decrypted_dob, 'dd/MM/yyyy'),
                to_date(decrypted_dob, 'MM/dd/yyyy'),
                to_date(decrypted_dob)
            ) AS dob
        FROM joined
    ),
    violations AS (
        SELECT
            w.order_id,
            w.order_created_at,
            w.order_created_at_ist,
            w.status,
            w.uuid,
            w.customer_id,
            w.dob,
            CAST(FLOOR(months_between(w.order_date_ist, w.dob) / 12) AS INT) AS age_years
        FROM with_dob w
        WHERE w.dob IS NOT NULL
          AND CAST(FLOOR(months_between(w.order_date_ist, w.dob) / 12) AS INT) > 60
    )
    SELECT v.*
    FROM violations v
    LEFT ANTI JOIN monitoring.senior_cb_alert_log l
      ON l.order_id = v.order_id
    """

        violations_df = spark.sql(query).cache()
        violation_count = violations_df.count()

        if violation_count == 0:
            return

        run_id = context["run_id"]
        ts_nodash = context["ts_nodash"]

        csv_spark_dir = os.path.join(local_tmp_dir, f"customer_ids_spark_{uuid.uuid4().hex}")
        csv_final_path = os.path.join(local_tmp_dir, f"senior_chequebook_customer_ids_{ts_nodash}.csv")

        (
            violations_df.select("customer_id")
            .where("customer_id IS NOT NULL")
            .distinct()
            .coalesce(1)
            .write.mode("overwrite")
            .option("header", "true")
            .csv(csv_spark_dir)
        )
        _single_csv_from_spark_dir(csv_spark_dir, csv_final_path)

        sample_rows = (
            violations_df.select(
                "order_id", "customer_id", "uuid", "age_years", "status", "order_created_at_ist"
            )
            .orderBy("order_created_at_ist")
            .limit(15)
            .collect()
        )
        sample_lines = [
            (
                f"- order_id={row['order_id']}, customer_id={row['customer_id']}, "
                f"uuid={row['uuid']}, age={row['age_years']}, status={row['status']}, "
                f"order_created_at_ist={row['order_created_at_ist']}"
            )
            for row in sample_rows
        ]

        slack_text = (
            "🚨 *Senior citizen cheque-book charge detected*\n"
            f"*Violations in this run:* {violation_count}\n"
            f"*Lookback window:* last {lookback_hours} hours\n"
            f"*DAG Run:* `{run_id}`\n\n"
            "*Sample rows:*\n"
            + "\n".join(sample_lines)
        )

        try:
            client = WebClient(token=slack_token)
            client.chat_postMessage(channel=slack_channel_id, text=slack_text)
            client.files_upload_v2(
                channel=slack_channel_id,
                title=f"senior_chequebook_customer_ids_{ts_nodash}.csv",
                initial_comment="CSV contains distinct customer_ids for this alert run.",
                file=csv_final_path,
                filename=Path(csv_final_path).name,
            )
        except SlackApiError as exc:
            raise AirflowException(f"Slack API call failed: {exc.response['error']}") from exc
        finally:
            if os.path.exists(csv_final_path):
                os.remove(csv_final_path)

        (
            violations_df.select("order_id", "uuid", "customer_id", "order_created_at")
            .withColumn("alerted_at", current_timestamp())
            .withColumn("dag_run_id", lit(run_id))
            .write.mode("append")
            .insertInto("monitoring.senior_cb_alert_log")
        )
    finally:
        if violations_df is not None:
            violations_df.unpersist()
        spark.stop()


with DAG(
    dag_id="senior_chequebook_charge_alert",
    description="Alert when senior citizen cheque-book orders are charged",
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "risk-monitoring",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["chequebook", "senior-citizen", "slack-alert"],
) as dag:
    detect_and_alert_task = PythonOperator(
        task_id="detect_and_alert_senior_chequebook_charges",
        python_callable=detect_and_alert,
    )

    detect_and_alert_task
