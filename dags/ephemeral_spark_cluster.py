from datetime import datetime

from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from emr_config import JOB_FLOW_OVERRIDES, CLUSTER_ID, SPARK_STEPS, STEP_ID

def fail_dag_run():
    raise RuntimeError("The Spark job failed. The cluster was still terminated..")


with DAG(
    dag_id="freight_route_metrics_emr",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
    tags=["emr", "spark", "ephemeral"],
) as dag:
    create_cluster = EmrCreateJobFlowOperator(
        task_id="create_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        emr_conn_id=None,
        aws_conn_id="aws_default",
        region_name="eu-central-1"
    )

    add_spark_step = EmrAddStepsOperator(
        task_id="add_spark_step",
        job_flow_id=CLUSTER_ID,
        steps=SPARK_STEPS,
        region_name="eu-central-1",
        aws_conn_id="aws_default"
    )

    watch_spark_step = EmrStepSensor(
        task_id="watch_spark_step",
        job_flow_id=CLUSTER_ID,
        step_id=STEP_ID,
        mode="reschedule",
        poke_interval=30,
        timeout=3 * 60 * 60,
        region_name="eu-central-1",
        aws_conn_id="aws_default",
    )

    terminate_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_cluster",
        job_flow_id=CLUSTER_ID,
        trigger_rule="all_done",
        region_name="eu-central-1",
        aws_conn_id="aws_default"
    )

    mark_run_failed = PythonOperator(
        task_id="mark_run_failed",
        python_callable=fail_dag_run,
        trigger_rule="one_failed",
    )

    create_cluster >> add_spark_step >> watch_spark_step >> terminate_cluster

    # terminate_cluster always succeeds, so this branch is what turns a failed job red.
    watch_spark_step >> mark_run_failed