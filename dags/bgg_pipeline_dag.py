from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src import tasks

RUN = "{{ run_id }}"  

default_args = {"owner": "fernand", "retries": 2, "retry_delay": timedelta(seconds=30)}

with DAG(
    dag_id="bgg_pipeline",
    description="BGG raw -> staging -> curated -> PostgreSQL -> model -> metric comparison",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    params={"mode": "full", "batch_size": 5000, "source_path": ""},
    tags=["dss150p", "bgg"],
) as dag:
    verify = PythonOperator(task_id="verify_source", python_callable=tasks.verify_source,
                            op_kwargs={"run_id": RUN, "source": "{{ params.source_path }}"})
    ingest = PythonOperator(task_id="ingest_raw", python_callable=tasks.ingest_raw,
                            op_kwargs={"run_id": RUN, "mode": "{{ params.mode }}",
                                       "batch_size": "{{ params.batch_size }}",
                                       "source": "{{ params.source_path }}"})
    staging = PythonOperator(task_id="build_staging", python_callable=tasks.build_staging_task,
                             op_kwargs={"run_id": RUN})
    curated = PythonOperator(task_id="build_curated", python_callable=tasks.build_curated_task,
                             op_kwargs={"run_id": RUN})
    load = PythonOperator(task_id="load_postgres", python_callable=tasks.load_postgres,
                          op_kwargs={"run_id": RUN})
    feats = PythonOperator(task_id="build_features", python_callable=tasks.build_features_task,
                           op_kwargs={"run_id": RUN})
    train = PythonOperator(task_id="train_model", python_callable=tasks.train_model,
                           op_kwargs={"run_id": RUN})
    compare = PythonOperator(task_id="compare_metrics", python_callable=tasks.compare_metrics_task,
                             op_kwargs={"run_id": RUN})
    finalize = PythonOperator(task_id="finalize_run", python_callable=tasks.finalize_run,
                              op_kwargs={"run_id": RUN}, trigger_rule="all_done")

    verify >> ingest >> staging >> curated >> load >> feats >> train >> compare >> finalize