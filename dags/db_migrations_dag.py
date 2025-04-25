import os
import datetime
from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.utils.task_group import TaskGroup
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

args = {
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
    'execution_timeout': timedelta(seconds=300),
}

with DAG(
    dag_id="db_migrations_dag",
    start_date=datetime.datetime(2024, 10, 13),
    schedule="@once",
    default_args=args,
    tags=['DB migrations'],
    catchup=False,
) as dag:
    for i, path in enumerate(sorted((Path.cwd() / Path("dags") / Path("sql_migrations")).glob("*.sql"))):
        file_name = path.stem
        migration_task = SQLExecuteQueryOperator(
            task_id=f"{file_name}",
            conn_id="dwh",
            sql=path.open().read(),
        )
        if i>0:
            migration_task.set_upstream(previous_task)
        previous_task = migration_task