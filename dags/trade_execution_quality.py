import os
import json

from enum import Enum
from datetime import timedelta, datetime
from pathlib import Path
import logging
from string import Template

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

import pandas as pd

from include.binance_client import BinanceClient
from include.dune_analytics_client import DuneAnalyticsClient

class IngestInputs(Enum):
	COW_TRADES = "cow_trades"
	BINANCE_TRADES = "binance_trades"

# Logger Part:
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.info("Starting the DAG")

# Arguments:
args = {
    'retries': 1,
    'retry_delay': timedelta(seconds=1),
    'execution_timeout': timedelta(seconds=300),
}

# All available ingest inputs:
ingest_inputs = [elem.value for elem in IngestInputs]

CONN_NAME = "dwh"
API_KEY = os.getenv("DUNE_ANALYTICS_API_KEY")

# This data was used to mock and test:
# def read_json_to_dataframe(task_name, **kwargs):
# 	match task_name:
# 		case IngestInputs.COW_TRADES.value:
# 			file_path = '/opt/airflow/dags/output.csv'
# 			df = pd.read_csv(file_path)
# 		case IngestInputs.BINANCE_TRADES.value:
# 			file_path = '/opt/airflow/dags/output3.json'
# 			df = pd.read_json(file_path)
# 		case _:
# 			raise ValueError("No such task_name")
# 	return df

def ingest_data(task_name, **kwargs):
	logical_date = kwargs['data_interval_start']
	date_start, date_end = get_range_from_logical_date(logical_date)
	match task_name:
		case IngestInputs.COW_TRADES.value:
			dune_analytics_client = DuneAnalyticsClient(logger, API_KEY)
			cow_trades = dune_analytics_client.get_latest_result(date_start, date_end)
			return cow_trades
		case IngestInputs.BINANCE_TRADES.value:
			binance_client = BinanceClient(logger)
			trades = binance_client.get_historical_trades("ETHUSDC", 1000, date_start, date_end)
			return pd.DataFrame(trades)
		case _:
			raise ValueError("No such task_name")
	return None

def get_range_from_logical_date(logical_date: datetime) -> tuple:
	date_end = logical_date.replace(hour=0, minute=0, second=0)
	date_start = date_end - timedelta(days=1)
	return (date_start, date_end)

def execute_clean_db_query(task_name, **kwargs):
	logical_date = kwargs['data_interval_start']
	date_start, date_end = get_range_from_logical_date(logical_date)
	db_cleanup_params_mapping = {
		IngestInputs.COW_TRADES.value: {"start": date_start, "end": date_end, "column_name": "block_time"},
		IngestInputs.BINANCE_TRADES.value: {"start": int(date_start.timestamp()*1000), "end": int(date_end.timestamp()*1000), "column_name": "time"},
	}
	db_cleanup_params = db_cleanup_params_mapping[task_name]
	logger.info(f"Using these params: {db_cleanup_params}")
	logger.info(f"Removing data from table: raw.{task_name}")
	sql = f"""DELETE FROM raw.{task_name}
	WHERE {db_cleanup_params['column_name']} < '{db_cleanup_params['end']}'
	and {db_cleanup_params['column_name']} >= '{db_cleanup_params['start']}'
	"""
	conn = PostgresHook(CONN_NAME).get_conn()
	cursor = conn.cursor()
	cursor.execute(sql)
	conn.commit()
	conn.close()

def save_dataframe_to_postgres(task_name, table_name, **kwargs):
	ti = kwargs["ti"]
	df = ti.xcom_pull(task_ids=task_name)
	logger.info(df)
	engine = PostgresHook(CONN_NAME).get_sqlalchemy_engine()
	logger.warning("SAVING DATA")
	df.to_sql(f"{table_name}",engine,if_exists='append', schema = "raw", index=False)

def execute_clean_staging_table_query(**kwargs):
	logical_date = kwargs['data_interval_start']
	date_start, date_end = get_range_from_logical_date(logical_date)
	sql = f"""DELETE FROM staging.price_improvements
	WHERE cow_ts < '{int(date_end.timestamp()*1000)}'
	and cow_ts >= '{int(date_start.timestamp()*1000)}'
	"""
	conn = PostgresHook(CONN_NAME).get_conn()
	cursor = conn.cursor()
	cursor.execute(sql)
	conn.commit()
	conn.close()

def calculate_price_improvements(**kwargs):
	logical_date = kwargs['data_interval_start']
	date_start, date_end = get_range_from_logical_date(logical_date)
	substitute_mapping = {
        "binance_time_start": int(date_start.timestamp()*1000),
        "binance_time_end": int(date_end.timestamp()*1000),
        "cow_block_time_start": date_start,
        "cow_block_time_end": date_end
    }
	engine = PostgresHook(CONN_NAME).get_sqlalchemy_engine()
	file_path = list((Path.cwd() / Path("dags") / Path("sql_transformations")).glob("staging_price_improvement_trades.sql"))
	if file_path:
		conn = PostgresHook(CONN_NAME).get_conn()
		cursor = conn.cursor()
		src = Template(file_path[-1].open("r").read())
		query = src.substitute(substitute_mapping)
		logger.info(f"Query to execute: {query}")
		cursor.execute(query)
		conn.commit()
		conn.close()



with DAG(dag_id='trade_execution_quality_dag',
         start_date=days_ago(1),
         dagrun_timeout=timedelta(minutes=60),
         max_active_runs=1,
         default_args=args,	
         catchup=False,
         schedule_interval="0 9 * * * *",
         tags=['trade_execution_quality']
         ) as dag:

	with TaskGroup("ingest") as ingest:
		for item in ingest_inputs:
			start = DummyOperator(task_id = f"start_{item}")
			get_data = PythonOperator(
        		task_id = f"get_data_{item}",
        		# It was used during the test:
        		# python_callable=read_json_to_dataframe, 
        		python_callable=ingest_data,
        		trigger_rule="one_success",
        		do_xcom_push=True,
        		op_kwargs={
        			'task_name': item
        		}
        		)
			clean_data_before_insert = PythonOperator(
    			task_id=f"clean_data_db_{item}",
    			python_callable=execute_clean_db_query,
    			trigger_rule="one_success",
    			op_kwargs={
        			'task_name': item
        		}
    			)
			save_to_pg = PythonOperator(
        		task_id = f"save_pg_{item}",
        		python_callable=save_dataframe_to_postgres,
        		trigger_rule="one_success",
        		op_kwargs={
        			'task_name': f"ingest.get_data_{item}",
        			'table_name': item
        		}
        		)
			finish = DummyOperator(task_id = f"finish_{item}")
			start >> get_data >> clean_data_before_insert >> save_to_pg >> finish
	with TaskGroup("transform") as transform:
		start = DummyOperator(task_id = "start_transform")
		clean_existing_data = PythonOperator(
			task_id = "clean_staging_table",
			python_callable = execute_clean_staging_table_query,
			trigger_rule="all_success",
			op_kwargs={}
			)
		process_data = PythonOperator(
			task_id = "process_data",
			python_callable=calculate_price_improvements,
			trigger_rule="all_success",
			op_kwargs={}
    		)
		finish = DummyOperator(task_id = f"finish_transform")
		chain(ingest, start, clean_existing_data, process_data, finish)
