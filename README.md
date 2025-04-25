<<<<<<< HEAD
# Task
## Overview
Measure trade execution quality (aka price improvement) of CoW Swap trades compared to an external price source (baseline) and make the results available on a daily basis.
Price improvement is defined as the difference in exchange rate between the reference source and the one the trade achieved on CoW Swap.
## Collect Historical Data:
- Gather historical data for completed trades on CoW Protocol for the WETH/USDC token pair for the latest available day.
- Use Dune Analytics for data collection and build a query leveraging the data stored inside the table cow_protocolethereum.trades.
## Simulate Baseline Trades:
- Use an open-source API to establish the baseline executions with the same token pairs and input amounts as the actual trades. Try to find the best matches, considering the time of each trade.
- Recommended APIs: Coingecko, CryptoCompare, Binance
## Calculate Price Improvement:
- Compare the realized prices of actual trades with the baseline prices from the external API.
- Create a joined dataset using both inputs.
- Add a new column to compute the price improvement between the datasets.
- Determine the average price improvement.
## Automate the Process:
- Develop a data pipeline that performs steps 1-3 daily at 9 AM UTC (e.g. Airflow, cron job, etc.) and stores the results in a Postgres database.
## Bonus
- Ensure the code is idempotent to handle potential backfills, ideally using a tool like Airflow for scheduling and orchestration.

# Overview
Here you can find the solution of the task

# How to run
## Steps to run
- Unarchive zip in some directory
- Prepare docker-compose.yaml ENV variables: **DUNE_ANALYTICS_API_KEY** environment variable should have your key to query Dune analytics
- Execute command: docker compose up
- Open UI: http://localhost:8080/hom
- Enter default login pass: airflow/airflow

## Airflow connections
You have to create a new connection with params:
Connection Id = dwh
Connection Type = Postgres	
Host = dwh
Database = dwh
Login = dwh
Password = dwh	
Port = 5432

## Available DAGs
Open http://localhost:8080/home to find a Airflow UI, then you see 2 DAGs:
- db_migrations_dag
- trade_execution_quality_dag

### db_migrations_dag
This DAG is for making initial migrations into DWH database

### trade_execution_quality_dag
This is the main logic DAG


# Future steps
- Create a separate repo for DDLs and Implement CI/CD with Alembic
- Implement more connectors with different sources to get more precise value with ASOF join
- More optimal data types for querying
- Use Timescale, InfluxDB for working with timeseries
- Personally I like Dagster more than Airflow
- Maybe something else...

# Why these technologies
- As far as I understand you're using Airflow, since it has been mentioned 2 times, that's why I've decided to use Airflow for orchestration
- Pandas for data processing since it's industry standard + Airflow operators use Pandas dataframe as a data container
- PG as a database, because it was mentioned in the task
- Docker for easier development - one command deploy
- Python - because Airflow written in Python

# Screenshots
You may find screenshots into screenshots folder if you don't want to run the whole code

P.S. I can provide my token for Dune analytics if needed
P.S2 I am ready to talk in case of any questions