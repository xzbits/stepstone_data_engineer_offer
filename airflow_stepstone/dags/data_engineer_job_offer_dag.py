from airflow import DAG
from datetime import datetime
from airflow_stepstone.plugins.helpers import process_wrangling_data
from airflow_stepstone.plugins.helpers import process_create_tables
from airflow_stepstone.plugins.helpers import process_etl
from airflow_stepstone.plugins.helpers.sql_queries import *
from airflow_stepstone.plugins.operators import (WranglingDataOperator, ETLOperator, CreateTablesOperator)


default_args = {'owner': 'stepstone',
                'start_date': datetime(2022, 12, 5),
                'depends_on_past': False,
                'retries': 1}

dag = DAG('stepstone_dag',
          default_args=default_args,
          description='Wrangling data and ETL Data Engineer job offers',
          schedule="@daily")

# Postgres connection id
postgres_conn_id = "postgres_stepstone"

wragling_data = WranglingDataOperator(task_id='Wragling_data', func=process_wrangling_data)

create_tables = CreateTablesOperator(task_id='Create_tables',
                                     func=process_create_tables,
                                     drop_queries=drop_table_queries,
                                     create_queries=create_table_queries)

ETL_operation = ETLOperator(task_id='ETL_operation', func=process_etl)

wragling_data >> create_tables
create_tables >> ETL_operation
