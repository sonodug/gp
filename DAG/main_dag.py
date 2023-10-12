from airflow import DAG
from datetime import datetime, timedelta, date
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

DB_CONN = "gp_std3_47"
DB_SCHEMA = 'std3_47'

DB_PROC_FULL_LOAD = 'f_load_full'
DB_PROC_PART_LOAD = 'f_load_simple_partition'
DB_UPSERT_LOAD = 'f_load_simple_upsert'
DB_PROC_LOAD_MART = 'f_calculate_plan_mart'

FULL_LOAD_TABLES = ['region', 'chanel', 'price', 'product']
PART_LOAD_TABLES = ['sales']
UPS_LOAD_TABLES = ['plan']

FULL_LOAD_FILES = {
    'region': 'region',
    'chanel': 'chanel',
    'price': 'price',
    'product': 'product',
}

MD_TABLE_FULL_LOAD_QUERY = (
    f"select {DB_SCHEMA}.{DB_PROC_FULL_LOAD}(%(tab_name)s, %(file_name)s);"
)
MD_TABLE_PART_LOAD_QUERY = f"select {DB_SCHEMA}.{DB_PROC_PART_LOAD}(%(tab_name)s, %(date_attr)s, %(start_date)s, %(end_date)s, %(source_tab)s, %(user)s, %(pass)s);"
MD_TABLE_UPSERT_LOAD_QUERY = f"select {DB_SCHEMA}.{DB_UPSERT_LOAD}(%(source_tab_name)s, %(tab_name)s, %(date_attr)s, %(user)s, %(pass)s);"
MD_LOAD_MART_JAN = f"select {DB_SCHEMA}.{DB_PROC_LOAD_MART}('202101');"

default_args = {
    'depends_on_past': False,
    'owner': 'std3_47',
    'start_date': datetime(2023, 10, 11),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    "std3_47_main_dag",
    max_active_runs=3,
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:
    task_start = DummyOperator(task_id="start")

    with TaskGroup("full_load") as task_full_load:
        for table in FULL_LOAD_TABLES:
            task = PostgresOperator(
                task_id=f"load_table_ref_{table}",
                postgres_conn_id=DB_CONN,
                sql=MD_TABLE_FULL_LOAD_QUERY,
                parameters={
                    'tab_name': f'{DB_SCHEMA}.{table}',
                    'file_name': f'{FULL_LOAD_FILES[table]}',
                },
            )

    with TaskGroup("delta_part_load") as task_part_load:
        for table in PART_LOAD_TABLES:
            task = PostgresOperator(
                task_id=f"load_table_fact_{table}",
                postgres_conn_id=DB_CONN,
                sql=MD_TABLE_PART_LOAD_QUERY,
                parameters={
                    'tab_name': f'{DB_SCHEMA}.{table}',
                    'date_attr': 'date',
                    'start_date': '2021-01-02',
                    'end_date': '2021-07-27',
                    'source_tab': 'gp.sales',
                    'user': 'intern',
                    'pass': 'intern',
                },
            )

    with TaskGroup("delta_ups_load") as task_ups_load:
        for table in UPS_LOAD_TABLES:
            task = PostgresOperator(
                task_id=f"load_table_fact_{table}",
                postgres_conn_id=DB_CONN,
                sql=MD_TABLE_UPSERT_LOAD_QUERY,
                parameters={
                    'source_tab_name': 'gp.plan',
                    'tab_name': f'{DB_SCHEMA}.{table}',
                    'date_attr': 'date',
                    'user': 'intern',
                    'pass': 'intern',
                },
            )

    task_load_mart = PostgresOperator(
        task_id='start_calculate_mart', postgres_conn_id=DB_CONN, sql=MD_LOAD_MART_JAN
    )
    task_end = DummyOperator(task_id="end")

    task_start >> task_full_load >> task_part_load >> task_ups_load >> task_load_mart >> task_end