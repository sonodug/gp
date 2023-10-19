from airflow import DAG
from datetime import datetime, timedelta, date
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# Greenplum db params
DB_CONN = "gp_std3_47"
DB_SCHEMA = 'std3_47'

# Clickhouse db params
CH_HOST_MAIN = "192.168.214.209"
CH_COPY_HOSTS = ["192.168.214.210", "192.168.214.211"]
CH_PORT = "9000"
CH_LOGIN = "std3_47"
CH_PASS = "5ZnVsbq48zvjW7bQ"
CH_DB = "std3_47"
CH_SCHEMA = "std3_47"
CH_CLUSTER = "default_cluster"

# Greenplum dag workflow
PROC_FULL_LOAD = 'f_p_load_full'
PROC_PART_LOAD = 'f_p_load_delta_partition'
# PROC_UPSERT_LOAD = 'f_load_simple_upsert'

DICT_LOAD_TABLES = ['promos', 'promo_types', 'stores']

DICT_LOAD_FILES = {
    'promos': 'promos',
    'promo_types': 'promo_types',
    'stores': 'stores',
}

FULL_LOAD_QUERY = (f"select {DB_SCHEMA}.{PROC_FULL_LOAD}(%(tab_name)s, %(file_name)s);")

START_DATE = "2021-01-01"
END_DATE = "2021-02-28"
PART_KEY = {"traffic": "date", "bills": "calday", "coupons": "date"}
PXF_TOOL = "pxf"
GPFDIST_TOOL = "gpfdist"

PXF_SCHEMA = "gp"
PXF_USER = "intern"
PXF_PASS = "intern"

TRAFFIC_LOAD_TABLE = "traffic"
TRAFFIC_EXT_TABLE = "traffic"
TRAFFIC_LOAD_QUERY = f"select {DB_SCHEMA}.{PROC_PART_LOAD}(%(tab_name)s, %(date_attr)s, %(start_date)s, %(end_date)s, p_conversion := TRUE, p_ext_tool := %(ext_tool)s, p_ext_table := %(ext_tab)s, p_pxf_user := %(user)s, p_pxf_pass := %(pass)s);"

COUPONS_LOAD_TABLE = "coupons"
COUPONS_FILENAME = "coupons"
COUPONS_LOAD_QUERY = f"select {DB_SCHEMA}.{PROC_PART_LOAD}(%(tab_name)s, %(date_attr)s, %(start_date)s, %(end_date)s, p_conversion := FALSE, p_ext_tool := %(ext_tool)s, p_gpf_filename := %(filename)s);"

# PARTITION WITH MERGE CASE
BILLS_PROC_PART_LOAD = "f_p_load_delta_partition_merge"
BILLS_LOAD_TABLE = "bills"
BILLS_EXT_TABLE_HEAD = "bills_head"
BILLS_EXT_TABLE_ITEM = "bills_item"
BILLS_LOAD_QUERY = f"select {DB_SCHEMA}.{BILLS_PROC_PART_LOAD}(%(tab_name)s, %(date_attr)s, %(start_date)s, %(end_date)s, %(source_tab1)s, %(source_tab2)s, %(user)s, %(pass)s);"

# ALTERNATIVE CASE
# BILLS_HEAD_LOAD_TABLE = "bills_head"
# BILLS_HEAD_EXT_TABLE = "bills_head"
# BILLS_HEAD_LOAD_QUERY = f"select {DB_SCHEMA}.{PROC_PART_LOAD}(%(tab_name)s, %(date_attr)s, %(start_date)s, %(end_date)s, p_conversion := FALSE, p_ext_tool := %(ext_tool)s, p_ext_table := %(ext_tab)s, %(user)s, %(pass)s);"

# BILLS_ITEM_LOAD_TABLE = "bills_item"
# BILLS_ITEM_EXT_TABLE = "bills_item"
# BILLS_ITEM_MERGE_KEY = "'billnum'"
# BILLS_ITEM_LOAD_QUERY = f"select {DB_SCHEMA}.{PROC_UPSERT_LOAD}(%(source_tab_name)s, %(tab_name)s, %(merge_key)s, %(user)s, %(pass)s);"

MART_START_DATE_MONTH = "20210101"
MART_END_DATE_MONTH = "20210228"
MART_LOAD_INTERVAL_MONTH = 2
MART_SELECTION_MODES_MONTH = ['month_interval', 'monthly']

MART_START_DATE_DAY = "20210101"
MART_END_DATE_DAY = "20210105"
MART_LOAD_INTERVAL_DAY = 4
MART_SELECTION_MODES_DAY = ['day_interval', 'daily']

MART_PROC_LOAD = "f_p_build_report_mart"
MART_LOAD_QUERY = f"select {DB_SCHEMA}.{MART_PROC_LOAD}(%(start_date)s, %(end_date)s, %(load_interval)s, ARRAY[%(smode1)s::selection_mode, %(smode2)s::selection_mode]);"

# Clickhouse dag workflow
CREATE_MART_EXT = ""
CREATE_MART = ""
CREATE_COPY_REPORT = ""
CREATE_COPY_REPORT_DISTR = ""

# ext -> copy -> distr -> insert in distr
CH_TABS = []
CH_QUERIES = []


def create_ext_query(from_table, table):
    return f"""CREATE TABLE {table}_ext
                     (
                        `plant` String,
                        `txt` String,
                        `turnover` Int32,
                        `coupon_discount` Decimal(17, 2),
                        `turnover_with_disc` Decimal(17, 2),
                        `material_qty` Int32,
                        `bills_qty` Int32,
                        `traffic` Int32,
                        `matdisc_qty` Int32,
                        `matdisc_percent` Decimal(7, 1),
                        `avg_mat_qty` Decimal(7, 2),
                        `conversion_rate` Decimal(7, 2),
                        `avg_bill` Decimal(7, 1),
                        `avg_profit` Decimal(7, 1)
                    )
                    ENGINE = PostgreSQL('192.168.214.203:5432','adb',{from_table}, {CH_DB}, {password}, {CH_DB})"""


def create_copy_query(table):
    return f"""CREATE TABLE {CH_DB}.{table} ON CLUSTER default_cluster
                        (
                            `plant` String,
                            `txt` String,
                            `turnover` Int32,
                            `coupon_discount` Decimal(17, 2),
                            `turnover_with_disc` Decimal(17, 2),
                            `material_qty` Int32,
                            `bills_qty` Int32,
                            `traffic` Int32,
                            `matdisc_qty` Int32,
                            `matdisc_percent` Decimal(7, 1),
                            `avg_mat_qty` Decimal(7, 2),
                            `conversion_rate` Decimal(7, 2),
                            `avg_bill` Decimal(7, 1),
                            `avg_profit` Decimal(7, 1)
                        )
                        ENGINE = ReplicatedMergeTree('/{CH_DB}/{table}/{{shard}}', '{{replica}}') 
                        ORDER BY store_id"""


def create_distr_query(table):
    return f"""CREATE TABLE {CH_DB}.{table}_distr
                        (
                            `plant` String,
                            `txt` String,
                            `turnover` Int32,
                            `coupon_discount` Decimal(17, 2),
                            `turnover_with_disc` Decimal(17, 2),
                            `material_qty` Int32,
                            `bills_qty` Int32,
                            `traffic` Int32,
                            `matdisc_qty` Int32,
                            `matdisc_percent` Decimal(7, 1),
                            `avg_mat_qty` Decimal(7, 2),
                            `conversion_rate` Decimal(7, 2),
                            `avg_bill` Decimal(7, 1),
                            `avg_profit` Decimal(7, 1)
                        )
                        ENGINE = Distributed('default_cluster', {CH_DB}, '{table}', store_id)"""


def create_query(from_table, to_table):
    CH_QUERIES.append(create_ext_query(from_table, to_table))
    CH_QUERIES.append(create_copy_query(to_table))
    CH_QUERIES.append(create_distr_query(to_table))


def generate_table_names(start_date, end_date, load_interval, prefix, is_monthly):
    current_date = datetime.strptime(start_date, "%Y%m%d")
    if is_monthly:
        end_date = datetime.strptime(end_date, "%Y%m%d") + relativedelta(months=1)
        table_name = f"{prefix}_{current_date.strftime('%Y%m')}_{end_date.strftime('%Y%m')}"
    else:
        end_date = datetime.strptime(end_date, "%Y%m%d")
        table_name = f"{prefix}_{current_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"

    CH_TABS.append(table_name)

    for i in range(load_interval):
        if is_monthly:
            table_name = f"{prefix}_{current_date.strftime('%Y%m')}"
            current_date += relativedelta(months=1)
        else:
            table_name = f"{prefix}_{current_date.strftime('%Y%m%d')}"
            current_date += relativedelta(days=1)

        CH_TABS.append(table_name)


client = Client(host=CH_HOST, port=CH_PORT, database=CH_DB, user=CH_LOGIN, password=CH_PASS)


def load_tables():
    for i in range(len(CH_TABS)):
        client.execute(f"TRUNCATE {CH_SCHEMA}.{CH_TABS[i]} ON CLUSTER {CH_CLUSTER}")
        client.execute(f"INSERT INTO {CH_SCHEMA}.{CH_TABS[i]} SELECT * FROM {CH_SCHEMA}.{CH_TABS[i]}_ext")


default_args = {
    'depends_on_past': False,
    'owner': 'std3_47',
    'start_date': datetime(2023, 10, 18),
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
}

# So far without clickhouse
with DAG(
        "std3_47_report_dag",
        max_active_runs=3,
        schedule_interval=None,
        default_args=default_args,
        catchup=False,
) as dag:
    task_start = DummyOperator(task_id="dag_start")

    with TaskGroup("dict_load") as task1:
        for table in DICT_LOAD_TABLES:
            task = PostgresOperator(
                task_id=f"load_dict_tables_{table}",
                postgres_conn_id=DB_CONN,
                sql=FULL_LOAD_QUERY,
                parameters={
                    'tab_name': f'{DB_SCHEMA}.{table}',
                    'file_name': f'{DICT_LOAD_FILES[table]}',
                },
            )

    task2 = PostgresOperator(task_id=f"load_table_{TRAFFIC_LOAD_TABLE}",
                             postgres_conn_id=DB_CONN,
                             sql=TRAFFIC_LOAD_QUERY,
                             parameters={"tab_name": f"{DB_SCHEMA}.{TRAFFIC_LOAD_TABLE}",
                                         "date_attr": f"{PART_KEY['traffic']}", "start_date": f"{START_DATE}",
                                         "end_date": f"{END_DATE}", "ext_tool": f"{PXF_TOOL}",
                                         "ext_tab": f"{PXF_SCHEMA}.{TRAFFIC_EXT_TABLE}", "user": f"{PXF_USER}",
                                         "pass": f"{PXF_PASS}"}
                             )

    task3 = PostgresOperator(task_id=f"load_table_{COUPONS_LOAD_TABLE}",
                             postgres_conn_id=DB_CONN,
                             sql=COUPONS_LOAD_QUERY,
                             parameters={"tab_name": f"{DB_SCHEMA}.{COUPONS_LOAD_TABLE}",
                                         "date_attr": f"{PART_KEY['coupons']}", "start_date": f"{START_DATE}",
                                         "end_date": f"{END_DATE}", "ext_tool": f"{GPFDIST_TOOL}",
                                         "filename": f"{COUPONS_FILENAME}"}
                             )

    task4 = PostgresOperator(task_id=f"load_table_{BILLS_LOAD_TABLE}",
                             postgres_conn_id=DB_CONN,
                             sql=BILLS_LOAD_QUERY,
                             parameters={"tab_name": f"{DB_SCHEMA}.{BILLS_LOAD_TABLE}",
                                         "date_attr": f"{PART_KEY['bills']}", "start_date": f"{START_DATE}",
                                         "end_date": f"{END_DATE}",
                                         "source_tab1": f"{PXF_SCHEMA}.{BILLS_EXT_TABLE_ITEM}",
                                         "source_tab2": f"{PXF_SCHEMA}.{BILLS_EXT_TABLE_HEAD}", "user": f"{PXF_USER}",
                                         "pass": f"{PXF_PASS}"}
                             )

    task_mart_month = PostgresOperator(task_id=f"report_{MART_START_DATE_MONTH}_{MART_END_DATE_MONTH}",
                                       postgres_conn_id=DB_CONN,
                                       sql=MART_LOAD_QUERY,
                                       parameters={"start_date": f"{MART_START_DATE_MONTH}",
                                                   "end_date": f"{MART_END_DATE_MONTH}",
                                                   "load_interval": f"{MART_LOAD_INTERVAL_MONTH}",
                                                   "smode1": f"{MART_SELECTION_MODES_MONTH[0]}",
                                                   "smode2": f"{MART_SELECTION_MODES_MONTH[1]}"}
                                       )

    task_mart_day = PostgresOperator(task_id=f"report_{MART_START_DATE_DAY}_{MART_END_DATE_DAY}",
                                     postgres_conn_id=DB_CONN,
                                     sql=MART_LOAD_QUERY,
                                     parameters={"start_date": f"{MART_START_DATE_DAY}",
                                                 "end_date": f"{MART_END_DATE_DAY}",
                                                 "load_interval": f"{MART_LOAD_INTERVAL_DAY}",
                                                 "smode1": f"{MART_SELECTION_MODES_DAY[0]}",
                                                 "smode2": f"{MART_SELECTION_MODES_DAY[1]}"}
                                     )

    # task_ch via taskgroup and python operator (cycle) есть CH_TABS = [], пробежать по ним и перекинуть исходные таблицы = итоговые

    task_end = DummyOperator(task_id="dag_end")

    task_start >> task1 >> task2 >> task3 >> task4 >> task_mart_month >> task_mart_day >> task_end
