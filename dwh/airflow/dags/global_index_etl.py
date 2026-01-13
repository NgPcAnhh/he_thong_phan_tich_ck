from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.models.baseoperator import chain

from function.datalake_df2csv import DfToCsvOperator

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

MINIO_BUCKET = "thongtin-congty-va-bctc"
MINIO_CONN_ID = "minio_finance"


@dag(
    dag_id="global_index_etl",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["macro_economy", "global_index", "forex", "bonds", "yfinance"],
)
def global_index_etl_dag():
    
    # Task 1: USD/VND Exchange Rate
    ingest_usd_vnd = DfToCsvOperator(
        task_id="ingest_usd_vnd",
        logic_file="usd_vnd_data",
        df_name="get_usd_vnd_rate",
        bucket_name=MINIO_BUCKET,
        object_path="global_index/usd_vnd/{{ ds }}/data.csv",
        conn_id=MINIO_CONN_ID,
        op_kwargs={
            "start_date": "{{ ds }}",
            "end_date": "{{ macros.ds_add(ds, 1) }}",  # Ngày mai để lấy được dữ liệu ngày hiện tại
        },
    )
    
    # Task 2: DXY Dollar Index
    ingest_dxy = DfToCsvOperator(
        task_id="ingest_dxy_index",
        logic_file="dxy_index_data",
        df_name="get_dxy_index",
        bucket_name=MINIO_BUCKET,
        object_path="global_index/dxy_index/{{ ds }}/data.csv",
        conn_id=MINIO_CONN_ID,
        op_kwargs={
            "start_date": "{{ ds }}",
            "end_date": "{{ macros.ds_add(ds, 1) }}",  # Ngày mai để lấy được dữ liệu ngày hiện tại
        },
    )
    
    # Task 3: USD/CNY Exchange Rate
    ingest_usd_cny = DfToCsvOperator(
        task_id="ingest_usd_cny",
        logic_file="usd_cny_data",
        df_name="get_usd_cny_rate",
        bucket_name=MINIO_BUCKET,
        object_path="global_index/usd_cny/{{ ds }}/data.csv",
        conn_id=MINIO_CONN_ID,
        op_kwargs={
            "start_date": "{{ ds }}",
            "end_date": "{{ macros.ds_add(ds, 1) }}",  # Ngày mai để lấy được dữ liệu ngày hiện tại
        },
    )
    
    # Task 4: EUR/USD Exchange Rate
    ingest_eur_usd = DfToCsvOperator(
        task_id="ingest_eur_usd",
        logic_file="eur_usd_data",
        df_name="get_eur_usd_rate",
        bucket_name=MINIO_BUCKET,
        object_path="global_index/eur_usd/{{ ds }}/data.csv",
        conn_id=MINIO_CONN_ID,
        op_kwargs={
            "start_date": "{{ ds }}",
            "end_date": "{{ macros.ds_add(ds, 1) }}",  # Ngày mai để lấy được dữ liệu ngày hiện tại
        },
    )
    
    # Task 5: US 10-Year Treasury Bond Yield
    ingest_us_bond = DfToCsvOperator(
        task_id="ingest_us_bond_10y",
        logic_file="us_bond_10y_data",
        df_name="get_us_bond_10y",
        bucket_name=MINIO_BUCKET,
        object_path="global_index/us_bond_10y/{{ ds }}/data.csv",
        conn_id=MINIO_CONN_ID,
        op_kwargs={
            "start_date": "{{ ds }}",
            "end_date": "{{ macros.ds_add(ds, 1) }}",  # Ngày mai để lấy được dữ liệu ngày hiện tại
        },
    )
    
    # All tasks run in parallel (no dependencies)
    [ingest_usd_vnd, ingest_dxy, ingest_usd_cny, ingest_eur_usd, ingest_us_bond]


global_index_etl_dag()
