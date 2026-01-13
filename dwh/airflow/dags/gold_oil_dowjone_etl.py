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
    dag_id="gold_oil_dowjone_etl",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["macro_economy", "gold", "oil", "dowjones", "commodities", "yfinance"],
)
def gold_oil_dowjone_etl_dag():
    """
    ETL pipeline for commodity prices and stock indices.
    
    Commodities & Indices:
    - Gold Futures (GC=F)
    - WTI Crude Oil (CL=F)
    - Dow Jones Industrial Average (^DJI)
    
    Data is stored in MinIO with partitioning by date.
    All tasks run in parallel.
    """
    
    # Task 1: Gold Price
    ingest_gold = DfToCsvOperator(
        task_id="ingest_gold_price",
        logic_file="gold_data",
        df_name="get_gold_price",
        bucket_name=MINIO_BUCKET,
        object_path="xau/{{ ds }}/data.csv",
        conn_id=MINIO_CONN_ID,
        op_kwargs={
            "start_date": "{{ ds }}",
            "end_date": "{{ macros.ds_add(ds, 1) }}",  # Ngày mai để lấy được dữ liệu ngày hiện tại
        },
    )
    
    # Task 2: Oil Price
    ingest_oil = DfToCsvOperator(
        task_id="ingest_oil_price",
        logic_file="oil_data",
        df_name="get_oil_price",
        bucket_name=MINIO_BUCKET,
        object_path="oil/{{ ds }}/data.csv",
        conn_id=MINIO_CONN_ID,
        op_kwargs={
            "start_date": "{{ ds }}",
            "end_date": "{{ macros.ds_add(ds, 1) }}",  # Ngày mai để lấy được dữ liệu ngày hiện tại
        },
    )
    
    # Task 3: Dow Jones Index
    ingest_dowjones = DfToCsvOperator(
        task_id="ingest_dowjones_index",
        logic_file="dowjones_data",
        df_name="get_dowjones_index",
        bucket_name=MINIO_BUCKET,
        object_path="dowjone/{{ ds }}/data.csv",
        conn_id=MINIO_CONN_ID,
        op_kwargs={
            "start_date": "{{ ds }}",
            "end_date": "{{ macros.ds_add(ds, 1) }}",  # Ngày mai để lấy được dữ liệu ngày hiện tại
        },
    )
    
    # All tasks run in parallel (no dependencies)
    [ingest_gold, ingest_oil, ingest_dowjones]


gold_oil_dowjone_etl_dag()
