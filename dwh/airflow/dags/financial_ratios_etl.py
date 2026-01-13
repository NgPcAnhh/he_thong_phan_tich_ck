from datetime import datetime, timedelta

from airflow.decorators import dag, task
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
    dag_id="financial_ratios_etl",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 2 * * 0",  # 02:00 every Sunday (weekly update)
    catchup=False,
    tags=["vnstock", "finance", "ratios", "financial_metrics"],
    description="Lấy dữ liệu chỉ số tài chính (P/E, EPS, ROA, ROE, ...) cho tất cả mã cổ phiếu",
    params={
        "period": "quarter",  # 'quarter' hoặc 'year'
    },
)
def financial_ratios_etl_dag():
    """
    ETL Pipeline cho chỉ số tài chính.
    
    - Chạy hàng tuần vào Chủ Nhật lúc 02:00
    - Lấy dữ liệu P/E, EPS, ROA, ROE, ... từ vnstock
    - Lưu vào MinIO bucket: thongtin-congty-va-bctc
    - Partition theo ngày: financial_ratios/YYYY-MM-DD/batch_X.csv
    """
    
    @task
    def get_batches(**context):
        """
        Tạo danh sách batch tickers để lấy financial ratios.
        """
        from logic.list_macp import get_ticker_batches

        # Lấy kỳ báo cáo từ params
        period = context["params"].get("period", "year")
        
        print(f"[FINANCIAL_RATIOS] Lấy dữ liệu kỳ: {period}")

        # Batch nhỏ để tránh rate limit (ratios API chậm hơn price)
        batches = [
            {
                "symbols": batch,
                "period": period,
            }
            for batch in get_ticker_batches(batch_size=2)  # Batch size nhỏ hơn vì API chậm (sleep 2s/mã)
        ]
        
        print(f"[FINANCIAL_RATIOS] Tổng số batches: {len(batches)}")
        return batches

    batches = get_batches()

    # Sử dụng DfToCsvOperator
    ingest_ratios = DfToCsvOperator.partial(
        task_id="ingest_financial_ratios",
        logic_file="financial_ratios",
        df_name="get_financial_ratios_batch",
        bucket_name=MINIO_BUCKET,
        object_path="financial_ratios/{{ ds }}/batch_{{ ti.map_index }}.csv",
        conn_id=MINIO_CONN_ID,
    ).expand(op_kwargs=batches)

    chain(batches, ingest_ratios)


financial_ratios_etl_dag()
