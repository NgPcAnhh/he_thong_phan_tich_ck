from datetime import datetime, timedelta
import math

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.models.param import Param

from function.datalake_df2csv import DfToCsvOperator


def _current_quarter() -> int:
    """Trả về quý hiện tại (1-4) dựa trên tháng UTC."""
    return math.ceil(datetime.utcnow().month / 3)

# Config chung
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

MINIO_BUCKET = "thongtin-congty-va-bctc"
MINIO_CONN_ID = "minio_finance"

@dag(
    dag_id='bctc',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule='@monthly',
    catchup=False,
    max_active_runs=1,           # Chỉ 1 DAG run cùng lúc
    params={
        "year": Param(
            default=datetime.utcnow().year,
            type="integer",
            minimum=2020,
            maximum=2030,
            description="Năm báo cáo tài chính cần lấy",
        ),
        "quarter": Param(
            default=_current_quarter(),
            type="integer",
            enum=[1, 2, 3, 4],
            description="Quý cần lấy (1-4). Mặc định = quý hiện tại",
        ),
    },
    tags=['vnstock', 'finance']
)
def stock_dag():

    @task
    def get_storage_ds():
        """Tạo partition datetime cho MinIO, không truyền vào logic function."""
        created_at = datetime.utcnow()
        return f"{created_at.strftime('%Y-%m-%d_%H:%M:%S')}:{int(created_at.microsecond / 1000):03d}"

    @task
    def get_batches(**context):
        """
        Chia danh sách mã (HOSE + HNX, bỏ UPCOM) thành batch 20 mã.
        Mỗi batch là 1 Airflow task, chạy TUẦN TỰ (max_active_tis=1).
        """
        from logic.list_macp import get_ticker_batches

        params = context["params"]
        year = params.get("year", datetime.utcnow().year)
        quarter = params.get("quarter", _current_quarter())

        print(f"[BCTC] Lấy BCTC năm {year}, quý {quarter}")

        batches = [
            {
                "symbols": batch,
                "current_year": str(year),
                "current_quarter": str(quarter),
            }
            for batch in get_ticker_batches(batch_size=20, exclude_upcom=True)
        ]

        print(f"[BCTC] Tổng số batches: {len(batches)}")
        return batches

    batches = get_batches()
    storage_ds = get_storage_ds()

    # SEQUENTIAL: chỉ 1 batch chạy tại 1 thời điểm
    # Trong mỗi batch: 20 mã × 3 calls × 3.2s ≈ 3 phút
    # Rate: ~19 req/phút (dưới giới hạn 20)
    ingest_bctc = DfToCsvOperator.partial(
        task_id="ingest_bctc",
        logic_file="bctc",
        df_name="get_financial_reports",
        bucket_name=MINIO_BUCKET,
        object_path="bctc/{{ ti.xcom_pull(task_ids='get_storage_ds') }}/batch_{{ ti.map_index }}.csv",
        conn_id=MINIO_CONN_ID,
        max_active_tis_per_dagrun=1,            # CHỈ 1 BATCH TẠI 1 THỜI ĐIỂM
        execution_timeout=timedelta(minutes=15), # 20 mã × 3 × 3.2s ≈ 3 phút (dư nhiều)
    ).expand(op_kwargs=batches)

    chain(batches, ingest_bctc)
    chain(storage_ds, ingest_bctc)

stock_dag()