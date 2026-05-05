from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.models import Variable

from function.datalake_df2csv import DfToCsvOperator

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

MINIO_BUCKET = Variable.get(
    "minio_bucket",
    default_var="thongtin-congty-va-bctc",
)

MINIO_CONN_ID = "minio_finance"


@dag(
    dag_id="vn_macro_per_year",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval="0 9 * * 6",
    catchup=False,
    tags=["macro_economy", "vietnam", "worldbank", "gdp", "cpi"],
)
def vn_macro_per_year_dag():
    """
    ETL pipeline: Lấy 16 chỉ số kinh tế vĩ mô Việt Nam từ World Bank API.

    Nhóm chỉ số:
    - Kinh tế vĩ mô: GDP, CPI, Công nghiệp, Chế biến chế tạo, Tiêu dùng
    - Lãi suất & Tiền tệ: Tỷ giá USD/VND, Lãi suất gửi/cho vay
    - Thương mại & Đầu tư: Xuất nhập khẩu, Cán cân TM, FDI
    - Tài chính: Dự trữ ngoại hối, M2, Nợ xấu

    Dữ liệu lưu dạng CSV vào MinIO với partition theo ngày.
    """

    ingest_vn_macro = DfToCsvOperator(
        task_id="ingest_vn_macro_yearly",
        logic_file="vn_macro_yearly",
        df_name="get_vn_macro_data",
        bucket_name=MINIO_BUCKET,
        object_path="vn_macro_yearly/{{ ds }}/data.csv",
        conn_id=MINIO_CONN_ID,
    )

    ingest_vn_macro


vn_macro_per_year_dag()
