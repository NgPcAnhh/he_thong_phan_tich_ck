FROM apache/airflow:2.9.3

# Cài gói ở layer build, chấp nhận chạy pip bằng root nhưng bỏ cảnh báo
USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN PIP_DEFAULT_TIMEOUT=120 pip install --no-cache-dir --default-timeout=120 -r /tmp/requirements.txt
