FROM apache/airflow:2.9.3

USER root
# Cài đặt các thư viện hệ thống nếu cần (tùy chọn)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /tmp/requirements.txt

# MẸO CỐT LÕI: Cài torch bản CPU trước để tránh tải 3GB CUDA dư thừa
RUN pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu

# Sau đó mới cài các thư viện khác trong requirements
RUN pip install --no-cache-dir --default-timeout=120 -r /tmp/requirements.txt

# Pre-download PhoBERT model
# Thêm biến môi trường để đảm bảo model được lưu đúng chỗ
ENV TRANSFORMERS_CACHE=/opt/airflow/models
RUN python -c "from transformers import pipeline; \
    pipeline('sentiment-analysis', \
    model='wonrax/phobert-base-vietnamese-sentiment', \
    tokenizer='wonrax/phobert-base-vietnamese-sentiment')"