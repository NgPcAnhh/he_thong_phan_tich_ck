#!/bin/bash
# Script để clear failed tasks và chạy lại ngay

DAG_ID="backup_db"

echo "🔄 Clearing failed tasks for DAG: $DAG_ID"

# Get the Airflow scheduler container name
CONTAINER=$(docker ps --filter "name=airflow-scheduler" --format "{{.Names}}" | head -n 1)

if [ -z "$CONTAINER" ]; then
    echo "❌ Airflow scheduler container not found!"
    exit 1
fi

echo "📦 Using container: $CONTAINER"

# Clear all failed tasks in the latest DAG run
docker exec -it "$CONTAINER" airflow tasks clear "$DAG_ID" \
    --only-failed \
    --yes

echo "✅ All failed tasks have been cleared and will run immediately!"
