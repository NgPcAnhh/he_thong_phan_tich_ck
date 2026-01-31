# PowerShell script để clear failed tasks và chạy lại ngay

$DAG_ID = "backup_db"

Write-Host "🔄 Clearing failed tasks for DAG: $DAG_ID" -ForegroundColor Cyan

# Get the Airflow scheduler container name
$CONTAINER = docker ps --filter "name=airflow-scheduler" --format "{{.Names}}" | Select-Object -First 1

if ([string]::IsNullOrEmpty($CONTAINER)) {
    Write-Host "❌ Airflow scheduler container not found!" -ForegroundColor Red
    exit 1
}

Write-Host "📦 Using container: $CONTAINER" -ForegroundColor Green

# Clear all failed tasks in the latest DAG run
docker exec -it $CONTAINER airflow tasks clear $DAG_ID --only-failed --yes

Write-Host "✅ All failed tasks have been cleared and will run immediately!" -ForegroundColor Green
