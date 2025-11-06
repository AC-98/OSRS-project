# OSRS Signals - Run Full Pipeline
# PowerShell script to run the complete data pipeline

Write-Host "OSRS Signals - Running Full Pipeline" -ForegroundColor Green
Write-Host "=====================================" -ForegroundColor Green

# Ensure we're in the project root
if (-not (Test-Path "flows/ingest_osrs.py")) {
    Write-Host "[ERROR] Please run this script from the project root directory" -ForegroundColor Red
    exit 1
}

# Check if virtual environment exists
if (-not (Test-Path "venv\Scripts\python.exe")) {
    Write-Host "[ERROR] Virtual environment not found. Run setup.ps1 first." -ForegroundColor Red
    exit 1
}

# Set virtual environment Python path
$venvPython = ".\venv\Scripts\python.exe"
Write-Host "[INFO] Using virtual environment Python: $venvPython" -ForegroundColor Gray

# Step 1: Run data ingestion
Write-Host "`nStep 1: Running data ingestion..." -ForegroundColor Yellow
& $venvPython flows/ingest_osrs.py
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Data ingestion failed" -ForegroundColor Red
    exit 1
}
Write-Host "[OK] Data ingestion completed" -ForegroundColor Green

# Step 2: Run dbt transformations
Write-Host "`nStep 2: Running dbt transformations..." -ForegroundColor Yellow
Set-Location dbt

# Test dbt connection
Write-Host "Testing dbt connection..." -ForegroundColor Gray
& ..\venv\Scripts\dbt.exe debug --profiles-dir .
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] dbt debug failed" -ForegroundColor Red
    Set-Location ..
    exit 1
}

# Run dbt build
Write-Host "Building dbt models..." -ForegroundColor Gray
& ..\venv\Scripts\dbt.exe build --profiles-dir .
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] dbt build failed" -ForegroundColor Red
    Set-Location ..
    exit 1
}

Set-Location ..
Write-Host "[OK] dbt transformations completed" -ForegroundColor Green

# Step 3: Run item selection
Write-Host "`nStep 3: Running item selection..." -ForegroundColor Yellow
& $venvPython scripts/select_items.py --mode hybrid --force-include
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Item selection failed" -ForegroundColor Red
    exit 1
}
Write-Host "[OK] Item selection completed" -ForegroundColor Green

# Step 4: Run backtests
Write-Host "`nStep 4: Running ML backtests..." -ForegroundColor Yellow
& $venvPython scripts/run_backtest.py --items "@config/items_selected.json" --window 10 --methods last_value --max-splits 5
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Backtesting failed" -ForegroundColor Red
    exit 1
}
Write-Host "[OK] Backtesting completed" -ForegroundColor Green

# Step 5: Test API
Write-Host "`nStep 5: Testing API endpoints..." -ForegroundColor Yellow
$api_test_script = @"
import sys
sys.path.append('api')
try:
    from fastapi.testclient import TestClient
    from main import app
    
    client = TestClient(app)
    response = client.get('/health')
    if response.status_code == 200:
        print('[OK] API health check passed')
    else:
        print(f'[ERROR] API health check failed: {response.status_code}')
        sys.exit(1)
except Exception as e:
    print(f'[ERROR] API test failed: {e}')
    sys.exit(1)
"@

$api_test_script | & $venvPython
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] API testing failed" -ForegroundColor Red
    exit 1
}
Write-Host "[OK] API testing completed" -ForegroundColor Green

# Summary
Write-Host "`n✅ Pipeline Complete!" -ForegroundColor Green
Write-Host "====================" -ForegroundColor Green
Write-Host ""
Write-Host "[OK] Data ingested from OSRS Wiki API" -ForegroundColor White
Write-Host "[OK] Bronze → Silver → Gold transformations completed" -ForegroundColor White
Write-Host "[OK] Items selected based on volume and coverage criteria" -ForegroundColor White
Write-Host "[OK] ML backtests executed and logged to MLflow" -ForegroundColor White
Write-Host "[OK] API endpoints tested and ready" -ForegroundColor White
Write-Host ""
Write-Host "🚀 Next Steps:" -ForegroundColor Cyan
Write-Host ""
Write-Host "1. Start the API server:" -ForegroundColor Yellow
Write-Host "   uvicorn api.main:app --reload" -ForegroundColor White
Write-Host ""
Write-Host "2. View the dashboard:" -ForegroundColor Yellow
Write-Host "   http://localhost:8000/ui" -ForegroundColor Green
Write-Host ""
Write-Host "3. Explore the API:" -ForegroundColor Yellow
Write-Host "   • http://localhost:8000/docs - Interactive API docs" -ForegroundColor White
Write-Host "   • http://localhost:8000/items - Available items" -ForegroundColor White
Write-Host "   • http://localhost:8000/health - Health check" -ForegroundColor White
Write-Host ""
Write-Host "4. View MLflow experiments:" -ForegroundColor Yellow
Write-Host "   mlflow ui --port 5000" -ForegroundColor White
Write-Host "   Then visit: http://localhost:5000" -ForegroundColor White
Write-Host ""
