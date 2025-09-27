# OSRS Signals - First Run Setup Script
# PowerShell script to set up the project for the first time on Windows

Write-Host "🚀 OSRS Signals - First Run Setup" -ForegroundColor Green
Write-Host "=================================" -ForegroundColor Green

# Check if Python is available
try {
    $pythonVersion = py --version 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "[OK] Found Python: $pythonVersion" -ForegroundColor Green
    } else {
        throw "Python not found"
    }
} catch {
    Write-Host "[ERROR] Python not found. Please install Python 3.11 first." -ForegroundColor Red
    Write-Host "[INFO] You can install Python from: https://www.python.org/downloads/" -ForegroundColor Yellow
    exit 1
}

# Step 1: Create virtual environment
Write-Host "`n📦 Step 1: Creating virtual environment..." -ForegroundColor Yellow
if (Test-Path "venv") {
    Write-Host "Virtual environment already exists. Removing old one..." -ForegroundColor Yellow
    Remove-Item -Recurse -Force venv
}

py -m venv venv
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Failed to create virtual environment" -ForegroundColor Red
    exit 1
}

Write-Host "[OK] Virtual environment created" -ForegroundColor Green

# Step 2: Set up virtual environment paths
Write-Host "`n🔧 Step 2: Setting up virtual environment..." -ForegroundColor Yellow
$venvPython = ".\venv\Scripts\python.exe"
$venvPip = ".\venv\Scripts\pip.exe"

if (-not (Test-Path $venvPython)) {
    Write-Host "[ERROR] Virtual environment Python not found at $venvPython" -ForegroundColor Red
    exit 1
}

Write-Host "[OK] Virtual environment ready" -ForegroundColor Green

# Step 3: Upgrade pip in virtual environment
Write-Host "`n⬆️  Step 3: Upgrading pip in virtual environment..." -ForegroundColor Yellow
& $venvPython -m pip install --upgrade pip
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Failed to upgrade pip" -ForegroundColor Red
    exit 1
}
Write-Host "[OK] pip upgraded" -ForegroundColor Green

# Step 4: Install dependencies in virtual environment
Write-Host "`n📚 Step 4: Installing dependencies..." -ForegroundColor Yellow
& $venvPip install -r requirements.txt
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Failed to install dependencies" -ForegroundColor Red
    exit 1
}

Write-Host "[OK] Dependencies installed" -ForegroundColor Green

# Step 5: Set up environment file
Write-Host "`n🔐 Step 5: Setting up environment file..." -ForegroundColor Yellow
if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "[OK] Created .env file from template" -ForegroundColor Green
    Write-Host "[INFO] Please edit .env file and set your OSRS_USER_AGENT" -ForegroundColor Cyan
} else {
    Write-Host "[OK] .env file already exists" -ForegroundColor Green
}

# Step 6: Create warehouse directory
Write-Host "`n🏗️  Step 6: Creating warehouse directory..." -ForegroundColor Yellow
if (-not (Test-Path "warehouse")) {
    New-Item -ItemType Directory -Path "warehouse"
    New-Item -ItemType Directory -Path "warehouse/raw"
}
Write-Host "[OK] Warehouse directory ready" -ForegroundColor Green

# Step 7: Test imports in virtual environment
Write-Host "`n🧪 Step 7: Testing imports..." -ForegroundColor Yellow
& $venvPython -c "
import pandas as pd
import duckdb
import prefect
import fastapi
import mlflow
print('[OK] All core imports successful')
"
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Import test failed" -ForegroundColor Red
    exit 1
}

Write-Host "[OK] Import test passed" -ForegroundColor Green

# Summary
Write-Host "`n🎉 Setup Complete!" -ForegroundColor Green
Write-Host "=================" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "1. Edit .env file and set your OSRS_USER_AGENT" -ForegroundColor White
Write-Host "2. Run the data ingestion: python flows/ingest_osrs.py" -ForegroundColor White
Write-Host "3. Build dbt models: cd dbt && dbt debug && dbt build" -ForegroundColor White
Write-Host "4. Start the API: uvicorn api.main:app --reload" -ForegroundColor White
Write-Host "5. Visit http://localhost:8000/health to test" -ForegroundColor White
Write-Host ""
Write-Host "For more details, see README.md" -ForegroundColor Gray
