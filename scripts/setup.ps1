# OSRS Signals - Setup Script
# Simple PowerShell script for Windows setup

Write-Host "OSRS Signals - Setup Script" -ForegroundColor Green
Write-Host "============================" -ForegroundColor Green

# Step 1: Check Python
Write-Host "`nStep 1: Checking Python..." -ForegroundColor Yellow
try {
    $pythonVersion = py --version 2>&1
    Write-Host "[OK] Found: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "[ERROR] Python not found. Please install Python 3.11" -ForegroundColor Red
    Write-Host "Download from: https://www.python.org/downloads/" -ForegroundColor Yellow
    exit 1
}

# Step 2: Create virtual environment
Write-Host "`nStep 2: Creating virtual environment..." -ForegroundColor Yellow
if (Test-Path "venv") {
    Write-Host "Removing existing venv..." -ForegroundColor Gray
    Remove-Item -Recurse -Force venv
}

py -m venv venv
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Failed to create virtual environment" -ForegroundColor Red
    exit 1
}
Write-Host "[OK] Virtual environment created" -ForegroundColor Green

# Step 3: Install dependencies
Write-Host "`nStep 3: Installing dependencies..." -ForegroundColor Yellow
.\venv\Scripts\python.exe -m pip install --upgrade pip
.\venv\Scripts\pip.exe install -r requirements.txt
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Failed to install dependencies" -ForegroundColor Red
    exit 1
}
Write-Host "[OK] Dependencies installed" -ForegroundColor Green

# Step 4: Create environment file
Write-Host "`nStep 4: Setting up environment..." -ForegroundColor Yellow
if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "[OK] Created .env file" -ForegroundColor Green
    Write-Host "[INFO] Edit .env and set OSRS_USER_AGENT" -ForegroundColor Cyan
} else {
    Write-Host "[OK] .env file exists" -ForegroundColor Green
}

# Step 5: Create directories
Write-Host "`nStep 5: Creating directories..." -ForegroundColor Yellow
@("warehouse", "warehouse\raw") | ForEach-Object {
    if (-not (Test-Path $_)) {
        New-Item -ItemType Directory -Path $_ -Force | Out-Null
    }
}
Write-Host "[OK] Directories created" -ForegroundColor Green

# Step 6: Test core imports
Write-Host "`nStep 6: Testing core imports..." -ForegroundColor Yellow
$testScript = @"
import sys
modules_to_test = ['pandas', 'numpy', 'duckdb', 'fastapi', 'mlflow']
failed = []

for module in modules_to_test:
    try:
        __import__(module)
        print(f'[OK] {module}')
    except ImportError as e:
        print(f'[ERROR] {module}: {e}')
        failed.append(module)

if failed:
    print(f'[ERROR] Failed to import: {", ".join(failed)}')
    sys.exit(1)
else:
    print('[OK] All core imports successful')
"@

$testScript | .\venv\Scripts\python.exe
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Import test failed" -ForegroundColor Red
    exit 1
}
Write-Host "[OK] Import test passed" -ForegroundColor Green

# Summary
Write-Host "`nSetup Complete!" -ForegroundColor Green
Write-Host "===============" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "1. Edit .env file with your OSRS_USER_AGENT" -ForegroundColor White
Write-Host "2. Activate venv: .\venv\Scripts\Activate.ps1" -ForegroundColor White
Write-Host "3. Run pipeline: python flows\ingest_osrs.py" -ForegroundColor White
Write-Host "4. Build dbt: cd dbt && dbt build --profiles-dir ." -ForegroundColor White
Write-Host "5. Start API: uvicorn api.main:app --reload" -ForegroundColor White
