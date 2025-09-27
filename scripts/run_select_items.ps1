# OSRS Signals - Item Selection Automation
# PowerShell script to run item selection after data ingestion

Write-Host "OSRS Signals - Running Item Selection" -ForegroundColor Green
Write-Host "====================================" -ForegroundColor Green

# Ensure we're in the project root
if (-not (Test-Path "config/items.yaml")) {
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

# Run item selection
Write-Host "`nRunning item selection..." -ForegroundColor Yellow
& $venvPython scripts/select_items.py

if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Item selection failed" -ForegroundColor Red
    exit 1
}

Write-Host "[OK] Item selection completed" -ForegroundColor Green

# Check if output file was created
if (Test-Path "config/items_selected.json") {
    Write-Host "[OK] Items configuration saved to config/items_selected.json" -ForegroundColor Green
    
    # Show summary
    Write-Host "`nSelected items summary:" -ForegroundColor Cyan
    $content = Get-Content "config/items_selected.json" | ConvertFrom-Json
    $itemCount = $content.items.Count
    Write-Host "  Total items: $itemCount" -ForegroundColor White
    
    if ($itemCount -gt 0) {
        Write-Host "  Items:" -ForegroundColor White
        foreach ($item in $content.items | Select-Object -First 5) {
            $units = [math]::Round($item.median_units_per_day)
            $turnover = if ($item.median_turnover_gp) { 
                $turnoverM = [math]::Round($item.median_turnover_gp / 1000000, 1)
                "${turnoverM}M gp/day"
            } else { "N/A" }
            Write-Host "    - $($item.id): $($item.name) ($units units/day, $turnover, $([math]::Round($item.coverage, 1))% coverage)" -ForegroundColor White
        }
        if ($itemCount -gt 5) {
            Write-Host "    ... and $($itemCount - 5) more items" -ForegroundColor White
        }
    }
} else {
    Write-Host "[WARNING] Items configuration file not found" -ForegroundColor Yellow
}

Write-Host "`nNext steps:" -ForegroundColor Cyan
Write-Host "1. Run backtests: .\venv\Scripts\python.exe scripts\run_backtest.py" -ForegroundColor White
Write-Host "2. Start API: .\venv\Scripts\uvicorn.exe api.main:app --reload" -ForegroundColor White
Write-Host "3. View items: curl http://localhost:8000/items" -ForegroundColor White

Write-Host "`nItem selection automation completed!" -ForegroundColor Green
