# OSRS Project

A complete data engineering pipeline for OSRS Grand Exchange price forecasting, built to demonstrate the fundamentals that matter in data roles.

## Why I Built This

I created this project to practice and showcase core data engineering skills:

- **Writing testable, modular data transformations** using dbt with proper testing and documentation
- **Proper experiment tracking and model versioning** with MLflow for reproducible ML workflows  
- **Building APIs that can handle real traffic patterns** with FastAPI, proper error handling, and structured logging
- **Creating documentation that teammates can actually use** with clear setup guides and comprehensive examples

The goal is a small but complete system that demonstrates these skills in action - the kind of foundation you'd want for any data product.

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   OSRS Wiki     │    │    Prefect      │    │     DuckDB      │
│  Real-time API  │───▶│  flows/ingest   │───▶│  warehouse/     │
│                 │    │                 │    │   osrs.duckdb   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
┌─────────────────┐    ┌─────────────────┐             │
│    FastAPI      │    │      dbt        │             │
│   api/main.py   │◀───│ bronze→silver→  │◀────────────┘
│                 │    │      gold       │
└─────────────────┘    └─────────────────┘
        │                       │
        ▼                       ▼
┌─────────────────┐    ┌─────────────────┐
│   Predictions   │    │    MLflow       │
│  /predict       │    │  models_ml/     │
│  /metrics       │    │   backtest.py   │
└─────────────────┘    └─────────────────┘
```

## Quickstart (Windows)

1. **Setup Environment**
   ```powershell
   # Create virtual environment
   python -m venv venv
   .\venv\Scripts\Activate.ps1
   
   # Install dependencies
   pip install -r requirements.txt
   
   # Copy environment template
   copy .env.example .env
   # Edit .env and set your OSRS_USER_AGENT
   ```

2. **Run Data Pipeline**
   ```powershell
   # Ingest OSRS data
   python flows/ingest_osrs.py
   
   # Transform data with dbt
   cd dbt
   dbt debug
   dbt build
   cd ..
   ```

3. **Start API**
   ```powershell
   uvicorn api.main:app --reload
   # Visit http://localhost:8000/health
   ```

## Data Sources

- **OSRS Wiki Real-time API**: Bulk endpoints for timeseries, latest prices, volumes, and item mapping
- **Rate Limited**: Polite requests with caching and backoff
- **User-Agent**: Always set descriptive header from environment

## Tech Stack

- **Orchestration**: Prefect (free tier)
- **Data Warehouse**: DuckDB (local file)
- **Transformations**: dbt Core
- **ML Tracking**: MLflow (local file backend)
- **API**: FastAPI + Pydantic v2
- **Data Quality**: Pandera schemas
- **Code Quality**: ruff + mypy

## Models

- **Baseline**: Last value, moving average
- **Statistical**: ETS (Exponential Smoothing) via statsmodels
- **Evaluation**: Rolling backtests with sMAPE/MAE metrics
- **No Heavy Models**: Explicit no Prophet/XGBoost to keep lean

## API Endpoints

- `GET /health` - Service health check
- `GET /items` - Available items for prediction
- `GET /predict?item_id=&horizon=1` - Price predictions
- `GET /metrics?item_id=&last=30d` - Model performance metrics

## Project Structure

```
flows/              # Prefect ingestion flows
dbt/               # dbt project (bronze→silver→gold)
warehouse/         # DuckDB database file
models_ml/         # ML backtesting and metrics
api/              # FastAPI application
tests/            # Unit and data tests
scripts/          # One-off utilities
```

## How Items Are Selected

The system uses a data-driven approach to select items for forecasting, balancing trading volume and data continuity.

### Selection Criteria

**Automatic Selection:**
- **Volume**: Median daily trading volume (units/day) over the last 365 days
- **Turnover**: Median units × median price = GP value traded per day
- **Coverage**: Percentage of days with trading data within the lookback window (365 days by default)
  - Formula: `(days_with_data / lookback_days) × 100`
  - Denominator: Total possible days in analysis period (e.g., 365)
  - Numerator: Unique dates where item had ≥1 trade recorded
- **Ranking**: Combined score using `log(median_units_per_day + 1) × coverage_pct / 100`
- **Minimums**: Configurable thresholds for data quality

**Manual Overrides:**
- **Include**: Force-include specific items (if they meet minimum data requirements)
- **Exclude**: Blacklist items that shouldn't be forecasted

### Configuration

Edit `config/items.yaml` to customize selection:

```yaml
auto_select:
  top_n: 10              # Number of items to auto-select
  min_days: 10           # Minimum days of data required
  min_volume: 1000       # Minimum daily volume (units)
  min_coverage_pct: 2.0  # Minimum coverage (%) in lookback period
  lookback_days: 365     # Analysis period

manual_include: [4151, 561]  # Always include these items (IDs)
manual_exclude: [995]        # Never include these items (IDs)
manual_include_names: ["Abyssal whip"]  # Include by name
manual_exclude_names: ["Coins"]         # Exclude by name
```

### Selection Modes

The item selection system supports three modes for different use cases:

**1. Auto Mode (Default)** - Purely data-driven selection:
```powershell
python scripts/select_items.py
# or explicitly:
python scripts/select_items.py --mode auto
```
- Applies all quality gates: `min_days`, `min_volume`, `min_coverage_pct`
- Ranks by scoring formula: `log(median_units_per_day + 1) × coverage_pct / 100`
- Selects top N items that pass all gates
- Ignores manual includes unless they pass gates naturally

**2. Hybrid Mode** - Combine auto-selection with manual picks:
```powershell
python scripts/select_items.py --mode hybrid
```
- Auto-selects top N items (as in auto mode)
- Adds manual includes from config (if they pass gates)
- Use `--force-include` to bypass `min_volume` gate for manual picks:
  ```powershell
  python scripts/select_items.py --mode hybrid --force-include
  ```
  *(Still enforces `min_days` and `min_coverage_pct` for data quality)*

**3. Manual Mode** - Use only manual overrides:
```powershell
python scripts/select_items.py --mode manual
```
- Disables automatic ranking
- Uses only items from `manual_include` and `manual_include_names`
- Still enforces `min_days` and `min_coverage_pct` gates

### Usage Examples

```powershell
# Run default auto-selection
python scripts/select_items.py

# Hybrid mode with force-include for low-volume manual picks
python scripts/select_items.py --mode hybrid --force-include

# Manual-only selection (for custom item lists)
python scripts/select_items.py --mode manual

# Use selected items in backtests
python scripts/run_backtest.py --items @config/items_selected.json

# View selected items in API
curl http://localhost:8000/items
```

The `/items` endpoint returns only curated items, not the full OSRS item database.

## Curating by Names

In addition to ID-based overrides, you can include/exclude items by name for easier configuration:

### Configuration Examples

```yaml
# config/items.yaml
manual_include_names: 
  - "Abyssal whip"      # Case-insensitive exact match
  - "Dragon longsword"  # Will resolve to item ID automatically
  - "nature rune"       # Case doesn't matter

manual_exclude_names:
  - "Coins"             # Exclude by name
  - "Fire cape"         # Untradeable items
```

### How Name Resolution Works

**Process:**
1. **Case-insensitive matching**: "ABYSSAL WHIP" matches "Abyssal whip"
2. **Exact match required**: Partial matches are ignored for safety
3. **Graceful handling**: Missing names log warnings but don't break the pipeline
4. **Deduplication**: Combined with ID-based includes, duplicates removed
5. **Local lookup only**: Uses warehouse mapping table, no external API calls

**Resolution Order:**
```
Auto-selected items (top N by ranking)
+ manual_include (IDs)
+ manual_include_names (resolved to IDs)
- manual_exclude (IDs)  
- manual_exclude_names (resolved to IDs)
= Final curated list
```

**Error Handling:**
- **Multiple matches**: Logs all matches and skips (e.g., if "Rune" matches multiple items)
- **No matches**: Logs warning and continues (names may change over time)
- **Pipeline continues**: Name resolution failures don't break item selection

### Coverage and Volume Definitions

**Coverage Calculation:**
- **Formula**: `(days_with_data / lookback_days) × 100`
- **Window**: Last 365 days by default (configurable via `lookback_days`)
- **Denominator**: Total possible days in analysis period
- **Numerator**: Unique dates where item had ≥1 trade recorded

**Volume Metrics:**
- **Units**: `median_units_per_day` - actual items traded (not GP value)
- **Turnover**: `median_turnover_gp` = units × median_price (GP value)
- **Ranking**: Based on `log(units + 1) × coverage_pct / 100`

## Technical Highlights

This project demonstrates several key technical practices:

**Data Engineering:**
- Modular, testable transformations with dbt (Bronze → Silver → Gold architecture)
- Comprehensive data quality tests and validation with Pandera schemas
- Incremental processing and proper handling of late-arriving data
- Automated data lineage and documentation

**MLOps & Experimentation:**
- Systematic experiment tracking with MLflow (metrics, parameters, artifacts)
- Rolling walk-forward validation for honest backtesting
- Automated model evaluation and comparison across multiple methods
- Reproducible ML workflows with proper versioning

**API Development:**
- Production-ready FastAPI with proper error handling and status codes
- Structured logging for observability and debugging
- Input validation with Pydantic v2 schemas
- Context-managed database connections and resource cleanup

**DevOps & Automation:**
- End-to-end pipeline automation with PowerShell scripts
- CI/CD setup with GitHub Actions
- Environment management and dependency pinning
- Clear documentation for local development and deployment

## Complete Usage Guide

### Step-by-Step Workflow

**🎯 Goal:** Set up the OSRS project, ingest data, select items, run backtests, and serve predictions via API.

#### 1. Initial Setup (First Time Only)

```powershell
# Option A: Full automated setup
.\scripts\first_run_setup.ps1

# Option B: Manual setup
.\scripts\setup.ps1
```

**What these scripts do:**
- **`first_run_setup.ps1`**: Complete first-time setup including data ingestion and dbt build
- **`setup.ps1`**: Basic environment setup (venv, dependencies, .env file)

#### 2. Data Pipeline Workflow

```powershell
# Step 2a: Ingest fresh OSRS data
python flows/ingest_osrs.py

# Step 2b: Transform data with dbt
cd dbt
dbt build --profiles-dir ..
cd ..

# Step 2c: Select items for forecasting (generates config/items_selected.json)
python scripts/select_items.py
# OR use automation script:
.\scripts\run_select_items.ps1
```

#### 3. Machine Learning & Backtesting

```powershell
# Run backtests using selected items
python scripts/run_backtest.py --items @config/items_selected.json

# Alternative: Run with specific items
python scripts/run_backtest.py --items "4151,561,5616" --methods last_value moving_average

# View MLflow results
mlflow ui --port 5000
# Then visit: http://localhost:5000
```

#### 4. API & Predictions

```powershell
# Start the API server
uvicorn api.main:app --reload

# Test endpoints (in another terminal)
curl http://localhost:8000/health
curl http://localhost:8000/items
curl "http://localhost:8000/predict?item_id=561&horizon=1"
curl "http://localhost:8000/metrics?item_id=561&last=30d"

# Access the web UI
open http://localhost:8000/ui
```

#### 5. Full Pipeline Automation

```powershell
# Run the complete pipeline (ingestion → dbt → backtests)
.\scripts\run_pipeline.ps1
```

### Script Reference

| Script | Purpose | When to Use |
|--------|---------|-------------|
| **`first_run_setup.ps1`** | Complete first-time setup | Initial project setup |
| **`setup.ps1`** | Basic environment setup only | If you want manual control |
| **`run_pipeline.ps1`** | Full pipeline automation | Regular data updates |
| **`run_select_items.ps1`** | Item selection automation | After ingestion |
| **`select_items.py`** | Item selection logic | Manual item curation |
| **`run_backtest.py`** | ML backtesting CLI | Model evaluation |
| **`config_utils.py`** | Shared configuration utilities | Used by other scripts |

### Recommended Workflow

**For Development:**
1. `.\scripts\first_run_setup.ps1` (first time only)
2. `.\scripts\run_pipeline.ps1` (regular updates)
3. `uvicorn api.main:app --reload` (start API)

**For Production:**
1. Schedule `.\scripts\run_pipeline.ps1` daily
2. Run API with `uvicorn api.main:app --host 0.0.0.0 --port 8000`

### Troubleshooting

**Common Issues:**
- **"Python not found"**: Install Python 3.11 and ensure it's in PATH
- **"ModuleNotFoundError"**: Activate virtual environment: `.\venv\Scripts\Activate.ps1`
- **"Database not found"**: Run data ingestion first: `python flows/ingest_osrs.py`
- **"No items selected"**: Relax criteria in `config/items.yaml` or add manual includes

**Data Quality:**
- Items need ≥10 days of data by default (configurable in `config/items.yaml`)
- Coverage calculation: `(days_with_data / 365) × 100%`
- Volume threshold: 1000 units/day minimum (configurable)

## Project Outcomes

This project successfully demonstrates:

- ✅ **Reliable data ingestion** with Prefect flows writing to DuckDB bronze tables
- ✅ **Modular transformations** with dbt building silver/gold layers plus testing
- ✅ **Proper ML workflows** with walk-forward backtests logged to MLflow
- ✅ **Production-ready API** serving predictions and metrics via FastAPI
- ✅ **Intelligent automation** with item-driven configuration and auto-selection
- ✅ **Professional documentation** with clear setup guides and technical explanations

## Web UI Dashboard

The project includes a responsive web dashboard at `/ui` that provides:

**📊 Three Main Panels:**
- **Selected Items Table**: Shows curated items with volume, coverage, and selection criteria
- **Performance Leaderboard**: Displays best-performing models per item (lowest sMAPE)
- **Item Analysis Chart**: Interactive price forecasting with Chart.js visualization

**🔧 Key Features:**
- **Real-time data**: All data fetched from API endpoints (no external calls)
- **Interactive charts**: Select items from dropdown to view metrics and forecasts
- **Responsive design**: Works on desktop and mobile devices
- **Accessibility**: ARIA labels, keyboard navigation, and semantic HTML
- **Error handling**: User-friendly messages for missing data

**📡 API Endpoints Used:**
- `GET /selection` - Returns curated item list with metadata
- `GET /leaderboard?last=30d` - Shows best method per item (sMAPE + MAE)
- `GET /predict?item_id=X&horizon=1` - Price forecasts for selected items
- `GET /metrics?item_id=X&last=30d` - Model performance metrics

## Development

```powershell
# Code quality
ruff check .
ruff format .
mypy .

# Run tests
python -m pytest tests/

# Run specific flow
python flows/ingest_osrs.py
```

## Limitations

- Local development only (no cloud deployment)
- Small subset of OSRS items for demonstration
- Simple forecasting models (no deep learning)
- File-based storage (DuckDB + MLflow local)

## License

This project is for portfolio demonstration purposes.
