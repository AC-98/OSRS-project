# OSRS Price Forecasting Pipeline

A small data engineering project I built to practice the fundamentals: ingestion, transformations, backtesting, and serving predictions via API.

## What This Is

I wanted to build something that demonstrates the core skills used in data engineering and ML roles:

- **Data pipelines** - fetching data from an API, transforming it through layers (bronze → silver → gold)
- **SQL transformations** - using dbt to build reusable, testable data models
- **ML workflows** - running backtests, tracking experiments with MLflow
- **API development** - serving predictions with FastAPI

The project uses OSRS (Old School RuneScape) Grand Exchange price data because it's real-time, free, and interesting to work with. This isn't meant to be a trading bot or anything fancy - just a solid example of building a complete data system from scratch.

## Architecture

```
OSRS Wiki API → Prefect Ingestion → DuckDB (bronze tables)
                                          ↓
                                    dbt transforms
                                    (silver → gold)
                                          ↓
                      ┌──────────────────┴──────────────────┐
                      ↓                                      ↓
               MLflow backtests                      FastAPI predictions
               (statsmodels ETS)                    (/predict, /metrics)
```

**Stack:**
- **DuckDB** - local OLAP database (no server needed)
- **Prefect** - workflow orchestration
- **dbt** - SQL transformations with tests
- **MLflow** - experiment tracking
- **FastAPI** - API framework
- **statsmodels** - time series forecasting (ETS, moving averages)

## Quick Start

**1. Setup (first time only):**
```powershell
.\scripts\first_run_setup.ps1
```

This creates a virtual environment, installs dependencies, and sets up directories.

**2. Configure your User-Agent:**
Edit `.env` and set a descriptive User-Agent (required by OSRS Wiki API):
```
OSRS_USER_AGENT="your-project-name/1.0 (your-email@example.com)"
```

**3. Run the full pipeline:**
```powershell
.\scripts\run_pipeline.ps1
```

This will:
- Fetch data from OSRS Wiki API
- Transform it with dbt (bronze → silver → gold)
- Select items based on volume and coverage
- Run backtests and log metrics to MLflow
- Test API endpoints

**4. Start the API:**
```powershell
uvicorn api.main:app --reload
```

Visit `http://localhost:8000/ui` to see the dashboard.

## Data Sources

**OSRS Wiki Real-time API** - bulk endpoints for timeseries, latest prices, and item mapping
- Free and publicly available
- Rate-limited (450ms sleep between requests)
- Requires descriptive User-Agent header

## How Items Are Selected

Items are selected based on **trading volume** and **data continuity**:

**Metrics:**
- **Volume**: Median daily trading volume (units/day, not GP)
- **Turnover**: Median units × median price (GP traded per day)
- **Coverage**: `(days_with_data / 365) × 100%`
- **Ranking**: `log(volume + 1) × coverage / 100`

**Configuration** (`config/items.yaml`):
```yaml
auto_select:
  top_n: 10              # Number of items to auto-select
  min_days: 10           # Minimum days of data
  min_volume: 1000       # Minimum daily volume (units)
  min_coverage_pct: 2.0  # Minimum coverage (%)
  lookback_days: 365     # Analysis window

manual_include_names:
  - "Twisted bow"        # Force-include specific items
  - "Dragon claws"
```

**Three selection modes:**

1. **Auto mode** (default): Purely data-driven, selects top N items
   ```powershell
   python scripts/select_items.py
   ```

2. **Hybrid mode**: Auto-selection + manual includes
   ```powershell
   python scripts/select_items.py --mode hybrid --force-include
   ```

3. **Manual mode**: Only manually specified items
   ```powershell
   python scripts/select_items.py --mode manual
   ```

The pipeline uses **hybrid mode** to balance data-driven selection with manually curated high-value items.

## Config-Driven Ingestion

You can control which items are fetched without touching code:

**Configuration** (`config/items.yaml`):
```yaml
ingest_targets:
  # Include specific items by name
  include_names: ["Twisted bow", "Scythe of vitur"]
  
  # Include by ID
  include_ids: [4151, 561]
  
  # Exclude items
  exclude_names: ["Coins"]
  
  # Auto-discover liquid items based on GE buy limits
  auto_discover:
    enabled: true
    top_k: 10           # Number to auto-discover
    min_limit: 1000     # Minimum GE buy limit
    members_only: null  # null=both, true=members, false=F2P

runtime_caps:
  max_items_per_run: 25    # Cap per ingestion run
  request_sleep_ms: 450    # Sleep between API calls
```

**Priority order:**
1. Selected items (from `items_selected.json`)
2. Manual includes (IDs + names)
3. Auto-discovered items (by GE buy limit)
4. Emergency seed (3 items, if everything else is empty)

**Auto-discovery** uses GE buy limits as a proxy for liquidity. Items with higher limits (e.g., 10k+) tend to be more liquid and stable.

## Forecasting Models

I started simple and only added complexity where it helped:

**Baseline models:**
- **Last value** - just use the most recent price (hard to beat for daily forecasts!)
- **Moving average** - smooth out noise

**Statistical model:**
- **ETS** (Exponential Smoothing) from statsmodels - handles trends and seasonality

**Evaluation:**
- Rolling walk-forward backtests (honest time series validation)
- Metrics: sMAPE (symmetric mean absolute percentage error) and MAE
- All logged to MLflow for comparison

I intentionally avoided Prophet, XGBoost, or neural networks. For daily price forecasts with limited data, simpler models often win.

## API Endpoints

- `GET /health` - Health check
- `GET /items` - Available items with metadata
- `GET /selection` - Curated item list with selection criteria
- `GET /leaderboard?last=30d` - Best-performing models per item
- `GET /predict?item_id=X&horizon=1` - Price forecast
- `GET /metrics?item_id=X&last=30d` - Model performance metrics
- `GET /ui` - Web dashboard

All responses are JSON with proper error handling (400/404/500).

## Project Structure

```
flows/              # Prefect ingestion flows
dbt/
  ├── models/
  │   ├── bronze/   # Raw data from API
  │   ├── silver/   # Cleaned, validated data
  │   └── gold/     # Business logic, aggregations
  └── tests/        # Data quality tests
warehouse/          # DuckDB database file
models_ml/          # Backtesting logic
api/                # FastAPI application
scripts/            # Automation and utilities
config/             # Configuration files
tests/              # Unit tests
```

## Scripts Reference

| Script | Purpose | When to Use |
|--------|---------|-------------|
| `first_run_setup.ps1` | Complete first-time setup | Initial project setup |
| `run_pipeline.ps1` | Full pipeline (ingest → dbt → backtests) | Regular data updates |
| `setup.ps1` | Basic environment setup only | If you want manual control |
| `run_select_items.ps1` | Item selection with reporting | After ingestion |
| `select_items.py` | Item selection logic | Manual item curation |
| `run_backtest.py` | ML backtesting CLI | Model evaluation |

**Typical workflow:**
```powershell
# First time
.\scripts\first_run_setup.ps1

# Edit .env with your User-Agent
notepad .env

# Run full pipeline
.\scripts\run_pipeline.ps1

# Start API
uvicorn api.main:app --reload
```

## Web Dashboard

Visit `http://localhost:8000/ui` after starting the API.

**Features:**
- **Selected Items Table** - shows curated items with volume/coverage stats
- **Performance Leaderboard** - best-performing models per item
- **Item Analysis Chart** - interactive price forecasts with Chart.js

All data comes from API endpoints (no external calls).

## What I Learned

**Data Engineering:**
- Building modular, testable SQL transformations with dbt
- Handling late-arriving data and incremental processing
- Data quality gates with Pandera schemas
- Working with DuckDB (surprisingly fast for local analytics)

**MLOps:**
- Experiment tracking with MLflow (parameters, metrics, artifacts)
- Walk-forward validation for honest backtesting
- Reproducible ML workflows

**API Development:**
- FastAPI with proper error handling and logging
- Pydantic schemas for validation
- Context-managed database connections

**DevOps:**
- PowerShell automation for Windows
- Environment management and dependency pinning
- End-to-end pipeline orchestration

## Limitations & Trade-offs

**What this project is NOT:**
- ❌ Not a trading bot (no live trading, no alerts)
- ❌ Not production-ready (local files, no auth, no monitoring)
- ❌ Not optimized for scale (small dataset, single-threaded)
- ❌ Not using fancy ML (no deep learning, no Prophet/XGBoost)

**Why these choices:**
- I wanted to focus on fundamentals, not complexity
- Local-first (no cloud costs, no deployment headaches)
- Simple models that I can explain and debug
- Free tools only (Prefect, dbt Core, DuckDB, MLflow)

**Known issues:**
- Small dataset (only a few weeks of data for demo)
- Some items have sparse data (low coverage %)
- Backtests need 10+ days of data (configurable)
- API has no authentication or rate limiting

## Future Improvements (If I Come Back to This)

- Add more robust error handling in ingestion
- Implement proper logging and monitoring
- Add unit tests for dbt models
- Try multi-step forecasts (horizon > 1 day)
- Add anomaly detection for sudden price spikes
- Build a proper frontend (React/Vue)

## Troubleshooting

**"Python not found"**
- Install Python 3.11 from python.org

**"ModuleNotFoundError"**
- Activate venv: `.\venv\Scripts\Activate.ps1`
- Or use the full path: `.\venv\Scripts\python.exe`

**"Database not found"**
- Run ingestion first: `python flows/ingest_osrs.py`

**"No items selected"**
- Relax criteria in `config/items.yaml` (lower `min_volume`, `min_coverage_pct`)
- Or add manual includes: `manual_include_names: ["Twisted bow"]`

**"Failed to load item data" in UI**
- Run backtests for that item: `python scripts/run_backtest.py --items @config/items_selected.json`

## Why OSRS?

I chose Old School RuneScape because:
- Real-time price data is free and well-documented
- High trading volume (thousands of items, active economy)
- Interesting patterns (supply shocks, updates, seasonal trends)
- Nostalgic (I played as a kid)

Plus, it's a fun conversation starter in interviews.

## License

This project is for portfolio demonstration purposes.

## Acknowledgments

- **OSRS Wiki** for providing the free real-time API
- **Jagex** for creating RuneScape
- All the open-source maintainers of the tools I used

---

**Built over ~2 months as a learning project.**

If you're reading this for a job application: I know this isn't perfect, but it demonstrates that I can build things end-to-end and actually finish them. Happy to walk through any part of the code or discuss trade-offs I made.
