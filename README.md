# DSS150P Lab 04 — Reproducible BGG Rating Pipeline

Re-engineers the group predictive-analytics notebook (BoardGameGeek games -> 3-class
`Rating Category`, Gradient Boosting) as a reproducible data pipeline:
**raw -> staging -> curated -> model-ready -> model -> predictions/metrics**, with PostgreSQL,
Airflow, data-quality checks, and a machine-readable baseline comparison.

See `docs/architecture.mmd`, `docs/lineage.mmd` (render to PNG, see below), `docs/runbook.md`,
`docs/reproducibility_report.md`.

## Prerequisites
Docker + Docker Compose, **or** Python 3.11+ (and a PostgreSQL 16 for the DB steps).

## Setup
```bash
git clone https://github.com/SrPenguin000/DSS150P_Lab04_Banez_Fernand.git
cd DSS150P_Lab04_Banez_Fernand
cp .env.example .env            # set POSTGRES_PASSWORD; never commit .env
```
Data: `data/raw/bgg_dataset.csv` (identity checked against `source_manifest.yml`; fill in the
source URL and license there). Re-compute checksum values with `python scripts/update_manifest.py`.

## Run
**Docker (recommended)**
```bash
docker compose up -d postgres
docker compose run --rm pipeline                       # full run
docker compose run --rm pipeline python -m pytest -q  # tests
docker compose run --rm pipeline python -m src.verify_rerun   # duplicate-key check
docker compose run --rm pipeline python -m benchmarks.storage_benchmark
```
**Local venv**
```bash
python -m venv .venv && source .venv/bin/activate    
pip install -r requirements.txt
python -m src.run_pipeline --no-db                    
python -m src.run_pipeline                            
```
**Airflow**
```bash
docker compose up -d --build airflow    
docker compose logs airflow | grep -i password    
```
Trigger DAG `bgg_pipeline` (config `{"mode":"full"}`). Tasks:
`verify_source >> ingest_raw >> build_staging >> build_curated >> load_postgres >> build_features >> train_model >> compare_metrics >> finalize_run`.

**Incremental / checkpoint demo** (simulated: dataset has no update timestamp)
```bash
python -m src.ingest --mode incremental --batch-size 5000   # repeat until complete
```

## Expected outputs
`outputs/metrics.json`, `outputs/predictions.csv`, `outputs/metrics_comparison.json`
(`PASS` expected), `outputs/run_summary.json`, `logs/pipeline.log` (JSON lines),
PostgreSQL schemas `raw`, `staging`, `curated`, `ml`, `audit`.

Expected counts: raw 20,343 -> staging 20,327 -> curated 20,327 -> model rows 14,840
(train 11,872 / test 2,968, 41 features). Baseline accuracy 0.7281 (tolerance ±0.01).

## Validation
`pytest` covers transformations, data-quality checks, failure injection, determinism,
rerun safety, and the incremental checkpoint. `python -m src.verify_rerun` shows zero duplicate keys after reruns.

## Diagrams
Render `docs/*.mmd` to PNG (e.g. https://mermaid.live -> export PNG) and save as
`docs/architecture.png` and `docs/lineage.png`.

## Troubleshooting
See `docs/runbook.md`.