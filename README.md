# DSS150P Lab 04 - Reproducible BGG Rating Pipeline

| Field | Student Entry |
|---|---|
| Name | Thomas Lipata |
| Student Number | 2024109933 |
| Section | CM17 |
| Previous Predictive Analytics Group / Project Title | <fill in> |
| Dataset Name / Source | BoardGameGeek (BGG) games dataset, `bgg_dataset.csv` - source: https://www.kaggle.com/datasets/andrewmvd/board-games |
| Predictive Task | Classification (3-class `Rating Category`: Low / Medium / High) |

## 1. Purpose

In this lab I rebuilt our group predictive-analytics notebook (BoardGameGeek games -> 3-class `Rating Category`, Gradient Boosting) into a pipeline that can be rerun and reproduced. The goal is not a better model. It is that anyone can clone the repo, place the same source file, run the pipeline and regenerate the baseline result.

Pipeline: `source -> raw -> staging -> curated -> model-ready -> model -> predictions/metrics`, orchestrated by Apache Airflow, stored in PostgreSQL, checked by data-quality rules and compared automatically with the baseline metrics.

## 2. Architecture

```text
data/raw/bgg_dataset.csv   immutable source file (SHA-256 in source_manifest.yml)
   |  verify_source -> ingest_raw (full or incremental batches + audit columns)
   v
raw        faithful copy + ingestion metadata (source_sha256, source_row_number, run id)
   |  build_staging   types, null standardization, key/duplicate handling
   v
staging    typed, validated, deduplicated
   |  build_curated   business rules, derived fields
   v
curated    stable analysis-ready dataset
   |  load_postgres -> build_features   encoding, scaling, deterministic split
   v
ml         model input and predictions
   |  train_model -> compare_metrics
   v
outputs/   metrics.json, predictions.csv, metrics_comparison.json, run_summary.json
   |  finalize_run
   v
audit      run records
```

Each layer exists under `data/` and as a PostgreSQL schema of the same name (`raw`, `staging`, `curated`, `ml`, `audit`).

Diagrams: `docs/architecture.png`, `docs/lineage.png` (sources: `docs/architecture.mmd`, `docs/lineage.mmd`).

| Layer | Purpose | Ownership / semantics |
|---|---|---|
| Source | Original dataset | Never modified. Identity proven by SHA-256 |
| Raw | Faithful ingested copy | Source values as read + audit columns. Not authoritative |
| Staging | Typed and cleaned | Technical cleaning only. Rows without an `ID` are removed |
| Curated | Business-rule dataset | Authoritative dataset for downstream use |
| ml | Model input and predictions | Feature matrix, deterministic split, predictions |
| Outputs | Delivery | Metrics, predictions, model artifact |

## 3. Repository layout and deviations from the suggested structure

```text
config/        pipeline.yml, data_contract.yml, baseline_metrics.json
dags/          bgg_pipeline_dag.py
src/           ingest, staging, curated, features, load_postgres, quality, train,
               compare_metrics, tasks, run_pipeline, verify_rerun, common, constants
sql/           schema.sql
benchmarks/    storage_benchmark.py, results.json
scripts/       update_manifest.py
tests/         unit, integration, failure-injection tests
docs/          architecture, lineage, runbook, reproducibility report, analysis, evidence
outputs/       metrics, predictions, comparison, run summary
```

| Suggested | This repository | Reason |
|---|---|---|
| `metadata/` | `config/` and `source_manifest.yml` | Contract, baseline metrics and manifest kept next to the config |
| `data/source/` | `data/raw/bgg_dataset.csv` | The source file is the immutable raw input |
| `src/transform.py`, `storage.py`, `validate.py`, `evaluate.py`, `cli.py` | `staging.py`, `curated.py`, `features.py`, `load_postgres.py`, `quality.py`, `compare_metrics.py`, `run_pipeline.py` | Same separation of concerns, split by layer |
| `dags/predictive_repro_pipeline.py` | `dags/bgg_pipeline_dag.py` | Naming only |

## 4. Prerequisites

- Docker Desktop with Docker Compose, **or** Python 3.11+ and PostgreSQL 16.
- Pinned versions are in `requirements.txt` (pandas 2.2.2, numpy 1.26.4, scikit-learn 1.5.1, pyarrow 16.1.0, psycopg2-binary 2.9.9, joblib 1.4.2).

## 5. Setup

```powershell
git clone https://github.com/lipatathomas12-design/DSS150P_Lab04_LIPATA_THOMAS.git
cd DSS150P_Lab04_LIPATA_THOMAS
Copy-Item .env.example .env
```

On Linux/macOS use `cp .env.example .env`. Edit `.env` and set `POSTGRES_PASSWORD`. Never commit `.env`.

### Data placement and checksum

The source file must be at `data/raw/bgg_dataset.csv`. Verify it:

```powershell
Get-FileHash data/raw/bgg_dataset.csv -Algorithm SHA256
```

Compare the hash with `source_manifest.yml`. They must match. `.gitattributes` disables line-ending conversion for the CSV so the checksum stays stable on every OS. To recompute the manifest values: `python scripts/update_manifest.py`.

Source and licence: BoardGameGeek data used by the previous group project. Source URL: https://www.kaggle.com/datasets/andrewmvd/board-games (Kaggle, board-games dataset by andrewmvd). License / redistribution terms: <fill in>. The dataset has no personal data. If redistribution is not permitted, remove the CSV from Git and keep only the manifest and these placement instructions.

## 6. Run

**Docker (recommended)**

```powershell
docker compose up -d postgres
docker compose run --rm pipeline
docker compose run --rm pipeline python -m pytest -q
docker compose run --rm pipeline python -m src.verify_rerun
docker compose run --rm pipeline python -m benchmarks.storage_benchmark
```

**Local virtual environment (Windows)**

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m src.run_pipeline --no-db
python -m src.run_pipeline
```

On Linux/macOS activate with `source .venv/bin/activate`. `--no-db` runs without PostgreSQL.

**Incremental / checkpoint demonstration**

The dataset has no update timestamp, so incremental ingestion is a laboratory simulation: deterministic batches by source row number, with the checkpoint stored in `state/checkpoint.json`. It is not a business watermark.

```powershell
python -m src.ingest --mode incremental --batch-size 5000
```

Repeat until complete. Completed batches are skipped on rerun.

## 7. Airflow

```powershell
docker compose up -d --build airflow
docker compose logs airflow | Select-String "Login with username"
```

- URL: http://localhost:8081
- Username: `admin`. Password: printed in the log. It is regenerated whenever the container is recreated.
- Open DAG `bgg_pipeline`, unpause it and trigger it. Parameters: `mode` (full / incremental), `batch_size` (default 5000), `source_path`.
- Tasks: `verify_source >> ingest_raw >> build_staging >> build_curated >> load_postgres >> build_features >> train_model >> compare_metrics >> finalize_run`
- Retries: 2, retry delay 30 seconds. `finalize_run` always runs.
- Business logic lives in `src/`, not in the DAG file.
- If `ingest_raw` fails with `PermissionError` on `state/`, fix the bind-mount permissions:

```powershell
docker compose exec -u root airflow bash -c "chown -R 50000:0 /opt/airflow/project/state /opt/airflow/project/logs /opt/airflow/project/outputs /opt/airflow/project/data && chmod -R 777 /opt/airflow/project/state /opt/airflow/project/logs /opt/airflow/project/outputs /opt/airflow/project/data"
```

Airflow runs in `standalone` mode (SQLite metadata, sequential executor). This is for development only.

## 8. Expected outputs

`outputs/metrics.json`, `outputs/predictions.csv`, `outputs/metrics_comparison.json` (`PASS` expected), `outputs/run_summary.json`, `outputs/rerun_check.json`, `logs/pipeline.log` (JSON lines), and PostgreSQL schemas `raw`, `staging`, `curated`, `ml`, `audit`.

Expected counts: raw 20,343 -> staging 20,327 (16 source rows without an `ID` removed) -> curated 20,327 -> model rows 14,840 (train 11,872 / test 2,968, 41 features).

Baseline: accuracy 0.7281, weighted F1 0.7229, tolerance +/-0.01 (`config/baseline_metrics.json`).

## 9. Validation

- `python -m pytest -q` covers transformations, data-quality checks, failure injection, determinism, rerun safety and the incremental checkpoint.
- `python -m src.verify_rerun` shows zero duplicate keys per layer after reruns. Run the pipeline twice and compare the counts.
- `outputs/metrics_comparison.json` compares expected and actual metrics side by side.

## 10. Evidence

Screenshots and logs are in `docs/evidence/`: Airflow runs and graph, pytest output, rerun verification. See `docs/runbook.md` for failure and recovery procedures and `docs/reproducibility_report.md` for the comparison with the prior project.

## 11. Reflection questions

Answers to the 15 required analysis questions: `docs/analysis_and_reflection.md`.

## 12. Known limitations

- The incremental watermark is simulated from row position.
- Source URL and licence must be confirmed before the CSV is redistributed.
- Airflow standalone is not a production deployment.
- Library versions differ from the original Colab notebook, so metrics are compared within a tolerance instead of exactly.

## 13. Attribution and AI use

- Prior work: the group project notebook `Predictive_Analytics_Project_Final.ipynb` (group / project title:) supplies the problem, target, feature definitions and hyperparameters.
- AI assistance: Claude (Anthropic) was used for debugging help (Airflow, Docker, Git), documentation drafting and review. In this lab I used it to help with Git and PowerShell commands, to debug Airflow and Docker problems, and to draft the README and the reflection answers, which I then reviewed and edited. All code and documentation were reviewed by me.

## 14. Troubleshooting

See `docs/runbook.md`. Common issues:

| Symptom | Fix |
|---|---|
| Airflow UI not on 8080 | Use http://localhost:8081 |
| Cannot log in | The standalone password changes on every container start. Read it from the log |
| `PermissionError` on `state/` | Run the permission command in section 7 |
| pandas dependency warning during the Airflow build | Harmless. Airflow provider packages prefer an older pandas |
| `LF will be replaced by CRLF` Git warnings | Harmless. `.gitattributes` protects the dataset |