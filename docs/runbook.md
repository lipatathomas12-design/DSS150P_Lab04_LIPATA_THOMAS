# Runbook

## Normal execution
`docker compose up -d postgres` then `docker compose run --rm pipeline`, or trigger the `bgg_pipeline` DAG.
Success = `PIPELINE SUCCESS`, `outputs/metrics_comparison.json` status `PASS`, `audit.pipeline_runs.status = 'SUCCESS'`.

## Common failures
| Symptom | Cause | Fix |
|---|---|---|
| `SourceError: Checksum mismatch` | CSV changed / wrong file | Restore the original file; or, if the change is legitimate, update `source_manifest.yml` and the baseline, and document it |
| `DataQualityError [raw/schema]` | Required column missing | Restore the column in the source |
| `DataQualityError [curated/...]` | Duplicates, nulls, out-of-range values, Min > Max players | Inspect `state/runs/<run_id>.json` (`quality`) and `logs/pipeline.log`; fix source/cleaning rule |
| `DataQualityError [model_ready/class_share]` | A target class collapsed (<1%) | Check binning, outlier filter, source |
| `POSTGRES_PASSWORD is not set` / connection refused | `.env` missing or DB down | `cp .env.example .env`; `docker compose up -d postgres` |
| `compare_metrics` FAIL | Metric outside tolerance or row counts differ | Compare `metrics_comparison.json` `structural_checks` first (data/split), then library versions in `metrics.json.environment` |
| Permission denied writing `data/`, `state/` (Linux + Airflow) | Container UID | `chmod -R a+rwX data state outputs logs` |

## Restart / rerun
Every task is idempotent: re-run the whole pipeline or a single Airflow task. Loads are upserts
keyed on `id` / `(source_sha256, source_row_number)`, so reruns and retries do not duplicate rows.

## Checkpoint recovery
`state/checkpoint.json` holds the last loaded source row. Raw data is written **before** the checkpoint,
so a crash only re-reads one batch (deduplicated on `source_row_number`). To restart ingestion from zero:
`rm state/checkpoint.json` and run `--mode full`.

## Database reset
`docker compose down -v` (deletes the volume) then `docker compose up -d postgres`.

## Rollback / cleanup
Generated layers are disposable: `rm -rf data/staging/* data/curated/* data/raw/bgg_raw.parquet state/* outputs/*.json outputs/*.csv`
and rerun. The source CSV and `config/` are the only authoritative inputs.

## Failure injection (demo)
```bash
python - <<'PY'
import pandas as pd
df = pd.read_csv("data/raw/bgg_dataset.csv", sep=";", dtype=str, encoding="utf-8-sig", keep_default_na=False)
df.drop(columns=["Rating Average"]).to_csv("/tmp/bad.csv", sep=";", index=False)
PY
python -m src.run_pipeline --no-db --source /tmp/bad.csv                  # 1) stops at checksum
python -m src.run_pipeline --no-db --source /tmp/bad.csv --skip-checksum  # 2) stops at schema check
python -m src.run_pipeline --no-db                                        # 3) recovery: clean rerun passes
```
Evidence: `logs/pipeline.log` (error events), `outputs/run_summary.json`, `audit.pipeline_runs`.
