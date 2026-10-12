# Reproducibility Report — DSS150P Lab 04

## 1. Prior project vs engineered reproduction
| | Notebook (Colab) | Pipeline |
|---|---|---|
| Source | `files.upload()` | `data/raw/bgg_dataset.csv`, sha256 in `source_manifest.yml` |
| Cleaning | in-memory cells | `src/staging.py`, `src/curated.py` |
| Features / split | in-memory cells | `src/features.py` (seed 42, stratified 80/20) |
| Model | GridSearchCV over 6 models | Gradient Boosting with the notebook's tuned params fixed in `config/pipeline.yml` |
| Metrics | printed | `outputs/metrics.json` + `metrics_comparison.json` |

Primary metric: accuracy of the best tuned model. Baseline 0.7281 (weighted F1 0.7229).

## 2. Results (fill from your final run)
Reproduced accuracy / F1: _from `outputs/metrics.json`_. Status: _PASS/REVIEW/FAIL_. Tolerance ±0.01.
In my validation run the structural counts matched the notebook exactly (20,327 cleaned rows; 11,872 / 2,968 train/test; 41 features).

## 3. Deviations from the notebook (explain each)
- Rows with a blank `ID` are rejected in staging (the notebook dropped them at the end of cleaning; final row count is the same).
- `Name` gaps are filled with `Unknown` before being dropped from features (no effect on the model).
- `CURRENT_YEAR` (2026) is a config value, not hardcoded in a cell.
- Hyperparameter search is not rerun; the tuned parameters are fixed to keep the run fast and deterministic.
- Library versions are pinned in `requirements.txt`; the Colab run used whatever versions Colab shipped, which can explain small metric differences.

## 4. Deterministic controls
Fixed seeds (split, model), fixed row order (`source_row_number`), `ID` kept as index through the split, pinned dependencies,
`test_ids_sha256` in `metrics.json` to prove the same test rows across runs.

## 5. Known limitations / remaining nondeterminism
- Imputation medians and IQR outlier limits are computed on the full dataset before the split (kept for parity with the baseline; this is mild leakage and should be moved after the split in a production redesign).
- Substring matching for multi-hot encoding (`str.contains`) can mark a mechanic present when another mechanic's name contains it (kept for parity).
- Different CPU/BLAS or library versions can change tree-model results slightly.
- Incremental ingestion is simulated by source row number; the checkpoint is not a business timestamp.

## 6. Reflection questions
Answer the 15 questions from the lab handout here, in your own words.
