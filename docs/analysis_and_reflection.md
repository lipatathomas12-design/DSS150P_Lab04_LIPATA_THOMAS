# Analysis and Reflection (Section 14)

**1. What prevented another person from reproducing the previous project immediately?**
The project was a notebook. It was run cell by cell with `!pip install` cells and unpinned library versions. The dataset was read from a session-local path, with no checksum to prove which file was used. Cleaning (median imputation, zero fixes, outlier removal) ran in place on one DataFrame, so the result depended on cell order. Grid-search results and metrics existed only as printed notebook output, with no machine-readable baseline.

**2. Which artifact is the true authoritative source?**
The original file `data/raw/bgg_dataset.csv`, whose identity is fixed by the SHA-256 in `source_manifest.yml`. The raw ingested copy is faithful evidence of what a run read, with audit columns, but it is not authoritative. Staging is the typed and cleaned representation. The curated dataset is the authoritative analysis-ready dataset that downstream steps read. The `ml` layer holds model-specific derivatives and predictions.

**3. What identifier or rule prevents duplicates, and what assumption makes it valid?**
In raw, the key is (`source_sha256`, `source_row_number`). This is valid because the source file is immutable and its checksum is verified, so a row position is stable. In staging and curated, the business key is the BGG `id`, which assumes one game per `id` and that the first occurrence is canonical. Rows without an `id` are removed in staging (16 rows). Predictions are keyed by `game_id`. `src/verify_rerun.py` counts duplicates for each key per layer.

**4. How was incremental ingestion simulated without pretending the checkpoint is a business timestamp?**
The dataset has no update timestamp. The file is split into deterministic batches of `batch_size` (default 5,000) by source row number, giving 5 batches for 20,343 rows. The checkpoint (last completed batch) is stored in `state/checkpoint.json`, outside the code, and advances only after a batch is written. Completed batches are skipped on rerun. The README and logs state that this is a laboratory simulation, not a natural watermark.

**5. Which transformations belong in staging versus curated, and why?**
Staging holds technical cleaning with no business meaning: type conversion (decimal commas to numbers), whitespace and null standardization, duplicate and missing-key handling, and audit columns. It can always be rebuilt from raw. Curated holds business rules and derived fields, and is the stable dataset other consumers rely on. Model-specific steps (outlier exclusion, encoding, scaling, the train/test split) come after curated, in the feature stage, because they are modeling decisions that should not change the shared dataset.

**6. Which storage format performed best for this workload, and why not generalize?**
Fill in from `benchmarks/results.json`: "In my run, <format> had the smallest size, <format> the fastest write, <format> the fastest full read and <format> the fastest selective query (median of 5 reads)." The result applies only to this workload: about 20,000 rows by 14 columns, which fits in memory, on my machine with a warm OS cache. Results depend on hardware, row and column counts, column types, caching, query shape and whether the first (cold) run is included. At 100x the size or with wide tables the ranking could change, so I do not claim a universal best format.

**7. What happens if Airflow retries the load task after the database committed but the task did not get confirmation?**
The retry runs the whole load again against data that is already committed. A plain insert would duplicate rows or fail on a key violation. The retried task must reach the same final state instead.

**8. How does the design make that retry safe?**
Loads are keyed by the business key defined in `sql/schema.sql`, so a rerun cannot create a second logical record. The load is idempotent and runs in one transaction. `verify_rerun.py` and the tests check that a second run leaves row counts and distinct-key counts unchanged. Airflow retries are limited to 2 with a 30-second delay.

**9. Which data-quality check is most likely to detect a change that silently damages model accuracy?**
The validity and consistency checks on the numeric inputs and the target (`Rating Average` ranges and agreement with `Rating Category`), because a shifted value or distribution can pass schema and null checks yet change the model. The final backstop is `compare_metrics`, which compares actual metrics with the baseline within +/-0.01.

**10. How do I know the training and test rows are the same logical rows as in the baseline?**
The split is stratified with a fixed `random_state` (42) on rows in a deterministic order, with the same preprocessing. The row counts match the baseline (14,840 rows, train 11,872 / test 2,968). `predictions.csv` lists the test `game_id`s. The notebook never saved row ids, so the identity of the baseline rows can only be shown through the same seed, ordering and counts. A stored split manifest with checksums of the train and test id lists would strengthen this.

**11. What sources of nondeterminism remain?**
Library and platform differences: floating-point results can vary slightly across CPU architectures and BLAS builds. Row order from PostgreSQL if a query lacks an explicit `ORDER BY`. Package or base-image updates that are not pinned. Run-specific values (timestamps, run ids) in audit columns, which are not model features. Gradient Boosting with a fixed seed is otherwise deterministic.

**12. If the reproduced metric differs, how do I find the cause?**
Eliminate causes in order. (1) Data: compare the source checksum and the raw, staging and curated row counts with the expected counts. (2) Preprocessing: check the feature count and column order (41 features) and the class distribution. (3) Split logic: check seed, ordering and the train/test counts. (4) Dependencies: compare `pip freeze` with `requirements.txt`. (5) Model parameters: compare `config/pipeline.yml` with `config/baseline_metrics.json`. (6) Nondeterminism: run the pipeline twice with identical inputs. If the two runs differ, the cause is the environment. If they match each other, the cause is in one of the earlier steps.

**13. What would I monitor if this pipeline ran every day?**
DAG run status, duration and retry counts. Source checksum and row-count drift. Null rates, value ranges and class distribution. Schema changes. Duplicate-key counts and row counts per layer. The metric versus the baseline. PostgreSQL availability and disk usage. I would add alerts on failure and on drift.

**14. Which part would become the first bottleneck if the dataset grew 100x?**
The single-process, in-memory pandas steps and the single-threaded scikit-learn Gradient Boosting training (about 2 million rows) would be first, followed by row-wise database loading if it is not done with bulk `COPY`. Airflow standalone with SQLite and a sequential executor cannot parallelize tasks. Mitigations: chunked or Parquet processing, `COPY` loads, a histogram-based or distributed learner, partitioned tables, and Airflow with a PostgreSQL metadata database and LocalExecutor or Celery.

**15. What would I change if the dataset became sensitive or regulated?**
Never commit raw data (use manifests and a secured store). Keep secrets in a secrets manager instead of `.env` files. Encrypt data at rest and in transit (TLS to PostgreSQL). Use least-privilege roles and per-layer access control. Mask or pseudonymize personal columns and keep them out of logs, metrics and predictions. Record data classification in the contract, enable audit logging, set retention and deletion rules, and keep lineage so subject requests can be traced. Separate development and production environments.