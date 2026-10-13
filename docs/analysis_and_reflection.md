# Analysis and Reflection Questions

**1. What stopped someone else from reproducing our old project right away?**
The old project was one notebook that ran in Colab, with `!pip install` cells and no pinned library versions. Nothing proved which version of the CSV we used. The cleaning steps changed one DataFrame step by step, so the result depended on running the cells in the right order. The metrics only existed as printed output, so no script could check them.

**2. Which one is the real source: the original file, the raw copy, the curated data or the model-ready data?**
The original file `data/raw/bgg_dataset.csv` is the real source, and its SHA-256 in `source_manifest.yml` proves it is the right file. The raw copy is just a faithful copy with extra audit columns, so I don't treat it as the source. Staging is the cleaned and typed version. Curated is the dataset the rest of the pipeline relies on. The model-ready data (features, split) is built from curated, so it comes last.

**3. What stops duplicates, and what do I assume for it to work?**
In raw I use the source file's row number together with the file's checksum. That works because the file never changes and I verify its checksum before every run. From staging onward I use the BGG `ID` as the business key. I assume one `ID` means one game and that the first row with an `ID` is the right one. The 16 rows with no `ID` are removed in staging. `src/verify_rerun.py` counts duplicates per layer after reruns.

**4. How did I do incremental loading when the data has no timestamp?**
I split the file into fixed batches of `batch_size` rows (5,000 by default) by row position, which gives 5 batches for 20,343 rows. The checkpoint stores the last finished batch in `state/checkpoint.json`, outside the code. It only moves forward after a batch is safely written, and finished batches are skipped when I rerun. This is a simulation for the lab. The checkpoint is a row position, not a real business timestamp, and the README says so.

**5. What goes in staging and what goes in curated?**
Staging has the technical cleanup that doesn't depend on business meaning: converting types (decimal commas to numbers), trimming text, handling nulls, removing rows with no `ID` and dropping duplicates. Curated has the business rules and derived fields, and it is the dataset others should use. Outlier removal, encoding, scaling and the train/test split are modeling choices, so I keep them after curated.

**6. Which storage format was best for my workload, and why not say it is best for everything?**
In my benchmark, <format> was the smallest, <format> had the fastest write, <format> had the fastest full read and <format> had the fastest filtered read (median of 5 reads). These numbers are from `benchmarks/results.json`. They only describe this dataset (about 20,000 rows and 14 columns), my laptop and a warm file cache. With a much bigger dataset, wider tables or a different query the order could change, so I don't claim one format is always best.

**7. What happens if Airflow retries the load after the database already committed?**
The load runs again on data that is already in the tables. If the load only did plain inserts, it would create duplicates or fail on a key error. A retry has to end in the same state as a single run.

**8. How does my design make the retry safe?**
The load is keyed by the business key in `sql/schema.sql`, so running it again can't create a second copy of a row. The load runs as one transaction, and I check with `verify_rerun.py` and the tests that a second run keeps the same row counts and distinct keys. Airflow retries twice with a 30-second delay.

**9. Which data-quality check would catch a silent drop in model accuracy?**
The value-range and consistency checks on the numeric columns and the rating target. A shifted value or changed distribution still passes schema and null checks but can change the model. The last safety net is `compare_metrics`, which compares my metrics with the baseline within +/-0.01.

**10. How do I know the train and test rows are the same as the baseline?**
I use the same stratified split with `random_state` 42, rows in the same order and the same preprocessing. The counts match the baseline: 14,840 rows, with 11,872 for training and 2,968 for testing. The notebook never saved row ids, so I can't prove the exact rows match. I can only show it through the same seed, order and counts. A saved split manifest with checksums of the train and test ids would make this stronger.

**11. What still isn't deterministic?**
Floating-point results can change slightly between CPUs and library builds. A PostgreSQL query without `ORDER BY` can return rows in a different order. Unpinned base images or packages can change. Timestamps and run ids change every run, but they aren't model features. With a fixed seed, Gradient Boosting itself gives the same result each time.

**12. If my metric is different from the baseline, how do I find the cause?**
I check one thing at a time. First the data: source checksum and the row counts per layer. Then preprocessing: the feature count (41), column order and class balance. Then the split: seed, row order and train/test counts. Then dependencies: `pip freeze` against `requirements.txt`. Then model settings: `config/pipeline.yml` against `config/baseline_metrics.json`. Last, I run the pipeline twice on the same input. If the two runs differ from each other, the environment is the cause. If they match, the problem is in one of the earlier steps.

**13. What would I monitor if it ran every day?**
DAG success, run time and retries. Source checksum and row-count changes. Null rates, value ranges and class balance. Schema changes. Duplicate and row counts per layer. The metric compared with the baseline. Database availability and disk space. I would add alerts for failures and drift.

**14. What would break first if the data were 100 times bigger?**
Loading everything into pandas in memory and training Gradient Boosting on about 2 million rows on a single thread would be the first bottleneck, then row-by-row database loading. Airflow standalone with SQLite and a sequential executor can't run tasks in parallel either. I would process in chunks or Parquet files, load with `COPY`, use a faster model, partition the tables and use Airflow with a PostgreSQL metadata database.

**15. What would I change if the data were sensitive or regulated?**
I would keep raw data out of Git completely and store secrets in a secrets manager instead of `.env` files. I would encrypt data at rest and in transit, give each layer its own access roles, mask personal columns and keep them out of logs, metrics and predictions. I would add data classification, audit logging, retention rules and separate dev and production environments.