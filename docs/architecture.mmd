flowchart LR
    CSV[(bgg_dataset.csv<br/>+ source_manifest.yml)] -->|sha256 verify| ING[ingest.py<br/>full / checkpoint]
    ING --> RAW[raw parquet<br/>text + audit cols]
    RAW --> STG[staging.py<br/>typed, deduped]
    STG --> CUR[curated.py<br/>cleaned]
    CUR --> FEAT[features.py<br/>encode, split, scale]
    FEAT --> MR[model-ready parquet]
    MR --> TRAIN[train.py<br/>GradientBoosting]
    TRAIN --> OUT[metrics.json<br/>predictions.csv]
    OUT --> CMP[compare_metrics.py<br/>vs baseline]
    RAW & STG & CUR & OUT --> PG[(PostgreSQL<br/>raw / staging / curated / ml / audit)]
    DQ{{quality.py<br/>data contract}} -.checks.-> RAW & STG & CUR & MR
    AF[Airflow DAG<br/>bgg_pipeline] -.orchestrates.-> ING & STG & CUR & FEAT & TRAIN & CMP
