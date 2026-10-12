flowchart TB
    S["bgg_dataset.csv<br/>14 cols, 20,343 rows (; and decimal comma)"] --> R["raw: all text + source_row_number,<br/>source_sha256, ingested_at, ingest_run_id"]
    R -->|"trim, blank/'nan' to null,<br/>comma to point, to numeric,<br/>reject null ID, dedupe on ID"| ST["staging: typed, 1 row per ID<br/>20,327 rows"]
    ST -->|"drop null Year, median fills,<br/>zero fixes, Min/Max players, int types"| CU["curated: cleaned<br/>20,327 rows"]
    CU -->|"drop ID/Name, IQR outliers on 5 cols,<br/>multi-hot Mechanics (top 20) + Domains,<br/>Player Range, Average Players, Game Age,<br/>categories, Pop_Complexity, Rating Category"| FE["features: 14,840 rows, 41 X cols + target"]
    FE -->|"stratified 80/20, seed 42,<br/>MinMax scaler fit on train"| MR["model-ready: 11,872 train / 2,968 test"]
    MR -->|"GradientBoosting (fixed params, seed 42)"| MO[model.joblib]
    MO --> PR["predictions.csv + ml.predictions<br/>(2,968 rows)"]
    MO --> ME[metrics.json]
    BASE[config/baseline_metrics.json] --> CMP[metrics_comparison.json]
    ME --> CMP
