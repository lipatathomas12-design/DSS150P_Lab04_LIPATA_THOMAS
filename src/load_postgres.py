import json
import os

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from .common import ROOT, resolve, snake, utcnow


def connect():
    pw = os.environ.get("POSTGRES_PASSWORD")
    if not pw:
        raise RuntimeError("POSTGRES_PASSWORD is not set (copy .env.example to .env)")
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        dbname=os.environ.get("POSTGRES_DB", "bgg"),
        user=os.environ.get("POSTGRES_USER", "bgg"), password=pw)


def apply_schema(conn):
    with conn.cursor() as cur:
        cur.execute((ROOT / "sql" / "schema.sql").read_text(encoding="utf-8"))


def _rows(df):
    obj = df.astype(object).where(pd.notna(df), None)
    return list(obj.itertuples(index=False, name=None))


def upsert(cur, table, df, key_cols, update=True):
    df = df.rename(columns=snake)
    cols = list(df.columns)
    if update:
        sets = ", ".join(f"{c}=EXCLUDED.{c}" for c in cols if c not in key_cols)
        conflict = f"ON CONFLICT ({', '.join(key_cols)}) DO UPDATE SET {sets}"
    else:
        conflict = f"ON CONFLICT ({', '.join(key_cols)}) DO NOTHING"
    sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s {conflict}"
    execute_values(cur, sql, _rows(df), page_size=2000)


def load_layers(cfg):
    """Raw, staging, curated -> PostgreSQL (single transaction)."""
    raw = pd.read_parquet(resolve(cfg["paths"]["raw"]))
    stg = pd.read_parquet(resolve(cfg["paths"]["staging"]))
    cur_df = pd.read_parquet(resolve(cfg["paths"]["curated"]))
    conn = connect()
    try:
        apply_schema(conn)
        with conn.cursor() as cur:
            upsert(cur, "raw.bgg_games", raw, ["source_sha256", "source_row_number"], update=False)
            upsert(cur, "staging.bgg_games", stg, ["id"])
            upsert(cur, "curated.bgg_games", cur_df, ["id"])
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {"raw": len(raw), "staging": len(stg), "curated": len(cur_df)}


def load_predictions(cfg, run_id, model_name, predictions):
    df = predictions.assign(model_name=model_name, run_id=run_id, predicted_at=utcnow())
    conn = connect()
    try:
        apply_schema(conn)
        with conn.cursor() as cur:
            upsert(cur, "ml.predictions", df, ["game_id"])
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return len(df)


def record_run(run_data):
    c = run_data["counts"]
    conn = connect()
    try:
        apply_schema(conn)
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO audit.pipeline_runs (pipeline_run_id, started_at, finished_at, status, error,
                    raw_rows, staging_rows, curated_rows, train_rows, test_rows, prediction_rows, summary)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (pipeline_run_id) DO UPDATE SET finished_at=EXCLUDED.finished_at,
                    status=EXCLUDED.status, error=EXCLUDED.error, raw_rows=EXCLUDED.raw_rows,
                    staging_rows=EXCLUDED.staging_rows, curated_rows=EXCLUDED.curated_rows,
                    train_rows=EXCLUDED.train_rows, test_rows=EXCLUDED.test_rows,
                    prediction_rows=EXCLUDED.prediction_rows, summary=EXCLUDED.summary""",
                        (run_data["pipeline_run_id"], run_data["started_at"], run_data["finished_at"],
                         run_data["status"], run_data["error"], c.get("raw"), c.get("staging"),
                         c.get("curated"), c.get("train"), c.get("test"), c.get("predictions"),
                         json.dumps(run_data, default=str)))
            for q in run_data["quality"]:
                cur.execute("""
                    INSERT INTO audit.data_quality_results VALUES (%s,%s,%s,%s,%s)
                    ON CONFLICT (pipeline_run_id, stage, check_name)
                    DO UPDATE SET passed=EXCLUDED.passed, detail=EXCLUDED.detail""",
                            (run_data["pipeline_run_id"], q["stage"], q["check"], q["passed"], q["detail"]))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()