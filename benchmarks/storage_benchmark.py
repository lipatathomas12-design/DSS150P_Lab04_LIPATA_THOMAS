import json
import shutil
import warnings
import tempfile
import time
from pathlib import Path

import pandas as pd

from src.common import ROOT, load_config, resolve
from src.constants import COLS

REPEATS = 3
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy")


def timed(fn):
    best = float("inf")
    for _ in range(REPEATS):
        t0 = time.perf_counter(); out = fn(); best = min(best, time.perf_counter() - t0)
    return round(best, 4), out


def main():
    cfg = load_config()
    df = pd.read_parquet(resolve(cfg["paths"]["curated"]))[COLS]
    tmp = Path(tempfile.mkdtemp())
    results, id_sets = {}, {}

    for fmt, write, read in [
        ("csv", lambda p: df.to_csv(p, index=False), pd.read_csv),
        ("json", lambda p: df.to_json(p, orient="records"), lambda p: pd.read_json(p, orient="records")),
        ("parquet", lambda p: df.to_parquet(p, index=False), pd.read_parquet),
    ]:
        path = tmp / f"curated.{fmt}"
        w, _ = timed(lambda: write(path))
        r, back = timed(lambda: read(path))
        results[fmt] = {"write_s": w, "read_s": r, "size_mb": round(path.stat().st_size / 1e6, 3), "rows": len(back)}
        id_sets[fmt] = set(back["ID"])

    try:                                         
        from src.load_postgres import connect
        conn = connect()
        def pg_write():
            with conn.cursor() as cur:
                cur.execute("CREATE SCHEMA IF NOT EXISTS bench; DROP TABLE IF EXISTS bench.curated;"
                            "CREATE TABLE bench.curated AS SELECT id,name,year_published,min_players,max_players,"
                            "play_time,min_age,users_rated,rating_average,bgg_rank,complexity_average,owned_users,"
                            "mechanics,domains FROM curated.bgg_games WITH NO DATA")
                from psycopg2.extras import execute_values
                cols = [c.lower().replace(" ", "_") for c in COLS]
                execute_values(cur, f"INSERT INTO bench.curated ({','.join(cols)}) VALUES %s",
                               list(df.astype(object).itertuples(index=False, name=None)), page_size=2000)
            conn.commit()
        def pg_read():
            return pd.read_sql("SELECT * FROM bench.curated", conn)
        w, _ = timed(pg_write)
        r, back = timed(pg_read)
        with conn.cursor() as cur:
            cur.execute("SELECT pg_total_relation_size('bench.curated')"); size = cur.fetchone()[0]
        conn.close()
        results["postgresql"] = {"write_s": w, "read_s": r, "size_mb": round(size / 1e6, 3), "rows": len(back)}
        id_sets["postgresql"] = set(back["id"])
    except Exception as exc:
        results["postgresql"] = {"skipped": f"{type(exc).__name__}: {exc}"}

    ref = id_sets["csv"]
    same = all(s == ref for s in id_sets.values())

   
    part_dir = ROOT / "benchmarks" / "partitions"
    shutil.rmtree(part_dir, ignore_errors=True)
    df.assign(decade=(df["Year Published"] // 10 * 10).astype(int)).to_parquet(
        part_dir, partition_cols=["decade"], index=False)
    t0 = time.perf_counter()
    one = pd.read_parquet(part_dir, filters=[("decade", "=", 2010)])
    partition = {"partition": "decade=2010", "rows_read": len(one),
                 "read_s": round(time.perf_counter() - t0, 4), "total_rows": len(df)}

    out = {"records": len(df), "same_logical_records_across_formats": same,
           "repeats_best_of": REPEATS, "formats": results, "partition_demo": partition,
           "caveat": "Single machine, ~20k rows, warm cache: do not generalize to other workloads."}
    (ROOT / "benchmarks" / "results.json").write_text(json.dumps(out, indent=2))
    print(json.dumps(out, indent=2))
    shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()