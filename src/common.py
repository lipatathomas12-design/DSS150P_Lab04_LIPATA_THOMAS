import json
import os
import re
import sys
import time
import traceback
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import yaml

try:  # optional: load .env for local runs
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
except ImportError:  # pragma: no cover
    pass

ROOT = Path(__file__).resolve().parents[1]


def utcnow():
    return datetime.now(timezone.utc)


def load_config(path=None):
    path = Path(path or os.environ.get("PIPELINE_CONFIG", ROOT / "config" / "pipeline.yml"))
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def resolve(rel_path):
    p = Path(rel_path)
    return p if p.is_absolute() else ROOT / p


def snake(col):
    return re.sub(r"\W+", "_", col.strip()).lower()


def sanitize_run_id(run_id):
    return re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)


def new_run_id():
    return utcnow().strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]


def use_db(cfg):
    if os.environ.get("PIPELINE_USE_DB") is not None:
        return os.environ["PIPELINE_USE_DB"] not in ("0", "false", "False")
    return bool(cfg.get("database", {}).get("enabled", True))


def write_parquet(df, path):
    """Atomic write: a crash never leaves a half-written layer behind."""
    path = resolve(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.to_parquet(tmp, index=False)
    os.replace(tmp, path)


def log_event(run_id, stage, event, **fields):
    rec = {"timestamp": utcnow().isoformat(), "pipeline_run_id": run_id,
           "stage": stage, "event": event, **fields}
    line = json.dumps(rec, default=str)
    print(line, file=sys.stdout, flush=True)
    log_dir = ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    with open(log_dir / "pipeline.log", "a", encoding="utf-8") as fh:
        fh.write(line + "\n")


class Run:
    """Run record persisted to state/runs/<run_id>.json so separate processes
    (e.g. Airflow tasks) share one summary."""

    def __init__(self, run_id=None):
        self.run_id = sanitize_run_id(run_id) if run_id else new_run_id()
        self.path = ROOT / "state" / "runs" / f"{self.run_id}.json"
        if self.path.exists():
            self.data = json.loads(self.path.read_text(encoding="utf-8"))
        else:
            self.data = {"pipeline_run_id": self.run_id, "started_at": utcnow().isoformat(),
                         "finished_at": None, "status": "RUNNING", "error": None,
                         "counts": {}, "stages": [], "quality": []}
            self.save()

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.data, indent=2, default=str), encoding="utf-8")

    def set_count(self, name, n):
        self.data["counts"][name] = int(n)
        self.save()

    def add_quality(self, results):
        self.data["quality"].extend(results)
        self.save()

    @contextmanager
    def stage(self, name):
        entry = {"stage": name, "started_at": utcnow().isoformat(), "status": "RUNNING"}
        self.data["status"], self.data["error"] = "RUNNING", None
        log_event(self.run_id, name, "start")
        t0 = time.perf_counter()
        try:
            yield entry
        except Exception as exc:
            entry.update(status="FAILED", error=f"{type(exc).__name__}: {exc}",
                         duration_s=round(time.perf_counter() - t0, 3))
            self.data["stages"].append(entry)
            self.data["status"], self.data["error"] = "FAILED", entry["error"]
            self.save()
            log_event(self.run_id, name, "error", error=entry["error"],
                      duration_s=entry["duration_s"], traceback=traceback.format_exc())
            raise
        entry.update(status="SUCCESS", duration_s=round(time.perf_counter() - t0, 3))
        self.data["stages"].append(entry)
        self.save()
        log_event(self.run_id, name, "end",
                  **{k: v for k, v in entry.items() if k in ("rows_in", "rows_out", "duration_s")})

    def finish(self):
        self.data["finished_at"] = utcnow().isoformat()
        if self.data["status"] != "FAILED":
            self.data["status"] = "SUCCESS"
        self.save()
        out = ROOT / "outputs" / "run_summary.json"
        out.parent.mkdir(exist_ok=True)
        out.write_text(json.dumps(self.data, indent=2, default=str), encoding="utf-8")
        log_event(self.run_id, "pipeline", "finish", status=self.data["status"],
                  counts=self.data["counts"])
        return self.data