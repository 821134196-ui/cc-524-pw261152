"""Batch execution engine. A fixed thread pool runs batches in parallel; each
worker opens its own SQLite connection (WAL mode). Per-item errors are caught so
one bad record yields a partial_failed batch instead of killing the run."""
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor

from . import db
from .core import utcnow, sha256_text
from .scoring import score_item


class Runner:
    def __init__(self, db_path, data_dir, workers=4, item_delay=0.02):
        self.db_path = db_path
        self.data_dir = data_dir
        self.item_delay = item_delay
        self.pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="batch")

    def submit(self, batch_id):
        self.pool.submit(self._run, batch_id)

    def shutdown(self):
        self.pool.shutdown(wait=True)

    def _run(self, batch_id):
        conn = db.connect(self.db_path)
        try:
            batch = conn.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
            if not batch or batch["status"] not in ("pending",):
                return
            conn.execute("UPDATE batches SET status='running', started_at=? WHERE id=?",
                         (utcnow(), batch_id))
            conn.commit()

            snap = conn.execute("SELECT * FROM feature_snapshots WHERE id=?",
                                (batch["snapshot_id"],)).fetchone()
            model = json.loads(conn.execute(
                "SELECT artifact_json FROM models WHERE id=?",
                (batch["model_id"],)).fetchone()["artifact_json"])
            params = json.loads(batch["params_json"])

            items, succeeded, failed = [], 0, 0
            with open(snap["path"], encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    rec = json.loads(line)
                    key, feats = str(rec["item_key"]), rec["features"]
                    if self.item_delay:
                        time.sleep(self.item_delay)  # make parallelism observable
                    try:
                        out = score_item(feats, model, params)
                        items.append((batch_id, key, json.dumps(feats, ensure_ascii=False),
                                      "success", out["score"], out["label"], None))
                        succeeded += 1
                    except Exception as e:  # per-item failure -> partial failure
                        items.append((batch_id, key, json.dumps(feats, ensure_ascii=False),
                                      "failed", None, None, str(e)))
                        failed += 1

            conn.executemany(
                "INSERT INTO batch_items(batch_id, item_key, features_json, status,"
                " score, label, error) VALUES(?,?,?,?,?,?,?)", items)

            # Materialize the result file; result rows are versioned and never
            # deleted, so old outputs stay downloadable after re-runs/rollbacks.
            version = (conn.execute(
                "SELECT COALESCE(MAX(version),0)+1 AS v FROM results WHERE batch_id=?",
                (batch_id,)).fetchone()["v"])
            lines = []
            for _, key, _, st, score, label, error in items:
                row = {"item_key": key, "status": st}
                if st == "success":
                    row["score"] = score
                    if label is not None:
                        row["label"] = label
                else:
                    row["error"] = error
                lines.append(json.dumps(row, ensure_ascii=False, sort_keys=True))
            content = "\n".join(lines) + "\n"
            path = os.path.join(self.data_dir, "results",
                                f"batch_{batch_id}_v{version}.jsonl")
            with open(path, "w", encoding="utf-8") as f:
                f.write(content)
            conn.execute(
                "INSERT INTO results(batch_id, version, path, content_hash, n_rows, created_at)"
                " VALUES(?,?,?,?,?,?)",
                (batch_id, version, path, sha256_text(content), len(lines), utcnow()))

            if failed == 0:
                status = "success"
            elif succeeded == 0:
                status = "failed"
            else:
                status = "partial_failed"
            conn.execute(
                "UPDATE batches SET status=?, total_items=?, succeeded_items=?,"
                " failed_items=?, finished_at=? WHERE id=?",
                (status, len(items), succeeded, failed, utcnow(), batch_id))
            conn.commit()
        except Exception as e:  # infrastructure-level failure -> whole batch failed
            conn.execute(
                "UPDATE batches SET status='failed', error=?, finished_at=? WHERE id=?",
                (f"{type(e).__name__}: {e}", utcnow(), batch_id))
            conn.commit()
        finally:
            conn.close()
