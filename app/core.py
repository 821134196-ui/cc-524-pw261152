"""Business logic: asset registration, batch submission (idempotent), re-runs,
publish/withdraw, lineage and diff. All lineage objects are content-hashed so a
batch pins exactly the data / features / model / params it ran with."""
import hashlib
import json
import os
import re
from datetime import datetime, timezone

from . import db


class ConflictError(Exception):
    """409-style error: resource already exists with different content, or a
    state transition is not allowed."""


class NotFoundError(Exception):
    pass


class ValidationError(Exception):
    pass


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sha256_text(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def canonical(obj):
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _safe(name):
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", name)


class Core:
    def __init__(self, db_path, data_dir):
        self.db_path = db_path
        self.data_dir = data_dir
        for sub in ("datasets", "snapshots", "results"):
            os.makedirs(os.path.join(data_dir, sub), exist_ok=True)
        db.init_db(db_path)

    def connect(self):
        return db.connect(self.db_path)

    # ---------------------------------------------------------------- assets
    def register_dataset(self, name, version, content):
        """content: JSONL text, one record per line, each with an item_key."""
        if not name or not version:
            raise ValidationError("dataset name and version are required")
        records = []
        for i, line in enumerate(content.splitlines(), 1):
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError as e:
                raise ValidationError(f"line {i}: invalid JSON: {e}")
            if not isinstance(rec, dict) or "item_key" not in rec:
                raise ValidationError(f"line {i}: record must be an object with 'item_key'")
            rec["item_key"] = str(rec["item_key"])
            records.append(rec)
        if not records:
            raise ValidationError("dataset has no records")
        keys = [r["item_key"] for r in records]
        if len(keys) != len(set(keys)):
            raise ValidationError("duplicate item_key in dataset")

        digest = sha256_text(content)
        path = os.path.join(self.data_dir, "datasets", f"{_safe(name)}@{_safe(version)}.jsonl")
        with open(path, "w", encoding="utf-8") as f:
            f.write(content if content.endswith("\n") else content + "\n")

        conn = self.connect()
        try:
            existing = conn.execute(
                "SELECT * FROM datasets WHERE name=? AND version=?", (name, version)
            ).fetchone()
            if existing:
                if existing["content_hash"] != digest:
                    raise ConflictError(
                        f"dataset {name}@{version} already exists with different content")
                return existing["id"], False
            cur = conn.execute(
                "INSERT INTO datasets(name, version, content_hash, path, n_records, created_at)"
                " VALUES(?,?,?,?,?,?)",
                (name, version, digest, path, len(records), utcnow()))
            conn.commit()
            return cur.lastrowid, True
        finally:
            conn.close()

    def build_snapshot(self, dataset_id, name, version, fields):
        """Materialize a feature snapshot: freeze the feature values extracted
        from a dataset so re-runs always see identical inputs."""
        if not fields:
            raise ValidationError("snapshot requires at least one feature field")
        conn = self.connect()
        try:
            ds = conn.execute("SELECT * FROM datasets WHERE id=?", (dataset_id,)).fetchone()
            if not ds:
                raise NotFoundError(f"dataset {dataset_id} not found")
            feature_config = {"fields": list(fields)}
            lines = []
            with open(ds["path"], encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    rec = json.loads(line)
                    feats = {k: rec.get(k) for k in fields}  # missing -> None (scoring will fail the item)
                    lines.append(json.dumps(
                        {"item_key": str(rec["item_key"]), "features": feats},
                        ensure_ascii=False, sort_keys=True))
            content = "\n".join(lines) + "\n"
            digest = sha256_text(content)
            path = os.path.join(self.data_dir, "snapshots",
                                f"{_safe(name)}@{_safe(version)}.jsonl")
            with open(path, "w", encoding="utf-8") as f:
                f.write(content)

            existing = conn.execute(
                "SELECT * FROM feature_snapshots WHERE name=? AND version=?",
                (name, version)).fetchone()
            if existing:
                if existing["content_hash"] != digest:
                    raise ConflictError(
                        f"snapshot {name}@{version} already exists with different content")
                return existing["id"], False
            cur = conn.execute(
                "INSERT INTO feature_snapshots(dataset_id, name, version, feature_config_json,"
                " content_hash, path, n_records, created_at) VALUES(?,?,?,?,?,?,?,?)",
                (dataset_id, name, version, canonical(feature_config), digest, path,
                 len(lines), utcnow()))
            conn.commit()
            return cur.lastrowid, True
        finally:
            conn.close()

    def register_model(self, name, version, artifact):
        try:
            weights = artifact["weights"]
            bias = float(artifact.get("bias", 0.0))
            weights = {str(k): float(v) for k, v in weights.items()}
        except (KeyError, TypeError, ValueError) as e:
            raise ValidationError(f"model artifact must have numeric weights/bias: {e}")
        artifact = {"weights": weights, "bias": bias,
                    "output": artifact.get("output", "sigmoid")}
        if artifact["output"] not in ("sigmoid", "linear"):
            raise ValidationError("model output must be 'sigmoid' or 'linear'")
        digest = sha256_text(canonical(artifact))
        conn = self.connect()
        try:
            existing = conn.execute(
                "SELECT * FROM models WHERE name=? AND version=?", (name, version)).fetchone()
            if existing:
                if existing["content_hash"] != digest:
                    raise ConflictError(
                        f"model {name}@{version} already exists with different content")
                return existing["id"], False
            cur = conn.execute(
                "INSERT INTO models(name, version, artifact_json, content_hash, status, created_at)"
                " VALUES(?,?,?,?,'active',?)",
                (name, version, canonical(artifact), digest, utcnow()))
            conn.commit()
            return cur.lastrowid, True
        finally:
            conn.close()

    def rollback_model(self, model_id):
        conn = self.connect()
        try:
            row = conn.execute("SELECT * FROM models WHERE id=?", (model_id,)).fetchone()
            if not row:
                raise NotFoundError(f"model {model_id} not found")
            conn.execute("UPDATE models SET status='rolled_back' WHERE id=?", (model_id,))
            conn.commit()
            return True
        finally:
            conn.close()

    # ---------------------------------------------------------------- batches
    def submit_batch(self, batch_key, snapshot_id, model_id, params=None, note=None):
        """Idempotent: an existing batch_key returns the existing batch, no new run."""
        if not batch_key:
            raise ValidationError("batch_key is required")
        params = params or {}
        if not isinstance(params, dict):
            raise ValidationError("params must be an object")
        conn = self.connect()
        try:
            snap = conn.execute("SELECT * FROM feature_snapshots WHERE id=?",
                                (snapshot_id,)).fetchone()
            if not snap:
                raise NotFoundError(f"snapshot {snapshot_id} not found")
            model = conn.execute("SELECT * FROM models WHERE id=?", (model_id,)).fetchone()
            if not model:
                raise NotFoundError(f"model {model_id} not found")
            try:
                cur = conn.execute(
                    "INSERT INTO batches(batch_key, dataset_id, snapshot_id, model_id,"
                    " params_json, params_hash, status, note, created_at)"
                    " VALUES(?,?,?,?,?,?, 'pending', ?,?)",
                    (batch_key, snap["dataset_id"], snapshot_id, model_id,
                     canonical(params), sha256_text(canonical(params)), note, utcnow()))
                conn.commit()
                return cur.lastrowid, True
            except Exception as e:
                if "UNIQUE" not in str(e):
                    raise
                existing = conn.execute(
                    "SELECT id FROM batches WHERE batch_key=?", (batch_key,)).fetchone()
                return existing["id"], False
        finally:
            conn.close()

    def rerun_batch(self, batch_id, snapshot_id=None, model_id=None, params=None, note=None):
        """Create a NEW batch linked to the original via rerun_of. The original
        batch and its results stay untouched and downloadable."""
        conn = self.connect()
        try:
            parent = conn.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
            if not parent:
                raise NotFoundError(f"batch {batch_id} not found")
            snapshot_id = snapshot_id or parent["snapshot_id"]
            model_id = model_id or parent["model_id"]
            if params is None:
                params = json.loads(parent["params_json"])
        finally:
            conn.close()
        run_index = parent["run_index"] + 1
        base_key = f"{parent['batch_key']}#r{run_index}"
        key, n = base_key, 1
        conn = self.connect()
        try:
            while conn.execute("SELECT 1 FROM batches WHERE batch_key=?", (key,)).fetchone():
                n += 1
                key = f"{base_key}.{n}"
        finally:
            conn.close()
        new_note = note or f"rerun of batch {batch_id}"
        new_id, _ = self.submit_batch(key, snapshot_id, model_id, params, note=new_note)
        conn = self.connect()
        try:
            conn.execute("UPDATE batches SET rerun_of=?, run_index=? WHERE id=?",
                         (batch_id, run_index, new_id))
            conn.commit()
        finally:
            conn.close()
        return new_id

    # ------------------------------------------------------------- publishing
    def publish(self, batch_id):
        """Idempotent: an already-published batch returns its active publication.
        The unique partial index makes a second active publication impossible."""
        conn = self.connect()
        try:
            batch = conn.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
            if not batch:
                raise NotFoundError(f"batch {batch_id} not found")
            if batch["status"] not in ("success", "partial_failed"):
                raise ConflictError(
                    f"cannot publish batch in status '{batch['status']}'"
                    " (must be success or partial_failed)")
            active = conn.execute(
                "SELECT * FROM publications WHERE batch_id=? AND status='published'",
                (batch_id,)).fetchone()
            if active:
                return active["id"], False
            result = conn.execute(
                "SELECT * FROM results WHERE batch_id=? ORDER BY version DESC LIMIT 1",
                (batch_id,)).fetchone()
            if not result:
                raise ConflictError("batch has no result to publish")
            try:
                cur = conn.execute(
                    "INSERT INTO publications(batch_id, result_id, status, published_at)"
                    " VALUES(?,?, 'published', ?)", (batch_id, result["id"], utcnow()))
                conn.commit()
                return cur.lastrowid, True
            except Exception as e:
                if "UNIQUE" not in str(e):
                    raise
                active = conn.execute(
                    "SELECT * FROM publications WHERE batch_id=? AND status='published'",
                    (batch_id,)).fetchone()
                return active["id"], False
        finally:
            conn.close()

    def withdraw(self, batch_id):
        conn = self.connect()
        try:
            active = conn.execute(
                "SELECT * FROM publications WHERE batch_id=? AND status='published'",
                (batch_id,)).fetchone()
            if not active:
                raise ConflictError(f"batch {batch_id} has no active publication")
            conn.execute(
                "UPDATE publications SET status='withdrawn', withdrawn_at=? WHERE id=?",
                (utcnow(), active["id"]))
            conn.commit()
            return active["id"]
        finally:
            conn.close()

    # ---------------------------------------------------------------- queries
    def get_batch(self, batch_id):
        conn = self.connect()
        try:
            row = conn.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def list_batches(self):
        conn = self.connect()
        try:
            rows = conn.execute(
                "SELECT b.*, m.name AS model_name, m.version AS model_version,"
                " s.name AS snapshot_name, s.version AS snapshot_version,"
                " (SELECT COUNT(*) FROM publications p WHERE p.batch_id=b.id"
                "   AND p.status='published') AS published"
                " FROM batches b"
                " JOIN models m ON m.id=b.model_id"
                " JOIN feature_snapshots s ON s.id=b.snapshot_id"
                " ORDER BY b.id DESC").fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def get_lineage(self, batch_id):
        """Full traceable chain: dataset -> snapshot -> model -> params -> results
        -> publications, each with its content hash."""
        conn = self.connect()
        try:
            b = conn.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
            if not b:
                raise NotFoundError(f"batch {batch_id} not found")
            ds = conn.execute("SELECT * FROM datasets WHERE id=?", (b["dataset_id"],)).fetchone()
            snap = conn.execute("SELECT * FROM feature_snapshots WHERE id=?",
                                (b["snapshot_id"],)).fetchone()
            model = conn.execute("SELECT * FROM models WHERE id=?", (b["model_id"],)).fetchone()
            results = conn.execute(
                "SELECT * FROM results WHERE batch_id=? ORDER BY version", (batch_id,)).fetchall()
            pubs = conn.execute(
                "SELECT * FROM publications WHERE batch_id=? ORDER BY id", (batch_id,)).fetchall()
            reruns = conn.execute(
                "SELECT id, batch_key, status, run_index FROM batches WHERE rerun_of=?"
                " ORDER BY id", (batch_id,)).fetchall()
            return {
                "batch": dict(b),
                "dataset": dict(ds),
                "snapshot": dict(snap),
                "model": dict(model),
                "params": json.loads(b["params_json"]),
                "results": [dict(r) for r in results],
                "publications": [dict(p) for p in pubs],
                "reruns": [dict(r) for r in reruns],
            }
        finally:
            conn.close()

    def list_items(self, batch_id):
        conn = self.connect()
        try:
            rows = conn.execute(
                "SELECT * FROM batch_items WHERE batch_id=? ORDER BY item_key",
                (batch_id,)).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def diff_batches(self, a_id, b_id):
        """Item-level diff between two batch runs plus a lineage diff showing
        which inputs (dataset/snapshot/model/params) changed."""
        la, lb = self.get_lineage(a_id), self.get_lineage(b_id)
        ia = {i["item_key"]: i for i in self.list_items(a_id)}
        ib = {i["item_key"]: i for i in self.list_items(b_id)}
        added, removed, changed = [], [], []
        unchanged = 0
        for k in sorted(ib.keys() - ia.keys()):
            added.append(ib[k])
        for k in sorted(ia.keys() - ib.keys()):
            removed.append(ia[k])
        for k in sorted(ia.keys() & ib.keys()):
            x, y = ia[k], ib[k]
            score_diff = (x["score"] is None) != (y["score"] is None) or (
                x["score"] is not None and y["score"] is not None
                and abs(x["score"] - y["score"]) > 1e-9)
            if score_diff or x["status"] != y["status"] or x["label"] != y["label"]:
                changed.append({"item_key": k, "a": x, "b": y,
                                "delta": None if x["score"] is None or y["score"] is None
                                        else round(y["score"] - x["score"], 6)})
            else:
                unchanged += 1
        lineage_diff = []
        for part in ("dataset", "snapshot", "model"):
            xa, xb = la[part], lb[part]
            lineage_diff.append({
                "part": part,
                "a": f"{xa['name']}@{xa['version']}",
                "b": f"{xb['name']}@{xb['version']}",
                "changed": xa["content_hash"] != xb["content_hash"],
            })
        lineage_diff.append({
            "part": "params",
            "a": la["batch"]["params_json"],
            "b": lb["batch"]["params_json"],
            "changed": la["batch"]["params_hash"] != lb["batch"]["params_hash"],
        })
        return {
            "a": la["batch"], "b": lb["batch"],
            "summary": {"added": len(added), "removed": len(removed),
                        "changed": len(changed), "unchanged": unchanged},
            "added": added, "removed": removed, "changed": changed,
            "lineage_diff": lineage_diff,
        }

    # ------------------------------------------------------------------ lists
    def list_assets(self):
        conn = self.connect()
        try:
            datasets = [dict(r) for r in conn.execute(
                "SELECT * FROM datasets ORDER BY id DESC").fetchall()]
            snapshots = [dict(r) for r in conn.execute(
                "SELECT s.*, d.name AS dataset_name, d.version AS dataset_version"
                " FROM feature_snapshots s JOIN datasets d ON d.id=s.dataset_id"
                " ORDER BY s.id DESC").fetchall()]
            models = [dict(r) for r in conn.execute(
                "SELECT * FROM models ORDER BY id DESC").fetchall()]
            return {"datasets": datasets, "snapshots": snapshots, "models": models}
        finally:
            conn.close()

    def get_result(self, result_id):
        conn = self.connect()
        try:
            row = conn.execute("SELECT * FROM results WHERE id=?", (result_id,)).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()
