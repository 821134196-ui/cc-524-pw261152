#!/usr/bin/env python3
"""Seed demo data against a running server (default http://127.0.0.1:5000).

Usage: python scripts/seed.py [base_url]
Creates: dataset v1 (with a bad record) + backfilled v2, snapshots, two model
versions, an original batch (partial_failed), a backfill rerun, a model-rollback
rerun, and publishes one batch — so every page has something to show.
"""
import json
import sys

import requests

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:5000"


def post(path, payload):
    r = requests.post(BASE + path, json=payload, timeout=10)
    if r.status_code not in (200, 201):
        print(f"POST {path} -> {r.status_code}: {r.text}")
        sys.exit(1)
    return r.json()


def wait(batch_id):
    import time
    for _ in range(120):
        b = requests.get(f"{BASE}/api/batches/{batch_id}", timeout=5).json()
        if b["status"] in ("success", "partial_failed", "failed"):
            return b
        time.sleep(0.3)
    raise TimeoutError(batch_id)


def main():
    records = [{"item_key": f"u{i}", "age": 18 + 3 * i, "income": 4000 + 350 * i,
                "clicks": i % 5, "region_score": round(0.1 * i, 2)} for i in range(1, 13)]
    bad = [dict(r) for r in records]
    bad[3] = {"item_key": "u4", "age": "unknown", "income": 5050, "clicks": 1,
              "region_score": 0.4}
    fields = ["age", "income", "clicks", "region_score"]

    ds1 = post("/api/datasets", {"name": "users", "version": "v1",
                                 "content": "\n".join(json.dumps(r) for r in bad)})["id"]
    ds2 = post("/api/datasets", {"name": "users", "version": "v2-backfill",
                                 "content": "\n".join(json.dumps(r) for r in records)})["id"]
    s1 = post("/api/snapshots", {"dataset_id": ds1, "name": "user-features",
                                 "version": "v1", "fields": fields})["id"]
    s2 = post("/api/snapshots", {"dataset_id": ds2, "name": "user-features",
                                 "version": "v2-backfill", "fields": fields})["id"]
    m1 = post("/api/models", {"name": "ctr", "version": "v1", "artifact": {
        "weights": {"age": 0.04, "income": 0.0002, "clicks": 0.3, "region_score": 1.2},
        "bias": -3.5, "output": "sigmoid"}})["id"]
    m2 = post("/api/models", {"name": "ctr", "version": "v2", "artifact": {
        "weights": {"age": 0.07, "income": 0.0001, "clicks": 0.5, "region_score": 0.8},
        "bias": -4.2, "output": "sigmoid"}})["id"]

    params = {"threshold": 0.5}
    b1 = post("/api/batches", {"batch_key": "daily-2026-11-19", "snapshot_id": s1,
                               "model_id": m1, "params": params,
                               "note": "每日评分（含坏数据，演示部分失败）"})["id"]
    wait(b1)
    post(f"/api/batches/{b1}/publish", {})

    b2 = post(f"/api/batches/{b1}/rerun", {"snapshot_id": s2,
                                           "note": "补数据后重跑"})["id"]
    wait(b2)

    b3 = post("/api/batches", {"batch_key": "daily-2026-11-20", "snapshot_id": s2,
                               "model_id": m2, "params": params,
                               "note": "模型 v2 上线"})["id"]
    wait(b3)
    post(f"/api/batches/{b3}/publish", {})
    post(f"/api/models/{m2}/rollback", {})
    b4 = post(f"/api/batches/{b3}/rerun", {"model_id": m1,
                                           "note": "模型 v2 回滚，用 v1 重跑"})["id"]
    wait(b4)
    post(f"/api/batches/{b3}/withdraw", {})
    post(f"/api/batches/{b4}/publish", {})

    print(f"seeded: batches b1={b1} b2={b2} b3={b3} b4={b4}")
    print(f"看板: {BASE}/  对比示例: {BASE}/batches/{b3}/diff/{b4}")


if __name__ == "__main__":
    main()
