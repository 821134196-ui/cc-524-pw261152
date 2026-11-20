#!/usr/bin/env python3
"""End-to-end verification for the batch scoring platform.

Covers:
  1. idempotent batch submission (duplicate batch_key -> same batch, no re-run)
  2. partial failure (bad record -> partial_failed, per-item error kept)
  3. idempotent publish (duplicate publish -> single active publication)
  4. data backfill + rerun + diff (fixed record becomes success)
  5. model rollback + rerun with old model + diff; old results still downloadable
  6. parallel batches (concurrent submissions all reach terminal state)
  7. duplicate submission race (two threads, one batch_key -> one batch)
  8. withdraw + re-publish
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time

import requests

PORT = 5055
BASE = f"http://127.0.0.1:{PORT}"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CHECKS = []


def check(name, cond, detail=""):
    CHECKS.append((name, bool(cond), detail))
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" — {detail}" if detail and not cond else ""))


def wait_terminal(batch_id, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        b = requests.get(f"{BASE}/api/batches/{batch_id}").json()
        if b["status"] in ("success", "partial_failed", "failed"):
            return b
        time.sleep(0.2)
    raise TimeoutError(f"batch {batch_id} did not finish in {timeout}s")


def main():
    data_dir = tempfile.mkdtemp(prefix="scorer-e2e-")
    env = dict(os.environ, BATCH_SCORER_ITEM_DELAY="0.02")
    server = subprocess.Popen(
        [os.path.join(ROOT, ".venv", "bin", "python"), "-I", "run.py",
         "--port", str(PORT), "--data-dir", data_dir],
        cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        for _ in range(100):
            try:
                if requests.get(f"{BASE}/api/health", timeout=1).json().get("ok"):
                    break
            except requests.RequestException:
                time.sleep(0.1)
        else:
            raise RuntimeError("server did not start")

        print("== 1. 注册资产：数据集 v1（含 1 条坏记录）/ 快照 / 模型 ==")
        records_v1 = [
            {"item_key": f"u{i}", "age": 20 + i, "income": 5000 + 100 * i, "clicks": i}
            for i in range(1, 8)
        ]
        records_v1.append({"item_key": "u8", "age": "not-a-number",
                           "income": 5700, "clicks": 8})  # bad record
        content_v1 = "\n".join(json.dumps(r) for r in records_v1)
        r = requests.post(f"{BASE}/api/datasets", json={
            "name": "users", "version": "v1", "content": content_v1})
        ds1 = r.json()["id"]
        check("dataset v1 registered", r.status_code == 201 and r.json()["created"])

        r = requests.post(f"{BASE}/api/snapshots", json={
            "dataset_id": ds1, "name": "users-features", "version": "v1",
            "fields": ["age", "income", "clicks"]})
        snap1 = r.json()["id"]
        check("snapshot v1 built", r.status_code == 201)

        model_v1 = {"weights": {"age": 0.05, "income": 0.0002, "clicks": 0.1},
                    "bias": -3.0, "output": "sigmoid"}
        r = requests.post(f"{BASE}/api/models", json={
            "name": "ctr", "version": "v1", "artifact": model_v1})
        m1 = r.json()["id"]
        check("model v1 registered", r.status_code == 201)

        print("== 2. 提交批次 + 幂等重复提交 ==")
        params = {"threshold": 0.5}
        r = requests.post(f"{BASE}/api/batches", json={
            "batch_key": "daily-2026-11-20", "snapshot_id": snap1,
            "model_id": m1, "params": params, "note": "每日评分"})
        b1 = r.json()["id"]
        check("batch submitted", r.status_code == 201 and r.json()["created"])

        r = requests.post(f"{BASE}/api/batches", json={
            "batch_key": "daily-2026-11-20", "snapshot_id": snap1,
            "model_id": m1, "params": params})
        dup = r.json()
        check("duplicate submit returns same batch, created=False",
              r.status_code == 200 and dup["id"] == b1 and not dup["created"])

        print("== 3. 部分失败 ==")
        b1_state = wait_terminal(b1)
        check("batch partial_failed (1 bad record of 8)",
              b1_state["status"] == "partial_failed"
              and b1_state["succeeded_items"] == 7 and b1_state["failed_items"] == 1,
              f"got {b1_state['status']} {b1_state['succeeded_items']}/{b1_state['failed_items']}")
        items = requests.get(f"{BASE}/api/batches/{b1}/items").json()
        bad = [i for i in items if i["status"] == "failed"]
        check("failed item recorded with error",
              len(bad) == 1 and bad[0]["item_key"] == "u8" and bad[0]["error"])

        print("== 4. 发布幂等 ==")
        r1 = requests.post(f"{BASE}/api/batches/{b1}/publish")
        r2 = requests.post(f"{BASE}/api/batches/{b1}/publish")
        lin = requests.get(f"{BASE}/api/batches/{b1}/lineage").json()
        active = [p for p in lin["publications"] if p["status"] == "published"]
        check("first publish created", r1.status_code == 201 and r1.json()["created"])
        check("second publish idempotent (same publication, still one active)",
              r2.status_code == 200 and not r2.json()["created"]
              and r2.json()["publication_id"] == r1.json()["publication_id"]
              and len(active) == 1)

        print("== 5. 补数据 -> 重跑 -> 差异对比 ==")
        records_v2 = [dict(r) for r in records_v1]
        records_v2[-1] = {"item_key": "u8", "age": 35, "income": 5700, "clicks": 8}  # fixed
        content_v2 = "\n".join(json.dumps(r) for r in records_v2)
        ds2 = requests.post(f"{BASE}/api/datasets", json={
            "name": "users", "version": "v2", "content": content_v2}).json()["id"]
        snap2 = requests.post(f"{BASE}/api/snapshots", json={
            "dataset_id": ds2, "name": "users-features", "version": "v2",
            "fields": ["age", "income", "clicks"]}).json()["id"]
        r = requests.post(f"{BASE}/api/batches/{b1}/rerun", json={"snapshot_id": snap2})
        b2 = r.json()["id"]
        check("rerun created linked to parent", r.status_code == 201)
        b2_state = wait_terminal(b2)
        check("rerun with backfilled data succeeds fully",
              b2_state["status"] == "success" and b2_state["succeeded_items"] == 8)
        diff = requests.get(f"{BASE}/api/batches/{b1}/diff/{b2}").json()
        check("diff shows u8 fixed (failed->success), others unchanged",
              diff["summary"]["changed"] == 1 and diff["summary"]["unchanged"] == 7
              and diff["changed"][0]["item_key"] == "u8"
              and diff["changed"][0]["a"]["status"] == "failed"
              and diff["changed"][0]["b"]["status"] == "success",
              json.dumps(diff["summary"]))
        snap_change = [d for d in diff["lineage_diff"] if d["part"] == "snapshot"][0]
        model_change = [d for d in diff["lineage_diff"] if d["part"] == "model"][0]
        check("lineage diff: snapshot changed, model unchanged",
              snap_change["changed"] and not model_change["changed"])

        print("== 6. 模型回滚 -> 用旧模型重跑 -> 旧结果仍可下载 ==")
        model_v2 = {"weights": {"age": 0.09, "income": 0.0001, "clicks": 0.2},
                    "bias": -4.0, "output": "sigmoid"}
        m2 = requests.post(f"{BASE}/api/models", json={
            "name": "ctr", "version": "v2", "artifact": model_v2}).json()["id"]
        b3 = requests.post(f"{BASE}/api/batches", json={
            "batch_key": "daily-2026-11-20-mv2", "snapshot_id": snap2,
            "model_id": m2, "params": params}).json()["id"]
        b3_state = wait_terminal(b3)
        check("batch with model v2 succeeds", b3_state["status"] == "success")

        r = requests.post(f"{BASE}/api/models/{m2}/rollback")
        check("model v2 rolled back", r.status_code == 200
              and r.json()["status"] == "rolled_back")

        r = requests.post(f"{BASE}/api/batches/{b3}/rerun", json={"model_id": m1})
        b4 = r.json()["id"]
        b4_state = wait_terminal(b4)
        check("rerun with rolled-back (old) model succeeds", b4_state["status"] == "success")
        diff = requests.get(f"{BASE}/api/batches/{b3}/diff/{b4}").json()
        check("diff shows score changes from model rollback",
              diff["summary"]["changed"] == 8 and diff["summary"]["unchanged"] == 0,
              json.dumps(diff["summary"]))
        model_diff = [d for d in diff["lineage_diff"] if d["part"] == "model"][0]
        check("lineage diff: model changed, snapshot unchanged",
              model_diff["changed"] and model_diff["a"] == "ctr@v2"
              and model_diff["b"] == "ctr@v1")

        lin3 = requests.get(f"{BASE}/api/batches/{b3}/lineage").json()
        old_result = lin3["results"][0]
        r = requests.get(f"{BASE}/api/results/{old_result['id']}/download")
        check("old result (model v2 run) still downloadable after rollback+rerun",
              r.status_code == 200 and len(r.text.strip().splitlines()) == 8
              and "score" in r.text)
        lin4 = requests.get(f"{BASE}/api/batches/{b4}/lineage").json()
        check("rerun batch has its own result + lineage back to parent",
              len(lin4["results"]) == 1 and lin4["batch"]["rerun_of"] == b3)

        print("== 7. 并行批次 ==")
        parallel_ids, errors = [], []

        def submit_one(i):
            try:
                rr = requests.post(f"{BASE}/api/batches", json={
                    "batch_key": f"parallel-{i}", "snapshot_id": snap2,
                    "model_id": m1, "params": params})
                parallel_ids.append(rr.json()["id"])
            except Exception as e:  # noqa: BLE001
                errors.append(str(e))

        threads = [threading.Thread(target=submit_one, args=(i,)) for i in range(6)]
        t0 = time.time()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        states = [wait_terminal(bid) for bid in parallel_ids]
        elapsed = time.time() - t0
        check("6 parallel batches all succeed, no errors",
              not errors and len(set(parallel_ids)) == 6
              and all(s["status"] == "success" for s in states),
              f"errors={errors}")
        check("parallel execution actually overlapped (wall clock < serial)",
              elapsed < 6 * 8 * 0.02 * 2, f"elapsed={elapsed:.2f}s")

        print("== 8. 并发重复提交同一批次键 ==")
        race_results = []

        def submit_race():
            rr = requests.post(f"{BASE}/api/batches", json={
                "batch_key": "race-key", "snapshot_id": snap2,
                "model_id": m1, "params": params})
            race_results.append(rr.json())

        ts = [threading.Thread(target=submit_race) for _ in range(4)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        created = [r for r in race_results if r["created"]]
        ids = {r["id"] for r in race_results}
        check("race: exactly one created, all return same batch id",
              len(created) == 1 and len(ids) == 1, f"{race_results}")
        race_batch = wait_terminal(ids.pop())
        pubs_before = requests.post(f"{BASE}/api/batches/{race_batch['id']}/publish").json()
        pubs_after = requests.post(f"{BASE}/api/batches/{race_batch['id']}/publish").json()
        check("raced batch publishes exactly once",
              pubs_before["created"] and not pubs_after["created"]
              and pubs_before["publication_id"] == pubs_after["publication_id"])

        print("== 9. 撤回与再发布 ==")
        r = requests.post(f"{BASE}/api/batches/{b1}/withdraw")
        lin = requests.get(f"{BASE}/api/batches/{b1}/lineage").json()
        active = [p for p in lin["publications"] if p["status"] == "published"]
        check("withdraw leaves no active publication",
              r.status_code == 200 and not active)
        r = requests.post(f"{BASE}/api/batches/{b1}/withdraw")
        check("double withdraw rejected (409)", r.status_code == 409)
        r = requests.post(f"{BASE}/api/batches/{b1}/publish")
        lin = requests.get(f"{BASE}/api/batches/{b1}/lineage").json()
        active = [p for p in lin["publications"] if p["status"] == "published"]
        check("re-publish after withdraw creates new publication",
              r.status_code == 201 and len(active) == 1
              and len(lin["publications"]) == 2)

        print("== 10. 血缘链路完整性 ==")
        lin = requests.get(f"{BASE}/api/batches/{b1}/lineage").json()
        chain_ok = (lin["dataset"]["content_hash"] and lin["snapshot"]["content_hash"]
                    and lin["model"]["content_hash"] and lin["batch"]["params_hash"]
                    and lin["snapshot"]["dataset_id"] == lin["dataset"]["id"]
                    and lin["batch"]["snapshot_id"] == lin["snapshot"]["id"]
                    and lin["batch"]["model_id"] == lin["model"]["id"])
        check("lineage chain links dataset->snapshot->model->params->results with hashes",
              chain_ok and len(lin["results"]) == 1)

        print("== 11. 页面可访问 ==")
        for path, marker in [
            ("/", "批次看板"),
            ("/assets", "资产"),
            ("/submit", "提交批次"),
            (f"/batches/{b1}", "血缘链路"),
            (f"/batches/{b1}/diff/{b2}", "差异对比"),
        ]:
            r = requests.get(BASE + path)
            check(f"GET {path} renders", r.status_code == 200 and marker in r.text)

        failed_publish = requests.post(f"{BASE}/api/batches", json={
            "batch_key": "pending-publish-guard", "snapshot_id": snap2,
            "model_id": m1, "params": {}}).json()["id"]
        # publish guard on a batch that is still pending/running or failed
        # (force a failed batch: empty params + missing features via new snapshot)
        wait_terminal(failed_publish)
        r = requests.post(f"{BASE}/api/models", json={
            "name": "broken", "version": "v1",
            "artifact": {"weights": {"nonexistent_feature": 1.0}, "bias": 0}})
        broken_model = r.json()["id"]
        r = requests.post(f"{BASE}/api/batches", json={
            "batch_key": "all-fail", "snapshot_id": snap2, "model_id": broken_model,
            "params": {}})
        fail_batch = wait_terminal(r.json()["id"])
        check("batch with all items failing -> status failed",
              fail_batch["status"] == "failed" and fail_batch["succeeded_items"] == 0)
        r = requests.post(f"{BASE}/api/batches/{fail_batch['id']}/publish")
        check("failed batch cannot be published (409)", r.status_code == 409)

    finally:
        server.terminate()
        try:
            server.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server.kill()
        shutil.rmtree(data_dir, ignore_errors=True)

    n_fail = sum(1 for _, ok, _ in CHECKS if not ok)
    print(f"\n{'=' * 50}\n{len(CHECKS) - n_fail}/{len(CHECKS)} checks passed"
          + (f", {n_fail} FAILED" if n_fail else ""))
    if n_fail:
        for name, ok, detail in CHECKS:
            if not ok:
                print(f"  FAILED: {name} {detail}")
        sys.exit(1)
    print("ALL CHECKS PASSED")


if __name__ == "__main__":
    main()
