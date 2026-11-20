"""HTTP layer: JSON API + server-rendered pages."""
import json
import os

from flask import (Flask, abort, jsonify, redirect, render_template, request,
                   send_file, url_for)

from .core import Core, ConflictError, NotFoundError, ValidationError
from .runner import Runner


def create_app(data_dir=None, workers=4, item_delay=None):
    data_dir = data_dir or os.environ.get("BATCH_SCORER_DATA", "data")
    data_dir = os.path.abspath(data_dir)
    db_path = os.path.join(data_dir, "app.db")
    core = Core(db_path, data_dir)
    runner = Runner(db_path, data_dir, workers=workers,
                    item_delay=float(os.environ.get(
                        "BATCH_SCORER_ITEM_DELAY",
                        item_delay if item_delay is not None else 0.02)))

    app = Flask(__name__)
    app.config["CORE"] = core
    app.config["RUNNER"] = runner

    # ------------------------------------------------------------ error handling
    @app.errorhandler(NotFoundError)
    def _nf(e):
        return _err(e, 404)

    @app.errorhandler(ConflictError)
    def _cf(e):
        return _err(e, 409)

    @app.errorhandler(ValidationError)
    def _ve(e):
        return _err(e, 400)

    def _err(e, code):
        if request.path.startswith("/api/"):
            return jsonify({"error": str(e)}), code
        return render_template("error.html", code=code, message=str(e)), code

    def _body():
        return request.get_json(force=True, silent=True) or {}

    # ---------------------------------------------------------------- JSON API
    @app.get("/api/health")
    def api_health():
        return jsonify({"ok": True})

    @app.post("/api/datasets")
    def api_register_dataset():
        if request.files.get("file"):
            content = request.files["file"].read().decode("utf-8")
            name = request.form.get("name")
            version = request.form.get("version")
        else:
            b = _body()
            content, name, version = b.get("content", ""), b.get("name"), b.get("version")
        did, created = core.register_dataset(name, version, content)
        return jsonify({"id": did, "created": created}), 201 if created else 200

    @app.post("/api/snapshots")
    def api_build_snapshot():
        b = _body() if not request.form else request.form
        fields = b.get("fields")
        if isinstance(fields, str):
            fields = [f.strip() for f in fields.split(",") if f.strip()]
        sid, created = core.build_snapshot(
            int(b.get("dataset_id")), b.get("name"), b.get("version"), fields)
        return jsonify({"id": sid, "created": created}), 201 if created else 200

    @app.post("/api/models")
    def api_register_model():
        b = _body()
        artifact = b.get("artifact")
        if isinstance(artifact, str):
            artifact = json.loads(artifact)
        mid, created = core.register_model(b.get("name"), b.get("version"), artifact)
        return jsonify({"id": mid, "created": created}), 201 if created else 200

    @app.post("/api/models/<int:model_id>/rollback")
    def api_rollback_model(model_id):
        core.rollback_model(model_id)
        return jsonify({"id": model_id, "status": "rolled_back"})

    @app.post("/api/batches")
    def api_submit_batch():
        b = _body()
        bid, created = core.submit_batch(
            b.get("batch_key"), int(b.get("snapshot_id")), int(b.get("model_id")),
            b.get("params") or {}, b.get("note"))
        if created:
            runner.submit(bid)
        return jsonify({"id": bid, "created": created}), 201 if created else 200

    @app.get("/api/batches")
    def api_list_batches():
        return jsonify(core.list_batches())

    @app.get("/api/batches/<int:batch_id>")
    def api_get_batch(batch_id):
        batch = core.get_batch(batch_id)
        if not batch:
            raise NotFoundError(f"batch {batch_id} not found")
        return jsonify(batch)

    @app.get("/api/batches/<int:batch_id>/lineage")
    def api_lineage(batch_id):
        return jsonify(core.get_lineage(batch_id))

    @app.get("/api/batches/<int:batch_id>/items")
    def api_items(batch_id):
        return jsonify(core.list_items(batch_id))

    @app.post("/api/batches/<int:batch_id>/rerun")
    def api_rerun(batch_id):
        b = _body()
        new_id = core.rerun_batch(
            batch_id,
            snapshot_id=int(b["snapshot_id"]) if b.get("snapshot_id") else None,
            model_id=int(b["model_id"]) if b.get("model_id") else None,
            params=b.get("params"),
            note=b.get("note"))
        runner.submit(new_id)
        return jsonify({"id": new_id, "rerun_of": batch_id}), 201

    @app.get("/api/batches/<int:a_id>/diff/<int:b_id>")
    def api_diff(a_id, b_id):
        return jsonify(core.diff_batches(a_id, b_id))

    @app.post("/api/batches/<int:batch_id>/publish")
    def api_publish(batch_id):
        pid, created = core.publish(batch_id)
        return jsonify({"publication_id": pid, "created": created}), 201 if created else 200

    @app.post("/api/batches/<int:batch_id>/withdraw")
    def api_withdraw(batch_id):
        pid = core.withdraw(batch_id)
        return jsonify({"publication_id": pid, "status": "withdrawn"})

    @app.get("/api/results/<int:result_id>/download")
    def api_download(result_id):
        result = core.get_result(result_id)
        if not result:
            raise NotFoundError(f"result {result_id} not found")
        return send_file(result["path"], as_attachment=True,
                         download_name=os.path.basename(result["path"]))

    # ------------------------------------------------------------------- pages
    @app.get("/")
    def page_dashboard():
        batches = core.list_batches()
        stats = {"total": len(batches), "running": 0, "success": 0,
                 "partial_failed": 0, "failed": 0, "published": 0}
        for b in batches:
            if b["status"] in ("pending", "running"):
                stats["running"] += 1
            elif b["status"] in stats:
                stats[b["status"]] += 1
            stats["published"] += 1 if b["published"] else 0
        active = any(b["status"] in ("pending", "running") for b in batches)
        return render_template("dashboard.html", batches=batches, stats=stats,
                               active=active)

    @app.get("/assets")
    def page_assets():
        return render_template("assets.html", **core.list_assets())

    @app.get("/submit")
    def page_submit():
        assets = core.list_assets()
        return render_template("submit.html", snapshots=assets["snapshots"],
                               models=assets["models"])

    @app.get("/batches/<int:batch_id>")
    def page_batch(batch_id):
        lineage = core.get_lineage(batch_id)
        items = core.list_items(batch_id)
        all_batches = core.list_batches()
        active_pub = next((p for p in lineage["publications"]
                           if p["status"] == "published"), None)
        assets = core.list_assets()
        return render_template(
            "batch_detail.html", lineage=lineage, items=items,
            active_pub=active_pub, all_batches=all_batches,
            snapshots=assets["snapshots"], models=assets["models"],
            running=lineage["batch"]["status"] in ("pending", "running"))

    @app.get("/batches/<int:a_id>/diff/<int:b_id>")
    def page_diff(a_id, b_id):
        diff = core.diff_batches(a_id, b_id)
        return render_template("diff.html", diff=diff)

    # ------------------------------------------------- form actions (from UI)
    @app.post("/ui/datasets")
    def ui_dataset():
        f = request.files.get("file")
        content = f.read().decode("utf-8") if f and f.filename else request.form.get("content", "")
        core.register_dataset(request.form.get("name"), request.form.get("version"), content)
        return redirect(url_for("page_assets"))

    @app.post("/ui/snapshots")
    def ui_snapshot():
        fields = [f.strip() for f in request.form.get("fields", "").split(",") if f.strip()]
        core.build_snapshot(int(request.form.get("dataset_id")),
                            request.form.get("name"), request.form.get("version"), fields)
        return redirect(url_for("page_assets"))

    @app.post("/ui/models")
    def ui_model():
        core.register_model(request.form.get("name"), request.form.get("version"),
                            json.loads(request.form.get("artifact")))
        return redirect(url_for("page_assets"))

    @app.post("/ui/models/<int:model_id>/rollback")
    def ui_rollback(model_id):
        core.rollback_model(model_id)
        return redirect(url_for("page_assets"))

    @app.post("/ui/batches")
    def ui_submit_batch():
        params_text = request.form.get("params", "").strip()
        params = json.loads(params_text) if params_text else {}
        bid, created = core.submit_batch(
            request.form.get("batch_key"), int(request.form.get("snapshot_id")),
            int(request.form.get("model_id")), params, request.form.get("note"))
        if created:
            runner.submit(bid)
        return redirect(url_for("page_batch", batch_id=bid))

    @app.post("/ui/batches/<int:batch_id>/rerun")
    def ui_rerun(batch_id):
        params_text = request.form.get("params", "").strip()
        params = json.loads(params_text) if params_text else None
        new_id = core.rerun_batch(
            batch_id,
            snapshot_id=int(request.form["snapshot_id"]) if request.form.get("snapshot_id") else None,
            model_id=int(request.form["model_id"]) if request.form.get("model_id") else None,
            params=params)
        runner.submit(new_id)
        return redirect(url_for("page_batch", batch_id=new_id))

    @app.post("/ui/batches/<int:batch_id>/publish")
    def ui_publish(batch_id):
        core.publish(batch_id)
        return redirect(url_for("page_batch", batch_id=batch_id))

    @app.post("/ui/batches/<int:batch_id>/withdraw")
    def ui_withdraw(batch_id):
        core.withdraw(batch_id)
        return redirect(url_for("page_batch", batch_id=batch_id))

    return app
