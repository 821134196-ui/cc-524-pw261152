"""Pointwise scoring. Every item is scored independently; a failing item is
recorded as failed and does NOT abort the batch (partial failure support)."""
import math


def score_item(features, model, params):
    """Return {"score": float, "label": int|None}. Raises ValueError on bad input."""
    z = float(model["bias"])
    for name, w in model["weights"].items():
        v = features.get(name)
        if v is None:
            raise ValueError(f"missing feature '{name}'")
        try:
            v = float(v)
        except (TypeError, ValueError):
            raise ValueError(f"feature '{name}' is not numeric: {v!r}")
        z += w * v
    if model.get("output", "sigmoid") == "sigmoid":
        z = max(-60.0, min(60.0, z))
        score = 1.0 / (1.0 + math.exp(-z))
    else:
        score = z
    if "scale" in params:
        score *= float(params["scale"])
    if "clip_min" in params:
        score = max(score, float(params["clip_min"]))
    if "clip_max" in params:
        score = min(score, float(params["clip_max"]))
    score = round(score, 6)
    label = None
    if "threshold" in params:
        label = int(score >= float(params["threshold"]))
    return {"score": score, "label": label}
