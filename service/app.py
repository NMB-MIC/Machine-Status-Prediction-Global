# service/app.py — Global Phase 5 inference service
# Supports:
#   1) New automatic Phase 4 artifact schema: phase4_auto_v1
#   2) Legacy ASSY artifact schema from the first deployment

import os
import re
import math
import time
import joblib
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional, Tuple
try:
    from prometheus_fastapi_instrumentator import Instrumentator
except Exception:
    Instrumentator = None

# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------
MODEL_DIR = os.environ.get("MODEL_DIR", "/models")
P50_MODEL_PATH = os.environ.get("P50_MODEL_PATH", f"{MODEL_DIR}/lgbm_quantile_p50_final.pkl")
P90_MODEL_PATH = os.environ.get("P90_MODEL_PATH", f"{MODEL_DIR}/lgbm_quantile_p90_final.pkl")
TYPE_MODEL_PATH = os.environ.get("TYPE_MODEL_PATH", f"{MODEL_DIR}/lgbm_next_type.pkl")
ARTIFACTS_PATH = os.environ.get("ARTIFACTS_PATH", f"{MODEL_DIR}/artifacts_phase4.pkl")

TYPE_CONF_THRESHOLD_ENV = os.environ.get("TYPE_CONF_THRESHOLD")
MIN_MARGIN_SECS = float(os.environ.get("MIN_MARGIN_SECS", "5.0"))
SERVICE_VERSION = os.environ.get("SERVICE_VERSION", "phase5_global_v1")

# ---------------------------------------------------------------------
# Text/status helpers — must match Phase 4 normalization as closely as possible
# ---------------------------------------------------------------------
def normalize_status_text(x) -> str:
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    s = str(x).strip().lower()
    s = s.replace("\u00a0", " ")
    s = re.sub(r"\s+", "_", s)
    s = s.replace("/", "_")
    s = s.replace("-", "_")
    s = re.sub(r"_+", "_", s)
    return s.strip("_")


def sanitize_feature_token(x) -> str:
    s = normalize_status_text(x)
    if not s:
        return "missing"
    s = re.sub(r"[^a-zA-Z0-9_]+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_") or "missing"


def canonicalize_status(x, alias_map: Dict[str, str]) -> str:
    s = normalize_status_text(x)
    alias_norm = {normalize_status_text(k): normalize_status_text(v) for k, v in (alias_map or {}).items()}
    return alias_norm.get(s, s)


def to_float(x, default: float = 0.0) -> float:
    try:
        if x is None or pd.isna(x):
            return default
        return float(x)
    except Exception:
        return default


def parse_utc(ts: str) -> datetime:
    return datetime.fromisoformat(str(ts).replace("Z", "+00:00")).astimezone(timezone.utc)


def predict_with_best_iteration(model, X):
    best_iter = getattr(model, "best_iteration_", None)
    if best_iter is None or best_iter == 0:
        return model.predict(X)
    try:
        return model.predict(X, num_iteration=best_iter)
    except TypeError:
        return model.predict(X)


def predict_proba_with_best_iteration(model, X):
    best_iter = getattr(model, "best_iteration_", None)
    if best_iter is None or best_iter == 0:
        return model.predict_proba(X)
    try:
        return model.predict_proba(X, num_iteration=best_iter)
    except TypeError:
        return model.predict_proba(X)


def inverse_transform_eta(pred, target_mode: str):
    pred = np.asarray(pred, dtype=np.float64)
    if target_mode == "log1p":
        return np.expm1(pred)
    return pred


def clean_eta_pred(pred, clip_upper=None):
    pred = np.asarray(pred, dtype=np.float64)
    pred = np.nan_to_num(
        pred,
        nan=1.0,
        posinf=clip_upper if clip_upper is not None else 1e9,
        neginf=1.0,
    )
    pred = np.maximum(pred, 1.0)
    if clip_upper is not None:
        pred = np.minimum(pred, clip_upper)
    return pred


# ---------------------------------------------------------------------
# Artifact adapters
# ---------------------------------------------------------------------
class ArtifactAdapter:
    def __init__(self, artifacts: Dict[str, Any]):
        self.artifacts = artifacts
        self.schema = "new" if "feature_contract" in artifacts else "legacy"

        if self.schema == "new":
            self._init_new()
        else:
            self._init_legacy()

    # ----------------------------
    # New automatic Phase 4 schema
    # ----------------------------
    def _init_new(self):
        a = self.artifacts
        fc = a["feature_contract"]
        sc = a.get("status_config", {})
        eta = a.get("eta_model_contract", {})
        typ = a.get("type_model_contract", {})

        self.process_id = str(a.get("process_id", "unknown"))
        self.model_version = str(a.get("artifact_schema_version", "phase4_auto_v1"))
        self.feature_version = str(a.get("feature_version", "auto_features_v1"))

        self.feature_cols = list(fc["feature_cols"])
        self.feature_fill_values = fc.get("feature_fill_values", {})
        self.feature_groups = fc.get("feature_groups", {})
        self.rolling_windows_min = list(fc.get("rolling_windows_min", [5, 15, 30, 60, 120]))

        self.status_alias_map = sc.get("status_alias_map", {}) or {}
        self.normal_statuses = set(normalize_status_text(s) for s in sc.get("normal_statuses", []))
        self.target_statuses = set(normalize_status_text(s) for s in sc.get("target_statuses", []))
        self.hidden_statuses = set(normalize_status_text(s) for s in sc.get("hidden_statuses", []))
        self.all_statuses = list(sc.get("all_statuses", sorted(self.normal_statuses | self.target_statuses)))

        self.p50_target_mode = eta.get("selected_p50_target_mode", "raw")
        self.p90_target_mode = eta.get("selected_p90_target_mode", "raw")
        self.clip_max = to_float(eta.get("clip_max_sec"), 1e9)
        self.eta_max_cap_sec = to_float(eta.get("eta_max_cap_sec"), self.clip_max if self.clip_max else 1e9)

        self.p50_cal = eta.get("p50_calibration", {}) or {}
        self.p90_cal = eta.get("p90_calibration", {}) or {}
        self.p50_global_scale = to_float(self.p50_cal.get("global_scale"), 1.0)
        self.p50_scale_by_machine = self.p50_cal.get("scale_by_machine", {}) or {}
        self.p90_global_multiplier = to_float(self.p90_cal.get("global_multiplier"), 1.0)
        self.p90_multiplier_by_machine = self.p90_cal.get("multiplier_by_machine", {}) or {}
        self.eta_postprocess_maps = eta.get("eta_postprocess_maps", {}) or {}

        self.behavior_maps = fc.get("behavior_maps", {}) or {}
        self._prepare_behavior_maps()

        self.type_label_encoder = typ.get("type_label_encoder")
        self.type_classes = [str(x) for x in typ.get("type_classes", [])]
        if not self.type_classes and self.type_label_encoder is not None:
            self.type_classes = [str(x) for x in self.type_label_encoder.classes_]
        if not self.type_classes:
            self.type_classes = sorted(self.target_statuses)

        artifact_thr = to_float(typ.get("global_confidence_threshold"), 0.60)
        self.type_conf_threshold = float(TYPE_CONF_THRESHOLD_ENV) if TYPE_CONF_THRESHOLD_ENV is not None else artifact_thr

        self.phase5_contract = a.get("phase5_output_contract", {})

    def _prepare_behavior_maps(self):
        self.machine_behavior_lookup = {}
        self.status_behavior_lookup = {}
        self.global_machine_behavior = {}
        self.global_status_behavior = {}

        bm = self.behavior_maps
        machine_df = bm.get("machine_behavior_train")
        status_df = bm.get("status_behavior_train")
        self.global_machine_behavior = bm.get("global_machine_behavior", {}) or {}
        self.global_status_behavior = bm.get("global_status_behavior", {}) or {}

        if isinstance(machine_df, pd.DataFrame):
            for _, r in machine_df.iterrows():
                key = (str(r.get("process")), str(r.get("mc_no")))
                self.machine_behavior_lookup[key] = {
                    c: to_float(r.get(c), to_float(self.global_machine_behavior.get(c), 0.0))
                    for c in machine_df.columns
                    if c not in ["process", "mc_no"]
                }

        if isinstance(status_df, pd.DataFrame):
            for _, r in status_df.iterrows():
                key = normalize_status_text(r.get("mc_status"))
                self.status_behavior_lookup[key] = {
                    c: to_float(r.get(c), to_float(self.global_status_behavior.get(c), 0.0))
                    for c in status_df.columns
                    if c != "mc_status"
                }

    # ----------------------------
    # Legacy ASSY schema
    # ----------------------------
    def _init_legacy(self):
        a = self.artifacts
        self.process_id = str(os.environ.get("PROCESS_ID", "assy"))
        self.model_version = os.environ.get("MODEL_VERSION", "legacy_assy_v1")
        self.feature_version = os.environ.get("FEATURE_VERSION", "feats_39_behavioral_v2")

        self.feature_cols = list(a["feature_cols"])
        self.feature_fill_values = a.get("global_medians", {})
        self.feature_groups = {}
        self.rolling_windows_min = [15, 60]

        self.status_alias_map = {
            "m/c stop": "m_c_stop",
            "no work": "no_work",
            "fullwork": "fullwork",
            "alarm": "alarm",
            "run": "run",
        }
        self.normal_statuses = {"run"}
        self.target_statuses = {normalize_status_text(x) for x in a.get("classes", ["alarm", "fullwork", "m/c stop", "no work"])}
        self.hidden_statuses = {"run", "no_work", "no work"}
        self.all_statuses = sorted(self.normal_statuses | self.target_statuses)

        self.p50_target_mode = "raw"
        self.p90_target_mode = "raw"
        self.clip_max = to_float(a.get("clip_max"), 2402.0)
        self.eta_max_cap_sec = self.clip_max

        self.p50_global_scale = 1.0
        self.p50_scale_by_machine = a.get("p50_scale_by_mc", {}) or {}
        self.p90_global_multiplier = to_float(a.get("p90_multiplier_global"), 1.0)
        self.p90_multiplier_by_machine = a.get("p90_multiplier_by_mc", {}) or {}
        self.eta_postprocess_maps = {}

        self.per_mc_medians = a.get("per_mc_medians")
        self.global_medians = a.get("global_medians")

        self.floor_by_mc = a.get("floor_by_mc", {}) or {}
        self.cap_by_mc = a.get("cap_by_mc", {}) or {}
        self.med_by_mc = a.get("med_by_mc", {}) or {}
        self.global_floor = to_float(a.get("global_floor"), 5.0)
        self.global_cap = to_float(a.get("global_cap"), 3600.0)
        self.global_median = to_float(a.get("global_median"), 60.0)

        self.mc_median_gap_map = a.get("mc_median_gap_map", {}) or {}
        self.mc_alert_ratio_map = a.get("mc_alert_ratio_map", {}) or {}
        self.mc_event_rate_map = a.get("mc_event_rate_map", {}) or {}
        self.activity_rate_map = a.get("activity_rate_map", {}) or {}
        self.shift_thr_by_mc = a.get("shift_thr_by_mc", {}) or {}
        self.global_median_gap = to_float(a.get("global_median_gap"), 80.0)
        self.global_alert_ratio = to_float(a.get("global_alert_ratio"), 0.5)
        self.global_event_rate = to_float(a.get("global_event_rate"), 0.1)
        self.global_activity = a.get("global_activity", {}) or {}
        self.global_shift_thr = to_float(a.get("global_shift_thr"), 0.0)

        self.type_classes = [str(x) for x in a.get("classes", ["alarm", "fullwork", "m/c stop", "no work"])]
        self.type_label_encoder = None
        self.type_conf_threshold = float(TYPE_CONF_THRESHOLD_ENV) if TYPE_CONF_THRESHOLD_ENV is not None else 0.60
        self.phase5_contract = {}

    # ----------------------------
    # Common helpers
    # ----------------------------
    def canonical_status(self, status: str) -> str:
        return canonicalize_status(status, self.status_alias_map)

    def is_target(self, status: str) -> bool:
        s = self.canonical_status(status)
        if self.target_statuses:
            return s in self.target_statuses
        return s not in self.normal_statuses

    def machine_key(self, process: str, mc_no: str):
        return (str(process), str(mc_no))

    def get_p50_scale(self, process: str, mc_no: str) -> float:
        # New schema keys are usually (process, mc_no). Legacy keys are mc_no.
        key = self.machine_key(process, mc_no)
        return to_float(
            self.p50_scale_by_machine.get(key, self.p50_scale_by_machine.get(str(mc_no), self.p50_global_scale)),
            self.p50_global_scale,
        )

    def get_p90_multiplier(self, process: str, mc_no: str) -> float:
        key = self.machine_key(process, mc_no)
        return to_float(
            self.p90_multiplier_by_machine.get(key, self.p90_multiplier_by_machine.get(str(mc_no), self.p90_global_multiplier)),
            self.p90_global_multiplier,
        )

    def get_new_eta_bounds(self, process: str, mc_no: str, status: str) -> Tuple[float, float, float, str]:
        maps = self.eta_postprocess_maps or {}
        by_ms = maps.get("by_machine_status", {}) or {}
        by_m = maps.get("by_machine", {}) or {}
        by_s = maps.get("by_status", {}) or {}
        glob = maps.get("global", {}) or {}

        status = self.canonical_status(status)
        key_ms = (str(process), str(mc_no), status)
        key_m = (str(process), str(mc_no))

        if key_ms in by_ms:
            b, src = by_ms[key_ms], "machine_status"
        elif key_m in by_m:
            b, src = by_m[key_m], "machine"
        elif status in by_s:
            b, src = by_s[status], "status"
        else:
            b, src = glob, "global"

        floor = to_float(b.get("floor"), 1.0)
        median = to_float(b.get("median"), floor)
        cap = to_float(b.get("cap"), self.eta_max_cap_sec)
        return floor, median, cap, src

    def get_legacy_bounds(self, mc_no: str) -> Tuple[float, float, float, str]:
        floor = to_float(self.floor_by_mc.get(mc_no), self.global_floor)
        median = to_float(self.med_by_mc.get(mc_no), self.global_median)
        cap = to_float(self.cap_by_mc.get(mc_no), self.global_cap)
        return floor, median, cap, "legacy_machine_or_global"

    def enrich_lookup_features(self, process: str, mc_no: str, status: str, features: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(features or {})
        status = self.canonical_status(status)

        if self.schema == "new":
            machine_vals = self.machine_behavior_lookup.get(
                self.machine_key(process, mc_no),
                self.global_machine_behavior,
            )
            status_vals = self.status_behavior_lookup.get(status, self.global_status_behavior)
            for k, v in (machine_vals or {}).items():
                out[k] = v
            for k, v in (status_vals or {}).items():
                out[k] = v

            eps = 1.0
            if "time_since_last_target_sec" in out and "mc_median_y_sec" in out:
                out["time_since_last_target_over_mc_median_y"] = to_float(out["time_since_last_target_sec"]) / (to_float(out["mc_median_y_sec"]) + eps)
            if "event_gap_sec" in out and "mc_median_event_gap_sec" in out:
                out["event_gap_over_mc_median_event_gap"] = to_float(out["event_gap_sec"]) / (to_float(out["mc_median_event_gap_sec"]) + eps)
            if "time_since_status_change_sec" in out and "mc_median_y_sec" in out:
                out["time_since_status_change_over_mc_median_y"] = to_float(out["time_since_status_change_sec"]) / (to_float(out["mc_median_y_sec"]) + eps)
            if "target_event_count_prior" in out and "machine_event_index" in out:
                out["target_event_count_prior_over_machine_events"] = to_float(out["target_event_count_prior"]) / (to_float(out["machine_event_index"]) + 1.0)

        else:
            hour = int(to_float(out.get("hour"), 0))
            act_profile = self.activity_rate_map.get(mc_no, self.global_activity)
            act_score = to_float(act_profile.get(hour, 0.0), 0.0) if isinstance(act_profile, dict) else 0.0
            thr = to_float(self.shift_thr_by_mc.get(mc_no), self.global_shift_thr)
            out.update({
                "mc_median_gap": to_float(self.mc_median_gap_map.get(mc_no), self.global_median_gap),
                "mc_alert_ratio": to_float(self.mc_alert_ratio_map.get(mc_no), self.global_alert_ratio),
                "mc_event_rate": to_float(self.mc_event_rate_map.get(mc_no), self.global_event_rate),
                "in_shift": 1.0 if act_score >= thr else 0.0,
                "activity_score": act_score,
            })
        return out

    def build_X(self, process: str, mc_no: str, status: str, features: Dict[str, Any]) -> pd.DataFrame:
        features = self.enrich_lookup_features(process, mc_no, status, features)
        row = {c: features.get(c, np.nan) for c in self.feature_cols}
        X = pd.DataFrame([row], columns=self.feature_cols)
        X = X.replace([np.inf, -np.inf], np.nan)

        if self.schema == "legacy":
            if isinstance(self.per_mc_medians, pd.DataFrame) and mc_no in self.per_mc_medians.index:
                X = X.fillna(self.per_mc_medians.loc[mc_no])
            if self.global_medians is not None:
                X = X.fillna(self.global_medians)
            X = X.fillna(0.0)
            return X.astype(np.float32)

        fill = self.feature_fill_values
        if isinstance(fill, pd.Series):
            X = X.fillna(fill)
        elif isinstance(fill, dict):
            X = X.fillna(fill)
        else:
            X = X.fillna(0.0)
        X = X.fillna(0.0)
        return X.astype(np.float32)

    def type_name_from_index(self, idx: int) -> Optional[str]:
        if self.type_label_encoder is not None:
            try:
                return str(self.type_label_encoder.inverse_transform([idx])[0])
            except Exception:
                pass
        if 0 <= idx < len(self.type_classes):
            return str(self.type_classes[idx])
        return None


# ---------------------------------------------------------------------
# Load models/artifacts
# ---------------------------------------------------------------------
try:
    p50_model = joblib.load(P50_MODEL_PATH)
    p90_model = joblib.load(P90_MODEL_PATH)
    type_model = joblib.load(TYPE_MODEL_PATH)
    artifacts = joblib.load(ARTIFACTS_PATH)
    adapter = ArtifactAdapter(artifacts)
except Exception as e:
    raise RuntimeError(f"Failed to load models/artifacts: {e}")

# ---------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------
app = FastAPI(title="Global Alert ETA Service", version=SERVICE_VERSION)
if Instrumentator is not None:
    Instrumentator().instrument(app).expose(app)


class InferRequest(BaseModel):
    mc_no: str = Field(..., description="Machine ID")
    occurred_ts: str = Field(..., description="Event time, ISO-8601")
    features: Dict[str, Any] = Field(default_factory=dict, description="Feature dict from Flink")
    process: Optional[str] = Field(None, description="Process ID. Required for global mode; defaults to artifact process_id.")
    plant: Optional[str] = Field(None, description="Plant ID, optional")
    mc_status: Optional[str] = Field(None, description="Current machine status, optional but recommended")


class InferResponse(BaseModel):
    eta_p50_sec: float
    eta_p90_sec: float
    eta_p50_ts: str
    eta_p90_ts: str
    next_type: Optional[str]
    type_conf: Optional[float]
    model_version: str
    feature_version: str
    process: Optional[str] = None
    plant: Optional[str] = None


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service_version": SERVICE_VERSION,
        "artifact_schema": adapter.schema,
        "model_version": adapter.model_version,
        "feature_version": adapter.feature_version,
        "process_id": adapter.process_id,
        "num_features": len(adapter.feature_cols),
        "classes": adapter.type_classes,
        "normal_statuses": sorted(adapter.normal_statuses),
        "target_statuses": sorted(adapter.target_statuses),
        "type_conf_threshold": adapter.type_conf_threshold,
    }


@app.get("/metadata")
def metadata():
    """Metadata used by Flink/dashboard/monitor so they do not hardcode ASSY/GD statuses."""
    return {
        "artifact_schema": adapter.schema,
        "process_id": adapter.process_id,
        "model_version": adapter.model_version,
        "feature_version": adapter.feature_version,
        "feature_cols": adapter.feature_cols,
        "feature_count": len(adapter.feature_cols),
        "rolling_windows_min": adapter.rolling_windows_min,
        "all_statuses": adapter.all_statuses,
        "normal_statuses": sorted(adapter.normal_statuses),
        "target_statuses": sorted(adapter.target_statuses),
        "hidden_statuses": sorted(adapter.hidden_statuses),
        "status_alias_map": adapter.status_alias_map,
        "type_classes": adapter.type_classes,
        "type_conf_threshold": adapter.type_conf_threshold,
    }


@app.post("/infer", response_model=InferResponse)
def infer(req: InferRequest):
    t0 = time.time()
    try:
        process = str(req.process or adapter.process_id).strip().lower()
        plant = req.plant
        mc_no = str(req.mc_no).strip()
        occurred = parse_utc(req.occurred_ts)
        current_status = adapter.canonical_status(req.mc_status or req.features.get("mc_status", ""))

        X = adapter.build_X(process, mc_no, current_status, req.features)

        # P50
        p50_model_space = predict_with_best_iteration(p50_model, X)
        p50_raw = clean_eta_pred(
            inverse_transform_eta(p50_model_space, adapter.p50_target_mode),
            clip_upper=adapter.clip_max,
        )[0]
        p50 = p50_raw * adapter.get_p50_scale(process, mc_no)
        p50 = clean_eta_pred([p50], clip_upper=adapter.clip_max)[0]

        # Bounds/postprocess
        if adapter.schema == "new":
            floor, median, cap, _src = adapter.get_new_eta_bounds(process, mc_no, current_status)
        else:
            floor, median, cap, _src = adapter.get_legacy_bounds(mc_no)

        p50 = float(np.clip(p50, floor, cap))

        # P90
        p90_model_space = predict_with_best_iteration(p90_model, X)
        p90_raw = clean_eta_pred(
            inverse_transform_eta(p90_model_space, adapter.p90_target_mode),
            clip_upper=adapter.clip_max,
        )[0]
        p90 = p90_raw * adapter.get_p90_multiplier(process, mc_no)
        min_margin = max(MIN_MARGIN_SECS, 0.05 * p50)
        p90 = max(p50 + min_margin, p90, median)
        p90 = float(np.clip(p90, p50 + min_margin, max(cap, p50 + min_margin)))

        # Type
        proba = predict_proba_with_best_iteration(type_model, X)[0]
        type_idx = int(np.argmax(proba))
        type_name = adapter.type_name_from_index(type_idx)
        type_conf = float(np.max(proba))

        if type_conf < adapter.type_conf_threshold:
            type_name_display = None
            type_conf_display = None
        else:
            type_name_display = type_name
            type_conf_display = type_conf

        return InferResponse(
            eta_p50_sec=float(p50),
            eta_p90_sec=float(p90),
            eta_p50_ts=(occurred + timedelta(seconds=float(p50))).isoformat(),
            eta_p90_ts=(occurred + timedelta(seconds=float(p90))).isoformat(),
            next_type=type_name_display,
            type_conf=type_conf_display,
            model_version=adapter.model_version,
            feature_version=adapter.feature_version,
            process=process,
            plant=plant,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Inference failed: {e}")
