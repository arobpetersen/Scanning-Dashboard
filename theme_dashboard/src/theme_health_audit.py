from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pandas as pd


AUDIT_PRESETS = [
    "At risk",
    "Empty",
    "Failures",
    "Stale",
    "Cleanup candidates",
    "All active",
    "All inactive",
    "All themes",
]

AUDIT_SORT_OPTIONS = [
    "Unhealthy first",
    "Empty first",
    "Low-count first",
    "Oldest snapshot first",
    "Most failures",
    "Constituent count ascending",
    "Theme name",
]


@dataclass(frozen=True)
class ThemeAuditCounts:
    empty_themes: int
    low_count_themes: int
    stale_themes: int
    recent_failure_themes: int
    active_zero_member_themes: int
    inactive_with_members: int
    recently_changed_themes: int


def _utc_now_naive() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def enrich_theme_health_for_audit(
    health: pd.DataFrame,
    *,
    stale_hours: int,
    recent_change_hours: int = 72,
) -> pd.DataFrame:
    if health.empty:
        return health.copy()

    view = health.copy()
    view["latest_snapshot_time"] = pd.to_datetime(view.get("latest_snapshot_time"), errors="coerce")
    view["updated_at"] = pd.to_datetime(view.get("updated_at"), errors="coerce")
    now = _utc_now_naive()
    stale_cutoff = now - timedelta(hours=int(stale_hours))
    recent_change_cutoff = now - timedelta(hours=int(recent_change_hours))

    view["constituent_count"] = pd.to_numeric(view.get("constituent_count"), errors="coerce").fillna(0).astype(int)
    view["live_failure_count_recent"] = pd.to_numeric(view.get("live_failure_count_recent"), errors="coerce").fillna(0).astype(int)
    view["empty_theme_flag"] = view.get("empty_theme_flag", False).fillna(False).astype(bool)
    view["low_count_flag"] = view.get("low_count_flag", False).fillna(False).astype(bool)
    view["is_active"] = view.get("is_active", False).fillna(False).astype(bool)
    view["no_recent_snapshot_flag"] = view["latest_snapshot_time"].isna()
    view["stale_theme_flag"] = view["no_recent_snapshot_flag"] | (view["latest_snapshot_time"] < stale_cutoff)
    view["recent_failure_flag"] = view["live_failure_count_recent"] > 0
    view["active_zero_members_flag"] = view["is_active"] & (view["constituent_count"] == 0)
    view["inactive_with_members_flag"] = (~view["is_active"]) & (view["constituent_count"] > 0)
    view["recently_changed_flag"] = view["updated_at"].notna() & (view["updated_at"] >= recent_change_cutoff)
    view["at_risk_flag"] = (
        view["active_zero_members_flag"]
        | view["inactive_with_members_flag"]
        | view["low_count_flag"]
        | view["stale_theme_flag"]
        | view["recent_failure_flag"]
    )

    def why_flagged(row) -> str:
        reasons: list[str] = []
        if bool(row["active_zero_members_flag"]):
            reasons.append("empty active theme")
        elif bool(row["empty_theme_flag"]):
            reasons.append("empty theme")
        if bool(row["inactive_with_members_flag"]):
            reasons.append("inactive with members")
        if bool(row["low_count_flag"]) and int(row["constituent_count"]) > 0:
            reasons.append("low constituent count")
        if bool(row["no_recent_snapshot_flag"]):
            reasons.append("no recent snapshot")
        elif bool(row["stale_theme_flag"]):
            reasons.append("stale snapshot")
        if int(row["live_failure_count_recent"]) > 0:
            reasons.append("recent live failures")
        return "; ".join(reasons) if reasons else "healthy"

    def next_action(row) -> str:
        if bool(row["active_zero_members_flag"]):
            return "review assignments or deactivate"
        if bool(row["inactive_with_members_flag"]):
            return "review assignments or reactivate"
        if bool(row["no_recent_snapshot_flag"]) and int(row["constituent_count"]) > 0:
            return "reconstruct or refresh snapshots"
        if bool(row["stale_theme_flag"]) and int(row["constituent_count"]) > 0:
            return "refresh or reconstruct"
        if int(row["live_failure_count_recent"]) >= 3:
            return "review failing members"
        if int(row["live_failure_count_recent"]) > 0:
            return "inspect failures"
        if bool(row["low_count_flag"]) and bool(row["is_active"]):
            return "review assignments"
        if bool(row["empty_theme_flag"]) and not bool(row["is_active"]):
            return "leave inactive or delete"
        return "monitor"

    def audit_status(row) -> str:
        if bool(row["active_zero_members_flag"]) or bool(row["inactive_with_members_flag"]):
            return "needs_attention"
        if bool(row["stale_theme_flag"]) or int(row["live_failure_count_recent"]) > 0 or bool(row["low_count_flag"]):
            return "watch"
        return "healthy"

    view["why_flagged"] = view.apply(why_flagged, axis=1)
    view["next_action"] = view.apply(next_action, axis=1)
    view["audit_status"] = view.apply(audit_status, axis=1)
    return view


def theme_health_audit_counts(health: pd.DataFrame) -> ThemeAuditCounts:
    if health.empty:
        return ThemeAuditCounts(0, 0, 0, 0, 0, 0, 0)
    return ThemeAuditCounts(
        empty_themes=int(health["empty_theme_flag"].fillna(False).sum()),
        low_count_themes=int(health["low_count_flag"].fillna(False).sum()),
        stale_themes=int(health["stale_theme_flag"].fillna(False).sum()),
        recent_failure_themes=int(health["recent_failure_flag"].fillna(False).sum()),
        active_zero_member_themes=int(health["active_zero_members_flag"].fillna(False).sum()),
        inactive_with_members=int(health["inactive_with_members_flag"].fillna(False).sum()),
        recently_changed_themes=int(health["recently_changed_flag"].fillna(False).sum()),
    )


def apply_theme_health_audit_preset(health: pd.DataFrame, preset: str) -> pd.DataFrame:
    if health.empty:
        return health.copy()
    view = health.copy()
    match str(preset or "At risk"):
        case "Empty":
            return view[view["empty_theme_flag"] == True]
        case "Failures":
            return view[view["recent_failure_flag"] == True]
        case "Stale":
            return view[view["stale_theme_flag"] == True]
        case "Cleanup candidates":
            return view[(view["inactive_with_members_flag"] == True) | ((view["empty_theme_flag"] == True) & (view["is_active"] == False))]
        case "All active":
            return view[view["is_active"] == True]
        case "All inactive":
            return view[view["is_active"] == False]
        case "All themes":
            return view
        case _:
            return view[view["at_risk_flag"] == True]


def sort_theme_health_audit(health: pd.DataFrame, sort_mode: str) -> pd.DataFrame:
    if health.empty:
        return health.copy()
    view = health.copy()
    if str(sort_mode or "") == "Empty first":
        return view.sort_values(["empty_theme_flag", "constituent_count", "theme_name"], ascending=[False, True, True]).reset_index(drop=True)
    if str(sort_mode or "") == "Low-count first":
        return view.sort_values(["low_count_flag", "constituent_count", "theme_name"], ascending=[False, True, True]).reset_index(drop=True)
    if str(sort_mode or "") == "Oldest snapshot first":
        return view.sort_values(["latest_snapshot_time", "constituent_count", "theme_name"], ascending=[True, True, True], na_position="first").reset_index(drop=True)
    if str(sort_mode or "") == "Most failures":
        return view.sort_values(["live_failure_count_recent", "constituent_count", "theme_name"], ascending=[False, True, True]).reset_index(drop=True)
    if str(sort_mode or "") == "Constituent count ascending":
        return view.sort_values(["constituent_count", "theme_name"], ascending=[True, True]).reset_index(drop=True)
    if str(sort_mode or "") == "Theme name":
        return view.sort_values(["theme_name"], ascending=[True]).reset_index(drop=True)
    return view.sort_values(
        ["at_risk_flag", "active_zero_members_flag", "inactive_with_members_flag", "stale_theme_flag", "live_failure_count_recent", "constituent_count", "theme_name"],
        ascending=[False, False, False, False, False, True, True],
    ).reset_index(drop=True)


def theme_health_action_eligibility(selection: pd.DataFrame) -> dict[str, list[int] | int]:
    if selection.empty:
        return {
            "selected_count": 0,
            "rebuild_theme_ids": [],
            "backfill_theme_ids": [],
            "deactivate_theme_ids": [],
        }
    view = selection.copy()
    view["theme_id"] = pd.to_numeric(view["theme_id"], errors="coerce").fillna(0).astype(int)
    view["constituent_count"] = pd.to_numeric(view["constituent_count"], errors="coerce").fillna(0).astype(int)
    view["is_active"] = view["is_active"].fillna(False).astype(bool)
    rebuild_ids = view[view["constituent_count"] > 0]["theme_id"].astype(int).tolist()
    deactivate_ids = view[(view["is_active"] == True) & (view["constituent_count"] == 0)]["theme_id"].astype(int).tolist()
    return {
        "selected_count": int(len(view)),
        "rebuild_theme_ids": rebuild_ids,
        "backfill_theme_ids": rebuild_ids,
        "deactivate_theme_ids": deactivate_ids,
    }
