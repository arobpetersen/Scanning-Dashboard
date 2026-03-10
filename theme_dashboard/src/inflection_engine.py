from __future__ import annotations

import pandas as pd

from .momentum_engine import compute_theme_momentum
from .rotation_engine import compute_theme_rotation


SIGNAL_PRIORITY = {
    "rotating_out": 5,
    "leadership_deterioration": 4,
    "rotating_into": 3,
    "emerging": 2,
    "accelerating": 1,
    "weakening": 1,
}

MIN_SIGNAL_MOMENTUM = 1.5
MIN_SIGNAL_COMPOSITE = 0.5
MIN_SIGNAL_AVG1M = 0.25


def _empty() -> dict:
    return {
        "signals": pd.DataFrame(
            columns=[
                "detected_at",
                "theme",
                "signal_type",
                "signal_label",
                "reason",
                "rank_change",
                "momentum_score",
                "delta_composite",
                "delta_avg_1m",
                "delta_breadth",
                "priority",
            ]
        ),
        "meta": {"snapshot_count": 0, "insufficient": True, "message": "No historical snapshots available."},
    }


def _recent_trend_flags(history: pd.DataFrame) -> pd.DataFrame:
    if history.empty:
        return pd.DataFrame(columns=["theme", "accel_trend_up", "avg1m_trend_up"])

    rows: list[dict] = []
    for theme, grp in history.sort_values("snapshot_time").groupby("theme"):
        g = grp.tail(4).copy()
        if len(g) < 3:
            rows.append({"theme": theme, "accel_trend_up": False, "avg1m_trend_up": False})
            continue

        comp = g["composite_score"].astype(float).tolist()
        avg1m = g["avg_1m"].astype(float).tolist()
        comp_delta_last = comp[-1] - comp[-2]
        comp_delta_prev = comp[-2] - comp[-3]
        avg1m_delta_last = avg1m[-1] - avg1m[-2]

        rows.append(
            {
                "theme": theme,
                "accel_trend_up": bool(comp_delta_last > 0 and comp_delta_prev >= 0),
                "avg1m_trend_up": bool(avg1m_delta_last > 0),
            }
        )

    return pd.DataFrame(rows)


def compute_theme_inflections(conn, lookback_days: int, top_n: int = 20) -> dict:
    momentum = compute_theme_momentum(conn, lookback_days, top_n=top_n)
    history = momentum["history"]
    if history.empty:
        return _empty()

    snapshot_count = int(history["snapshot_time"].nunique())
    if snapshot_count < 2:
        return {
            **_empty(),
            "meta": {
                "snapshot_count": snapshot_count,
                "insufficient": True,
                "message": "Need at least two boundary snapshots to detect inflection signals.",
            },
        }

    summary = momentum["window_summary"].copy()
    if summary.empty:
        return {
            **_empty(),
            "meta": {
                "snapshot_count": snapshot_count,
                "insufficient": True,
                "message": "No window summary available for inflection detection.",
            },
        }

    rotation = compute_theme_rotation(summary, top_n, momentum["new_leaders"], momentum["dropped_leaders"])
    trend_flags = _recent_trend_flags(history)
    summary = summary.merge(trend_flags, on="theme", how="left")
    summary["accel_trend_up"] = summary["accel_trend_up"].fillna(False)
    summary["avg1m_trend_up"] = summary["avg1m_trend_up"].fillna(False)

    rotate_in = set(rotation["rotating_into"]["theme"].tolist()) if not rotation["rotating_into"].empty else set()
    rotate_out = set(rotation["rotating_out"]["theme"].tolist()) if not rotation["rotating_out"].empty else set()
    deterioration = set(rotation["deterioration"]["theme"].tolist()) if not rotation["deterioration"].empty else set()

    rank_thr = max(5, int(top_n * 0.25))
    detected_at = pd.to_datetime(history["snapshot_time"]).max()
    window_start = pd.to_datetime(history["snapshot_time"]).min()

    signals: list[dict] = []
    for row in summary.to_dict(orient="records"):
        theme = str(row["theme"])
        rc = float(row.get("rank_change", 0))
        ms = float(row.get("momentum_score", 0))
        dc = float(row.get("delta_composite", 0))
        da1m = float(row.get("delta_avg_1m", 0))
        db = float(row.get("delta_breadth", 0))
        accel_up = bool(row.get("accel_trend_up", False))
        avg1m_up = bool(row.get("avg1m_trend_up", False))

        if theme in rotate_out and rc <= -rank_thr and (ms < 0 or dc < 0):
            signals.append(
                {
                    "detected_at": detected_at,
                    "theme": theme,
                    "signal_type": "rotating_out",
                    "signal_label": "Rotating Out",
                    "reason": f"Exited top {top_n}; rank_change {rc:+.0f}, momentum {ms:+.2f}.",
                    "rank_change": rc,
                    "momentum_score": ms,
                    "delta_composite": dc,
                    "delta_avg_1m": da1m,
                    "delta_breadth": db,
                    "priority": SIGNAL_PRIORITY["rotating_out"],
                }
            )
        if theme in deterioration and rc < 0 and ms <= -MIN_SIGNAL_MOMENTUM and db < 0:
            signals.append(
                {
                    "detected_at": detected_at,
                    "theme": theme,
                    "signal_type": "leadership_deterioration",
                    "signal_label": "Leadership Deterioration",
                    "reason": f"Still in leadership but deteriorating; momentum {ms:+.2f}, breadth {db:+.2f}.",
                    "rank_change": rc,
                    "momentum_score": ms,
                    "delta_composite": dc,
                    "delta_avg_1m": da1m,
                    "delta_breadth": db,
                    "priority": SIGNAL_PRIORITY["leadership_deterioration"],
                }
            )
        if theme in rotate_in and rc >= rank_thr and (ms >= MIN_SIGNAL_MOMENTUM or dc >= MIN_SIGNAL_COMPOSITE):
            signals.append(
                {
                    "detected_at": detected_at,
                    "theme": theme,
                    "signal_type": "rotating_into",
                    "signal_label": "Rotating Into Leadership",
                    "reason": f"Entered top {top_n}; rank_change {rc:+.0f}, composite {dc:+.2f}.",
                    "rank_change": rc,
                    "momentum_score": ms,
                    "delta_composite": dc,
                    "delta_avg_1m": da1m,
                    "delta_breadth": db,
                    "priority": SIGNAL_PRIORITY["rotating_into"],
                }
            )
        if rc >= rank_thr and ms >= MIN_SIGNAL_MOMENTUM and db > 0 and da1m >= MIN_SIGNAL_AVG1M:
            signals.append(
                {
                    "detected_at": detected_at,
                    "theme": theme,
                    "signal_type": "emerging",
                    "signal_label": "Emerging",
                    "reason": f"Rank +{int(rc)} with improving momentum ({ms:+.2f}) and breadth ({db:+.2f}).",
                    "rank_change": rc,
                    "momentum_score": ms,
                    "delta_composite": dc,
                    "delta_avg_1m": da1m,
                    "delta_breadth": db,
                    "priority": SIGNAL_PRIORITY["emerging"],
                }
            )
        if accel_up and avg1m_up and dc >= MIN_SIGNAL_COMPOSITE and da1m >= MIN_SIGNAL_AVG1M and ms >= MIN_SIGNAL_MOMENTUM:
            signals.append(
                {
                    "detected_at": detected_at,
                    "theme": theme,
                    "signal_type": "accelerating",
                    "signal_label": "Accelerating",
                    "reason": f"Recent composite and avg_1m trend improving; momentum {ms:+.2f}.",
                    "rank_change": rc,
                    "momentum_score": ms,
                    "delta_composite": dc,
                    "delta_avg_1m": da1m,
                    "delta_breadth": db,
                    "priority": SIGNAL_PRIORITY["accelerating"],
                }
            )
        if ms <= -MIN_SIGNAL_MOMENTUM and dc < 0 and da1m < 0 and db < 0:
            signals.append(
                {
                    "detected_at": detected_at,
                    "theme": theme,
                    "signal_type": "weakening",
                    "signal_label": "Weakening",
                    "reason": f"Momentum/breadth deteriorating; momentum {ms:+.2f}, breadth {db:+.2f}.",
                    "rank_change": rc,
                    "momentum_score": ms,
                    "delta_composite": dc,
                    "delta_avg_1m": da1m,
                    "delta_breadth": db,
                    "priority": SIGNAL_PRIORITY["weakening"],
                }
            )

    if not signals:
        return {
            "signals": pd.DataFrame(columns=_empty()["signals"].columns),
            "meta": {"snapshot_count": snapshot_count, "insufficient": False, "message": "No high-confidence signals detected."},
        }

    # Noise control: keep highest-priority signal per theme to avoid repetitive overlap in a single run.
    sig_df = pd.DataFrame(signals)
    sig_df = (
        sig_df.sort_values(["theme", "priority", "momentum_score", "rank_change"], ascending=[True, False, False, False])
        .drop_duplicates(subset=["theme"], keep="first")
        .sort_values(["priority", "momentum_score", "rank_change"], ascending=[False, False, False])
        .reset_index(drop=True)
    )

    return {
        "signals": sig_df,
        "meta": {
            "snapshot_count": snapshot_count,
            "insufficient": False,
            "message": "ok",
            "detected_at": detected_at,
            "window_start": window_start,
            "window_end": detected_at,
        },
    }
