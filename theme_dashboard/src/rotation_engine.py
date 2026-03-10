from __future__ import annotations

import pandas as pd


def _empty() -> dict:
    empty = pd.DataFrame()
    return {
        "rotating_into": empty,
        "rotating_out": empty,
        "emerging": empty,
        "fading": empty,
        "acceleration": empty,
        "deterioration": empty,
        "rotation_intensity": {
            "entered_top_n": 0,
            "exited_top_n": 0,
            "rotation_intensity_score": 0.0,
        },
    }


def compute_theme_rotation(summary: pd.DataFrame, top_n: int, new_leaders: list[str], dropped_leaders: list[str]) -> dict:
    if summary.empty:
        return _empty()

    df = summary.copy()
    enter_cond = (df["rank_end"] <= top_n) & (df["rank_start"] > top_n)
    exit_cond = (df["rank_start"] <= top_n) & (df["rank_end"] > top_n)

    rotating_into = df[enter_cond].sort_values(["rank_change", "momentum_score"], ascending=False)
    rotating_out = df[exit_cond].sort_values(["rank_change", "momentum_score"], ascending=[True, True])

    emerging = df[
        (df["rank_change"] >= max(5, int(top_n * 0.25)))
        & (df["delta_composite"] > 0)
        & (df["delta_avg_1m"] > 0)
        & (df["delta_breadth"] > 0)
    ].sort_values(["momentum_score", "rank_change"], ascending=False)

    fading = df[
        (df["rank_change"] <= -max(5, int(top_n * 0.25)))
        & (df["delta_composite"] < 0)
        & (df["delta_avg_1m"] < 0)
        & (df["delta_breadth"] < 0)
    ].sort_values(["momentum_score", "rank_change"], ascending=[True, True])

    leaders_now = df[df["rank_end"] <= top_n]
    acceleration = leaders_now[
        (leaders_now["delta_composite"] > 0)
        & (leaders_now["rank_change"] > 0)
        & (leaders_now["momentum_score"] >= leaders_now["momentum_score"].quantile(0.6))
    ].sort_values("momentum_score", ascending=False)

    deterioration = leaders_now[
        (leaders_now["delta_composite"] < 0)
        & (leaders_now["rank_change"] < 0)
    ].sort_values(["momentum_score", "rank_change"], ascending=[True, True])

    rotation_intensity_score = round(((len(new_leaders) + len(dropped_leaders)) / max(1, top_n)) * 100.0, 2)

    return {
        "rotating_into": rotating_into,
        "rotating_out": rotating_out,
        "emerging": emerging,
        "fading": fading,
        "acceleration": acceleration,
        "deterioration": deterioration,
        "rotation_intensity": {
            "entered_top_n": int(len(new_leaders)),
            "exited_top_n": int(len(dropped_leaders)),
            "rotation_intensity_score": rotation_intensity_score,
        },
    }
