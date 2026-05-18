from __future__ import annotations

from dataclasses import asdict, dataclass

import numpy as np
import pandas as pd

from .market_context import (
    DOWN_DAY,
    FADE,
    QUIET,
    STRONG_DOWN_DAY,
    STRONG_UP_DAY,
    TREND_DOWN,
    TREND_UP,
    UP_DAY,
    VOLATILE_CHOP,
    VOLATILE_FADE,
)
from .rankings import (
    _load_current_ranking_constituents,
    compute_current_ranking_operating_snapshot,
    current_ticker_is_eligible,
)


DEFAULT_SIGNAL_LIMIT = 8

BROAD_THRUST_MIN_TICKERS = 3
BROAD_THRUST_1D_PCT = 5.0
BROAD_THRUST_1W_PCT = 12.0

EXTREME_STANDOUT_EXCESS_PCT = 30.0
OUTLIER_STANDOUT_EXCESS_PCT = 40.0
OUTLIER_MIN_THEME_AVG_PCT = 8.0
OUTLIER_BROAD_CONFIRMATION_MIN_TICKERS = 3

EMERGING_CLUSTER_MIN_RANK = 13
EMERGING_CLUSTER_MIN_STRONG_TICKERS = 2
EMERGING_CLUSTER_MIN_ELIGIBLE_TICKERS = 3
EMERGING_CLUSTER_MIN_ELIGIBLE_COVERAGE_RATIO = 0.60
EMERGING_CLUSTER_MIN_STRONG_COVERAGE_RATIO = 0.40

WEAKENING_TOP_RANK_MAX = 12
WEAKENING_MAX_POSITIVE_BREADTH_PCT = 40.0
WEAKENING_MAX_STRONG_TICKERS = 1
WEAKENING_THIN_ELIGIBLE_MAX = 2

MAX_SIGNALS_PER_THEME = 2
SIGNAL_TYPE_PRIORITY = {
    "Broad Same-Theme Thrust": 500,
    "Outlier-Led Theme (Broadly Confirmed)": 400,
    "Outlier-Led Theme (Narrow/Fragile)": 390,
    "Emerging Cluster": 300,
    "Weakening / Narrowing Theme": 200,
    "Extreme Ticker Standout": 100,
}

MARKET_CONTEXT_DOWN_TAPE_BOOST = 24.0
MARKET_CONTEXT_QUIET_TAPE_BOOST = 14.0
MARKET_CONTEXT_GENERIC_UP_TAPE_PENALTY = 24.0
MARKET_CONTEXT_STRONG_OUTPERFORMANCE_PCT = 8.0
MARKET_CONTEXT_CLUSTER_STRONG_TICKER_COUNT = 4
DEFAULT_SCOUT_ITEM_LIMIT = 5
SCOUT_OVERLAP_MIN_THEMES = 2
SCOUT_OVERLAP_MIN_COHERENT_TICKERS = 3
SCOUT_OVERLAP_MIN_FAMILY_PURITY = 0.60
SCOUT_WIDESPREAD_BROAD_THRUST_MIN_SIGNALS = 8
OVERLAP_THEME_STOPWORDS = {
    "and",
    "the",
    "tech",
    "technology",
    "systems",
    "services",
    "software",
    "hardware",
    "infrastructure",
}
OVERLAP_GENERIC_SHARED_TOKENS = {
    "apps",
    "cloud",
    "data",
    "enterprise",
    "platform",
    "software",
}
OVERLAP_FAMILY_KEYWORDS = {
    "Mobility / Vision": {"lidar", "autonomous", "vehicle", "vehicles", "vision", "mobility", "robotaxi", "adas"},
    "AI / Software": {
        "agentic",
        "ai",
        "app",
        "apps",
        "artificial",
        "cloud",
        "compute",
        "cyber",
        "data",
        "devops",
        "enterprise",
        "infrastructure",
        "intelligence",
        "saas",
        "security",
        "software",
    },
    "Cyber / Enterprise Software": {"cyber", "security", "saas", "cloud", "enterprise", "software"},
    "Space / Defense": {"space", "defense", "aerospace", "satellite", "launch"},
}


@dataclass(frozen=True)
class ThemePatternSignal:
    signal_type: str
    theme: str
    tickers: tuple[str, ...]
    why_notable: str
    read: str
    priority: float
    metadata: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        out = asdict(self)
        out["tickers"] = list(self.tickers)
        return out


@dataclass(frozen=True)
class MarketScoutItem:
    headline: str
    pattern: str
    why_it_matters: str
    tickers_to_inspect: tuple[str, ...]
    next_look: str
    priority: float
    signal_indices: tuple[int, ...]
    metadata: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        out = asdict(self)
        out["tickers_to_inspect"] = list(self.tickers_to_inspect)
        out["signal_indices"] = list(self.signal_indices)
        return out


def build_theme_pattern_audit(
    conn,
    limit: int = DEFAULT_SIGNAL_LIMIT,
    *,
    qqq_market_context: dict[str, object] | None = None,
) -> list[dict[str, object]]:
    raw_constituents = _load_current_ranking_constituents(conn)
    current_snapshot = compute_current_ranking_operating_snapshot(conn)
    theme_metrics = current_snapshot.get("theme_metrics", pd.DataFrame())
    rankings = current_snapshot.get("standardized_rankings", pd.DataFrame())
    if rankings.empty:
        rankings = current_snapshot.get("rankings", pd.DataFrame())

    signals = build_theme_pattern_signals(
        raw_constituents,
        theme_metrics,
        rankings,
        limit=limit,
        qqq_market_context=qqq_market_context,
    )
    return [signal.to_dict() for signal in signals]


def build_market_scout_items(
    signals: list[dict[str, object]] | list[ThemePatternSignal],
    *,
    qqq_market_context: dict[str, object] | None = None,
    limit: int = DEFAULT_SCOUT_ITEM_LIMIT,
) -> list[dict[str, object]]:
    normalized = [_normalize_signal(signal) for signal in signals]
    if not normalized:
        return []

    items: list[MarketScoutItem] = []
    used_signal_indices: set[int] = set()

    overlap_items = _build_overlap_scout_items(normalized, qqq_market_context)
    if overlap_items:
        items.extend(overlap_items)
        for overlap_item in overlap_items:
            if not bool(overlap_item.metadata.get("quality_note")):
                used_signal_indices.update(overlap_item.signal_indices)

    broad_item = _build_grouped_broad_thrust_item(
        normalized,
        qqq_market_context,
        skip_indices=used_signal_indices,
    )
    if broad_item is not None:
        items.append(broad_item)
        used_signal_indices.update(broad_item.signal_indices)

    for idx, signal in enumerate(normalized):
        if idx in used_signal_indices:
            continue
        item = _signal_to_scout_item(idx, signal, qqq_market_context)
        if item is not None:
            items.append(item)

    ordered = sorted(
        items,
        key=lambda item: (
            -float(item.priority),
            item.pattern,
            item.headline,
        ),
    )
    return [item.to_dict() for item in ordered[: max(int(limit), 0)]]


def no_market_scout_items_message() -> str:
    return "No high-value scout items detected from the latest stored data."


def format_theme_pattern_signal_evidence(signal: dict[str, object] | ThemePatternSignal) -> list[str]:
    payload = signal.to_dict() if isinstance(signal, ThemePatternSignal) else dict(signal or {})
    signal_type = str(payload.get("signal_type") or "")
    tickers = payload.get("tickers") or []
    metadata = payload.get("metadata") or {}
    if not isinstance(metadata, dict):
        metadata = {}

    evidence: list[str] = []
    rank = metadata.get("rank")
    if rank is not None:
        evidence.append(f"Current rank: #{rank}.")
    if tickers:
        evidence.append(f"Top involved tickers: {', '.join(str(ticker) for ticker in tickers)}.")
    if metadata.get("market_context_note"):
        evidence.append(f"Market context: {metadata.get('market_context_note')}")

    if signal_type == "Broad Same-Theme Thrust":
        evidence.append(
            "Trigger: "
            f"{metadata.get('strong_ticker_count', 'n/a')} strong movers >= "
            f"{BROAD_THRUST_1D_PCT:.1f}% 1D or >= {BROAD_THRUST_1W_PCT:.1f}% 1W "
            f"(minimum {BROAD_THRUST_MIN_TICKERS})."
        )
        evidence.append(f"Theme 1W average: {_pct(metadata.get('theme_avg_1w'))}.")
        if metadata.get("theme_move_vs_qqq_pct") is not None:
            evidence.append(f"Theme move vs QQQ 1D: {_pct(metadata.get('theme_move_vs_qqq_pct'))}.")
    elif signal_type == "Extreme Ticker Standout":
        evidence.append(
            "Trigger: "
            f"{metadata.get('window', '1W/1M')} excess {_pct(metadata.get('excess_pct'))} "
            f"versus threshold {_pct(EXTREME_STANDOUT_EXCESS_PCT)}."
        )
        evidence.append(
            f"Ticker move: {_pct(metadata.get('ticker_perf'))}; "
            f"theme average: {_pct(metadata.get('theme_avg'))}."
        )
        if metadata.get("ticker_move_vs_qqq_pct") is not None:
            evidence.append(f"Ticker move vs QQQ 1D: {_pct(metadata.get('ticker_move_vs_qqq_pct'))}.")
    elif signal_type.startswith("Outlier-Led Theme"):
        status = "broadly confirmed" if bool(metadata.get("broad_confirmation")) else "narrow/fragile"
        evidence.append(
            "Trigger: "
            f"leader excess {_pct(metadata.get('excess_vs_avg_pct'))} "
            f"versus threshold {_pct(OUTLIER_STANDOUT_EXCESS_PCT)}."
        )
        evidence.append(
            f"Leader 1M: {_pct(metadata.get('leader_perf_1m'))}; "
            f"next member 1M: {_pct(metadata.get('next_perf_1m'))}; "
            f"theme 1M average: {_pct(metadata.get('theme_avg_1m'))}."
        )
        if metadata.get("ticker_move_vs_qqq_pct") is not None:
            evidence.append(f"Leader move vs QQQ 1D: {_pct(metadata.get('ticker_move_vs_qqq_pct'))}.")
        evidence.append(
            f"Breadth status: {status}; "
            f"{metadata.get('strong_ticker_count', 'n/a')} strong movers "
            f"versus confirmation threshold {OUTLIER_BROAD_CONFIRMATION_MIN_TICKERS}."
        )
    elif signal_type == "Emerging Cluster":
        evidence.append(
            "Trigger: "
            f"rank #{metadata.get('rank', 'n/a')} >= {EMERGING_CLUSTER_MIN_RANK} and "
            f"{metadata.get('strong_ticker_count', 'n/a')} strong movers "
            f"(minimum {EMERGING_CLUSTER_MIN_STRONG_TICKERS})."
        )
        evidence.append(
            f"Coverage: {metadata.get('eligible_count', 'n/a')} eligible contributors; "
            f"eligible coverage {_ratio_pct(metadata.get('eligible_coverage_ratio'))} "
            f"(minimum {_ratio_pct(EMERGING_CLUSTER_MIN_ELIGIBLE_COVERAGE_RATIO)}); "
            f"strong coverage {_ratio_pct(metadata.get('strong_coverage_ratio'))} "
            f"(minimum {_ratio_pct(EMERGING_CLUSTER_MIN_STRONG_COVERAGE_RATIO)})."
        )
        if metadata.get("theme_move_vs_qqq_pct") is not None:
            evidence.append(f"Theme move vs QQQ 1D: {_pct(metadata.get('theme_move_vs_qqq_pct'))}.")
    elif signal_type == "Weakening / Narrowing Theme":
        evidence.append(f"Trigger: top-{WEAKENING_TOP_RANK_MAX} theme with weak participation evidence.")
        evidence.append(
            f"Eligible contributors: {metadata.get('eligible_count', 'n/a')} "
            f"(thin threshold <= {WEAKENING_THIN_ELIGIBLE_MAX}); "
            f"1W breadth {_pct(metadata.get('positive_1w_breadth_pct'))} "
            f"(weak threshold <= {_pct(WEAKENING_MAX_POSITIVE_BREADTH_PCT)}); "
            f"1W average {_pct(metadata.get('theme_avg_1w'))}."
        )

    return evidence


def _normalize_signal(signal: dict[str, object] | ThemePatternSignal) -> dict[str, object]:
    if isinstance(signal, ThemePatternSignal):
        return signal.to_dict()
    return dict(signal or {})


def _build_overlap_scout_items(
    signals: list[dict[str, object]],
    qqq_market_context: dict[str, object] | None,
) -> list[MarketScoutItem]:
    ticker_map: dict[str, set[int]] = {}
    theme_map: dict[str, set[str]] = {}
    for idx, signal in enumerate(signals):
        theme = str(signal.get("theme") or "").strip()
        for ticker in signal.get("tickers") or []:
            normalized = str(ticker or "").strip().upper()
            if not normalized:
                continue
            ticker_map.setdefault(normalized, set()).add(idx)
            theme_map.setdefault(normalized, set()).add(theme)

    overlapping_tickers = [
        ticker
        for ticker, indices in ticker_map.items()
        if len(indices) >= 2 and len({theme for theme in theme_map.get(ticker, set()) if theme}) >= SCOUT_OVERLAP_MIN_THEMES
    ]
    if not overlapping_tickers:
        return []

    family_groups: dict[str, list[str]] = {}
    ticker_family: dict[str, str] = {}
    ticker_family_context: dict[str, list[str]] = {}
    for ticker in overlapping_tickers:
        ticker_themes = sorted({theme for theme in theme_map.get(ticker, set()) if theme})
        coherence = _overlap_theme_coherence(ticker_themes)
        family = str(coherence.get("family") or "Mixed")
        if not bool(coherence.get("coherent")):
            family = "Mixed"
        ticker_family[ticker] = family
        ticker_family_context[ticker] = ticker_themes
        if family != "Mixed":
            family_groups.setdefault(family, []).append(ticker)

    best_family = ""
    best_family_tickers: list[str] = []
    if family_groups:
        best_family, best_family_tickers = sorted(
            family_groups.items(),
            key=lambda item: (-len(item[1]), item[0]),
        )[0]
    family_purity = len(best_family_tickers) / max(len(overlapping_tickers), 1)
    coherent_tickers = [ticker for ticker in overlapping_tickers if ticker in set(best_family_tickers)]
    coherent_enough = (
        bool(best_family)
        and len(coherent_tickers) >= SCOUT_OVERLAP_MIN_COHERENT_TICKERS
        and family_purity >= SCOUT_OVERLAP_MIN_FAMILY_PURITY
    )

    items: list[MarketScoutItem] = []
    if coherent_enough:
        items.append(
            _overlap_cluster_item(
                signals,
                qqq_market_context,
                coherent_tickers,
                family=best_family,
                ticker_family=ticker_family,
                ticker_family_context=ticker_family_context,
            )
        )

    mixed_tickers = [ticker for ticker in overlapping_tickers if ticker not in set(coherent_tickers)] if coherent_enough else overlapping_tickers
    if mixed_tickers:
        items.append(
            _mixed_overlap_warning_item(
                signals,
                mixed_tickers,
                ticker_family=ticker_family,
                ticker_family_context=ticker_family_context,
            )
        )
    return items


def _overlap_cluster_item(
    signals: list[dict[str, object]],
    qqq_market_context: dict[str, object] | None,
    overlapping_tickers: list[str],
    *,
    family: str,
    ticker_family: dict[str, str],
    ticker_family_context: dict[str, list[str]],
) -> MarketScoutItem:
    signal_indices = _signal_indices_for_tickers(signals, overlapping_tickers)
    themes = sorted({str(signals[idx].get("theme") or "").strip() for idx in signal_indices if signals[idx].get("theme")})
    market_clause = _market_context_clause(qqq_market_context, item_kind="cluster")
    ticker_text = _join_names(overlapping_tickers[:5])
    theme_text = _join_names(themes[:5])
    why = (
        f"{ticker_text} are repeated across {theme_text} and mostly map to {family}. "
        "Treat this as one underlying cluster, not several independent theme signals."
    )
    if market_clause:
        why = f"{why} {market_clause}"
    priority = 240.0 + len(signal_indices) * 8.0 + len(overlapping_tickers) * 5.0
    return MarketScoutItem(
        headline=f"{family} overlap",
        pattern="Coherent Overlap Cluster",
        why_it_matters=why,
        tickers_to_inspect=tuple(overlapping_tickers[:6]),
        next_look=f"Review the shared tickers first, then confirm which of {theme_text} best represents the underlying cluster.",
        priority=priority,
        signal_indices=tuple(signal_indices),
        metadata={
            "themes": themes,
            "overlap_ticker_count": len(overlapping_tickers),
            "quality_note": False,
            "coherent_overlap": True,
            "overlap_family": family,
            "coherence_confidence": "strong",
            "ticker_family": ticker_family,
            "ticker_family_context": ticker_family_context,
        },
    )


def _mixed_overlap_warning_item(
    signals: list[dict[str, object]],
    overlapping_tickers: list[str],
    *,
    ticker_family: dict[str, str],
    ticker_family_context: dict[str, list[str]],
) -> MarketScoutItem:
    signal_indices = _signal_indices_for_tickers(signals, overlapping_tickers)
    themes = sorted({str(signals[idx].get("theme") or "").strip() for idx in signal_indices if signals[idx].get("theme")})
    theme_text = _join_names(themes[:5])
    why = (
        "Several strong tickers are repeated across flagged themes, but they do not form one clean theme cluster. "
        "Treat this as an overlap warning and review the names in their proper theme contexts."
    )
    return MarketScoutItem(
        headline="Mixed repeated leaders",
        pattern="Overlap Warning",
        why_it_matters=why,
        tickers_to_inspect=tuple(overlapping_tickers[:6]),
        next_look=f"Review the repeated names first, then confirm whether {theme_text} have broader participation beyond those names.",
        priority=105.0 + min(len(signal_indices), 6) * 4.0 + min(len(overlapping_tickers), 6) * 3.0,
        signal_indices=tuple(signal_indices),
        metadata={
            "themes": themes,
            "overlap_ticker_count": len(overlapping_tickers),
            "quality_note": True,
            "coherent_overlap": False,
            "overlap_family": "Mixed",
            "coherence_confidence": "weak",
            "ticker_family": ticker_family,
            "ticker_family_context": ticker_family_context,
        },
    )


def _signal_indices_for_tickers(signals: list[dict[str, object]], tickers: list[str]) -> list[int]:
    ticker_set = {str(ticker).strip().upper() for ticker in tickers if str(ticker).strip()}
    return sorted(
        idx
        for idx, signal in enumerate(signals)
        if ticker_set.intersection({str(ticker).strip().upper() for ticker in signal.get("tickers") or []})
    )


def _build_grouped_broad_thrust_item(
    signals: list[dict[str, object]],
    qqq_market_context: dict[str, object] | None,
    *,
    skip_indices: set[int],
) -> MarketScoutItem | None:
    broad = [
        (idx, signal)
        for idx, signal in enumerate(signals)
        if idx not in skip_indices and signal.get("signal_type") == "Broad Same-Theme Thrust"
    ]
    if not broad:
        return None

    sorted_broad = sorted(
        broad,
        key=lambda pair: -float((pair[1].get("priority") or 0.0)),
    )
    signal_indices = tuple(idx for idx, _signal in sorted_broad)
    themes = [str(signal.get("theme") or "").strip() for _idx, signal in sorted_broad if signal.get("theme")]
    tickers = _unique_tickers([signal for _idx, signal in sorted_broad], limit=8)
    market_clause = _market_context_clause(qqq_market_context, item_kind="broad")
    widespread = len(sorted_broad) >= SCOUT_WIDESPREAD_BROAD_THRUST_MIN_SIGNALS
    if len(sorted_broad) == 1:
        headline = f"{themes[0]} broad thrust" if themes else "Broad same-theme thrust"
        why = str(sorted_broad[0][1].get("why_notable") or "Multiple tickers are moving together inside one theme.")
        next_look = "Inspect whether the listed tickers are confirming each other or just reacting to broad tape."
    elif widespread:
        headline = "Broad thrust is widespread"
        why = (
            f"{len(sorted_broad)} themes have broad-thrust triggers, so this is better treated as market-wide breadth context "
            "than a single primary scout target."
        )
        next_look = "Use this as backdrop, then prioritize the more specific overlap, outlier, and emerging-cluster items."
    else:
        headline = f"{len(sorted_broad)} themes showing broad thrust"
        why = f"{len(sorted_broad)} themes have multiple strong movers, making this a focused breadth scan rather than isolated ticker noise."
        next_look = f"Start with the highest-overlap tickers and then compare theme breadth: {', '.join(themes[:4])}."
    if market_clause:
        why = f"{why} {market_clause}"

    return MarketScoutItem(
        headline=headline,
        pattern="Broad Same-Theme Thrust",
        why_it_matters=why,
        tickers_to_inspect=tuple(tickers),
        next_look=next_look,
        priority=(
            60.0
            if widespread
            else 150.0 + sum(float(signal.get("priority") or 0.0) for _idx, signal in sorted_broad[:3]) / 5.0
        ),
        signal_indices=signal_indices,
        metadata={"themes": themes, "grouped_signal_count": len(sorted_broad), "widespread": widespread},
    )


def _signal_to_scout_item(
    idx: int,
    signal: dict[str, object],
    qqq_market_context: dict[str, object] | None,
) -> MarketScoutItem | None:
    signal_type = str(signal.get("signal_type") or "Pattern")
    theme = str(signal.get("theme") or "").strip()
    tickers = tuple(_unique_tickers([signal], limit=6))
    market_clause = _market_context_clause(
        qqq_market_context,
        item_kind="cluster" if signal_type == "Emerging Cluster" else "single",
    )
    why = str(signal.get("why_notable") or signal.get("read") or "Deterministic audit signal fired.")
    if market_clause:
        why = f"{why} {market_clause}"
    metadata = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
    why = _scout_item_why(signal_type, why, metadata, market_clause)
    next_look = _next_look_for_signal(signal_type, theme, tickers, signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {})
    return MarketScoutItem(
        headline=f"{theme}: {signal_type}" if theme else signal_type,
        pattern=signal_type,
        why_it_matters=why,
        tickers_to_inspect=tickers,
        next_look=next_look,
        priority=_scout_item_priority(signal),
        signal_indices=(idx,),
        metadata={"theme": theme},
    )


def _scout_item_priority(signal: dict[str, object]) -> float:
    signal_type = str(signal.get("signal_type") or "")
    raw_priority = float(signal.get("priority") or 0.0)
    metadata = signal.get("metadata") if isinstance(signal.get("metadata"), dict) else {}
    if signal_type == "Outlier-Led Theme (Broadly Confirmed)":
        return 210.0 + raw_priority * 0.25
    if signal_type == "Emerging Cluster":
        return 140.0 + raw_priority * 0.25
    if signal_type == "Outlier-Led Theme (Narrow/Fragile)":
        excess = _safe_float(metadata.get("excess_vs_avg_pct")) or _safe_float(metadata.get("excess_pct")) or 0.0
        strong_count = int(metadata.get("strong_ticker_count") or 0)
        supporting = len([ticker for ticker in (metadata.get("supporting_tickers") or []) if str(ticker).strip()])
        material_bonus = 55.0 if excess >= 85.0 else 0.0
        support_bonus = min(strong_count + supporting, 4) * 8.0
        return 82.0 + min(raw_priority, 120.0) * 0.12 + material_bonus + support_bonus
    if signal_type == "Extreme Ticker Standout":
        return 95.0 + raw_priority * 0.15
    if signal_type == "Weakening / Narrowing Theme":
        return 70.0 + raw_priority * 0.10
    return raw_priority


def _scout_item_why(
    signal_type: str,
    fallback: str,
    metadata: dict[str, object],
    market_clause: str,
) -> str:
    if signal_type == "Outlier-Led Theme (Narrow/Fragile)":
        leader = str(metadata.get("leader") or "").strip().upper()
        leader_text = f"{leader} is" if leader else "One ticker is"
        return (
            f"{leader_text} doing most of the work, so the move may be distorting the theme read. "
            "Treat it as a watch item until other constituents confirm breadth."
        )
    if signal_type == "Outlier-Led Theme (Broadly Confirmed)":
        leader = str(metadata.get("leader") or "").strip().upper()
        leader_text = f"{leader} is the extreme leader" if leader else "The lead ticker is the extreme leader"
        supporting = [
            str(ticker).strip().upper()
            for ticker in (metadata.get("supporting_tickers") or [])
            if str(ticker).strip()
        ]
        if supporting:
            support_text = _join_names(supporting[:4])
            verb = "are" if len(supporting[:4]) != 1 else "is"
            return (
                f"{leader_text}, but {support_text} {verb} also participating. "
                "Verify those support names before treating this as a durable group read."
            )
        return (
            f"{leader_text}, and supporting names are participating too. "
            "Verify breadth before treating this as a durable group read."
        )
    if market_clause:
        return f"{fallback}"
    return fallback


def _next_look_for_signal(signal_type: str, theme: str, tickers: tuple[str, ...], metadata: dict[str, object] | None = None) -> str:
    ticker_text = ", ".join(tickers) if tickers else "the theme constituents"
    metadata = metadata or {}
    if signal_type == "Emerging Cluster":
        return f"Check whether {ticker_text} are rotating into leadership before the theme reaches the top cohort."
    if signal_type.startswith("Outlier-Led Theme"):
        supporting = [
            str(ticker).strip().upper()
            for ticker in (metadata.get("supporting_tickers") or [])
            if str(ticker).strip()
        ]
        if supporting:
            return f"Inspect {ticker_text} first, then compare supporting tickers {', '.join(supporting[:4])} for breadth confirmation."
        return f"Inspect {ticker_text} first, then check whether other constituents are participating or lagging."
    if signal_type == "Weakening / Narrowing Theme":
        return f"Review breadth and contributor quality inside {theme or 'the theme'} before trusting the headline rank."
    if signal_type == "Extreme Ticker Standout":
        return f"Start with {ticker_text}; decide whether the ticker is a real leader or a one-name distortion."
    return f"Inspect {ticker_text} for confirmation, liquidity, and follow-through."


def _join_names(values: list[str] | tuple[str, ...]) -> str:
    names = [str(value).strip() for value in values if str(value).strip()]
    if not names:
        return "-"
    if len(names) == 1:
        return names[0]
    if len(names) == 2:
        return f"{names[0]} and {names[1]}"
    return f"{', '.join(names[:-1])}, and {names[-1]}"


def _overlap_theme_coherence(themes: list[str]) -> dict[str, object]:
    token_counts: dict[str, int] = {}
    all_tokens: set[str] = set()
    for theme in themes:
        tokens = _theme_tokens(theme)
        all_tokens.update(tokens)
        for token in tokens:
            token_counts[token] = token_counts.get(token, 0) + 1

    theme_token_sets = [_theme_tokens(theme) for theme in themes]
    best_family = None
    best_hits = 0
    best_theme_hits = 0
    for family, keywords in OVERLAP_FAMILY_KEYWORDS.items():
        hits = len(all_tokens.intersection(keywords))
        theme_hits = sum(1 for tokens in theme_token_sets if tokens.intersection(keywords))
        distinctive_hits = len(all_tokens.intersection(keywords - OVERLAP_GENERIC_SHARED_TOKENS))
        if distinctive_hits <= 0:
            continue
        if (theme_hits, hits) > (best_theme_hits, best_hits):
            best_family = family
            best_hits = hits
            best_theme_hits = theme_hits
    if best_family and best_hits >= 2 and best_theme_hits >= 2:
        return {
            "coherent": True,
            "family": best_family,
            "shared_tokens": sorted(all_tokens.intersection(OVERLAP_FAMILY_KEYWORDS[best_family])),
            "confidence": "strong",
        }

    shared_tokens = sorted(token for token, count in token_counts.items() if count >= 2)
    specific_shared_tokens = [token for token in shared_tokens if token not in OVERLAP_GENERIC_SHARED_TOKENS]
    if specific_shared_tokens:
        return {
            "coherent": True,
            "family": specific_shared_tokens[0].replace("_", " ").title(),
            "shared_tokens": shared_tokens,
            "confidence": "strong",
        }

    return {"coherent": False, "family": "Mixed", "shared_tokens": shared_tokens, "confidence": "weak"}


def _theme_tokens(theme: str) -> set[str]:
    cleaned = "".join(ch.lower() if ch.isalnum() else " " for ch in str(theme or ""))
    return {
        token
        for token in cleaned.split()
        if (len(token) >= 3 or token == "ai") and token not in OVERLAP_THEME_STOPWORDS
    }


def _market_context_clause(qqq_market_context: dict[str, object] | None, *, item_kind: str) -> str:
    if not qqq_market_context:
        return ""
    move_label = str(qqq_market_context.get("move_label") or "").strip()
    character_tag = str(qqq_market_context.get("character_tag") or "").strip()
    qqq_pct = _safe_float(qqq_market_context.get("qqq_pct_change"))
    qqq_text = _pct(qqq_pct) if qqq_pct is not None else "weak"
    if move_label in {DOWN_DAY, STRONG_DOWN_DAY}:
        return f"Against a {qqq_text} QQQ tape, this strength stands out."
    if character_tag == QUIET and item_kind in {"cluster", "broad"}:
        return "Quiet QQQ tape makes this clustered strength look more group-specific."
    if move_label == STRONG_UP_DAY or character_tag == TREND_UP:
        return "Strong QQQ tape raises the bar; prefer names that clearly outperform the broad Nasdaq bid."
    if character_tag in {VOLATILE_CHOP, FADE, VOLATILE_FADE}:
        return "Choppy or fading QQQ tape means follow-through matters more than the first signal."
    return ""


def _unique_tickers(signals: list[dict[str, object]], *, limit: int) -> list[str]:
    out: list[str] = []
    for signal in signals:
        for ticker in signal.get("tickers") or []:
            normalized = str(ticker or "").strip().upper()
            if normalized and normalized not in out:
                out.append(normalized)
            if len(out) >= int(limit):
                return out
    return out


def build_theme_pattern_signals(
    constituents: pd.DataFrame,
    theme_metrics: pd.DataFrame,
    rankings: pd.DataFrame,
    *,
    limit: int = DEFAULT_SIGNAL_LIMIT,
    qqq_market_context: dict[str, object] | None = None,
) -> list[ThemePatternSignal]:
    prepared = _prepare_constituents(constituents)
    metrics = _prepare_theme_metrics(theme_metrics, rankings)
    if prepared.empty or metrics.empty:
        return []

    signals: list[ThemePatternSignal] = []
    signals.extend(_broad_same_theme_thrust(prepared, metrics))
    signals.extend(_extreme_ticker_standouts(prepared, metrics))
    signals.extend(_outlier_led_themes(prepared, metrics))
    signals.extend(_emerging_clusters(prepared, metrics))
    signals.extend(_weakening_or_narrowing_themes(metrics))
    signals = _apply_market_context(signals, qqq_market_context)

    if not signals:
        return []

    ordered = _dedupe_theme_signals(signals)
    selected: list[ThemePatternSignal] = []
    theme_counts: dict[str, int] = {}
    for signal in ordered:
        theme_key = signal.theme.lower().strip()
        if theme_counts.get(theme_key, 0) >= MAX_SIGNALS_PER_THEME:
            continue
        theme_counts[theme_key] = theme_counts.get(theme_key, 0) + 1
        selected.append(signal)
        if len(selected) >= max(int(limit), 0):
            break
    return selected


def _dedupe_theme_signals(signals: list[ThemePatternSignal]) -> list[ThemePatternSignal]:
    by_theme: dict[str, list[ThemePatternSignal]] = {}
    for signal in signals:
        by_theme.setdefault(signal.theme.lower().strip(), []).append(signal)

    retained: list[ThemePatternSignal] = []
    for theme_signals in by_theme.values():
        type_deduped: dict[str, ThemePatternSignal] = {}
        for signal in theme_signals:
            existing = type_deduped.get(signal.signal_type)
            if existing is None or _signal_sort_key(signal) < _signal_sort_key(existing):
                type_deduped[signal.signal_type] = signal
        candidates = sorted(type_deduped.values(), key=_signal_sort_key)

        has_broad = any(signal.signal_type == "Broad Same-Theme Thrust" for signal in candidates)
        has_outlier = any(signal.signal_type.startswith("Outlier-Led Theme") for signal in candidates)
        filtered: list[ThemePatternSignal] = []
        for signal in candidates:
            if signal.signal_type == "Extreme Ticker Standout" and (has_broad or has_outlier):
                continue
            filtered.append(signal)
        retained.extend(filtered)

    return sorted(retained, key=_signal_sort_key)


def _signal_sort_key(signal: ThemePatternSignal) -> tuple[float, float, int, str, str]:
    return (
        -float(SIGNAL_TYPE_PRIORITY.get(signal.signal_type, 0)),
        -float(signal.priority),
        int(signal.metadata.get("rank") or 9999),
        signal.theme,
        ",".join(signal.tickers),
    )


def _apply_market_context(
    signals: list[ThemePatternSignal],
    qqq_market_context: dict[str, object] | None,
) -> list[ThemePatternSignal]:
    if not signals or not qqq_market_context:
        return signals

    tape_read = _market_context_bucket(qqq_market_context)
    qqq_return_1d = _safe_float(qqq_market_context.get("qqq_pct_change"))
    if not tape_read or qqq_return_1d is None:
        return signals

    adjusted: list[ThemePatternSignal] = []
    for signal in signals:
        metadata = dict(signal.metadata or {})
        priority = float(signal.priority)
        read_parts = [signal.read]
        market_note = ""

        theme_avg_1w = _safe_float(metadata.get("theme_avg_1w"))
        if theme_avg_1w is not None:
            theme_move_vs_qqq = round(theme_avg_1w - qqq_return_1d, 2)
            metadata["theme_move_vs_qqq_pct"] = theme_move_vs_qqq
        else:
            theme_move_vs_qqq = None

        ticker_perf = _first_present(metadata.get("ticker_perf"), metadata.get("leader_perf_1m"))
        if ticker_perf is not None:
            metadata["ticker_move_vs_qqq_pct"] = round(float(ticker_perf) - qqq_return_1d, 2)

        if signal.signal_type == "Broad Same-Theme Thrust":
            strong_count = int(metadata.get("strong_ticker_count") or 0)
            if tape_read == "Trend Down Day":
                priority += MARKET_CONTEXT_DOWN_TAPE_BOOST
                market_note = (
                    f"QQQ is {tape_read} ({_pct(qqq_return_1d)}), so broad positive thrust is relative strength."
                )
                read_parts.append("Inspect the listed tickers first because this is relative strength against weak Nasdaq tape.")
            elif tape_read == "Quiet Sideways Day":
                priority += MARKET_CONTEXT_QUIET_TAPE_BOOST
                market_note = (
                    f"QQQ is {tape_read} ({_pct(qqq_return_1d)}), so multiple strong movers point to group-specific activity."
                )
                read_parts.append("Inspect the group cluster for theme-specific sponsorship.")
            elif tape_read == "Trend Up Day":
                strongly_outperforming = theme_move_vs_qqq is not None and theme_move_vs_qqq >= MARKET_CONTEXT_STRONG_OUTPERFORMANCE_PCT
                unusually_clustered = strong_count >= MARKET_CONTEXT_CLUSTER_STRONG_TICKER_COUNT
                if not (strongly_outperforming or unusually_clustered):
                    priority -= MARKET_CONTEXT_GENERIC_UP_TAPE_PENALTY
                    market_note = (
                        f"QQQ is {tape_read} ({_pct(qqq_return_1d)}); broad thrust may partly reflect a Nasdaq tailwind."
                    )
                    read_parts.append("Treat as lower priority because this may partly reflect a Nasdaq tailwind unless follow-through separates from the tape.")
                else:
                    market_note = (
                        f"QQQ is {tape_read}, but this theme still shows strong internal clustering or outperformance."
                    )
                    read_parts.append("Inspect for leadership that is stronger than the broad Nasdaq bid.")
            elif tape_read == "Wide Choppy Day":
                market_note = f"QQQ is {tape_read}; expect noisy confirmation until price action tightens."
                read_parts.append("Use extra caution because broad tape is volatile and two-sided.")

        elif signal.signal_type == "Emerging Cluster":
            if tape_read == "Quiet Sideways Day":
                priority += MARKET_CONTEXT_QUIET_TAPE_BOOST
                market_note = (
                    f"QQQ is {tape_read} ({_pct(qqq_return_1d)}), so the lower-ranked cluster looks more theme-specific."
                )
                read_parts.append("Inspect whether this theme-specific cluster is starting to rotate into leadership.")
            elif tape_read == "Trend Down Day":
                priority += MARKET_CONTEXT_DOWN_TAPE_BOOST * 0.5
                market_note = (
                    f"QQQ is {tape_read}; emerging strength is notable if it persists against weak tape."
                )
                read_parts.append("Inspect the tickers for relative strength durability.")
            elif tape_read == "Wide Choppy Day":
                market_note = f"QQQ is {tape_read}; cluster evidence needs cleaner follow-through."
                read_parts.append("Use caution because choppy tape can exaggerate intraday clusters.")

        elif tape_read == "Wide Choppy Day" and signal.signal_type in {
            "Extreme Ticker Standout",
            "Outlier-Led Theme (Broadly Confirmed)",
            "Outlier-Led Theme (Narrow/Fragile)",
        }:
            market_note = f"QQQ is {tape_read}; standalone strength may need confirmation."
            read_parts.append("Use caution and verify follow-through before treating the move as clean leadership.")

        if market_note:
            metadata["market_tape_read"] = tape_read
            metadata["qqq_return_1d_pct"] = qqq_return_1d
            metadata["market_context_note"] = market_note
            metadata["market_context_priority_adjustment"] = round(priority - float(signal.priority), 2)

        adjusted.append(
            ThemePatternSignal(
                signal_type=signal.signal_type,
                theme=signal.theme,
                tickers=signal.tickers,
                why_notable=signal.why_notable,
                read=" ".join(part.strip() for part in read_parts if part and str(part).strip()),
                priority=priority,
                metadata=metadata,
            )
        )

    return adjusted


def _market_context_bucket(qqq_market_context: dict[str, object]) -> str:
    character_tag = str(qqq_market_context.get("character_tag") or "").strip()
    move_label = str(qqq_market_context.get("move_label") or "").strip()
    if character_tag == QUIET:
        return "Quiet Sideways Day"
    if character_tag == VOLATILE_CHOP:
        return "Wide Choppy Day"
    if character_tag == TREND_UP or move_label in {UP_DAY, STRONG_UP_DAY}:
        return "Trend Up Day"
    if character_tag == TREND_DOWN or move_label in {DOWN_DAY, STRONG_DOWN_DAY}:
        return "Trend Down Day"
    return character_tag or move_label


def _prepare_constituents(constituents: pd.DataFrame) -> pd.DataFrame:
    if constituents.empty:
        return pd.DataFrame()
    out = constituents.copy()
    for col in ("theme_id", "run_id", "price", "avg_volume", "perf_1d", "perf_1w", "perf_1m", "perf_3m"):
        if col not in out.columns:
            out[col] = np.nan
        out[col] = pd.to_numeric(out[col], errors="coerce")
    for col in ("theme", "ticker", "status"):
        if col not in out.columns:
            out[col] = ""
    if "is_active" not in out.columns:
        out["is_active"] = True
    out["theme"] = out["theme"].fillna("").astype(str)
    out["ticker"] = out["ticker"].fillna("").astype(str).str.upper().str.strip()
    out["status"] = out["status"].fillna("active").astype(str)
    out["is_active"] = out["is_active"].astype("boolean").fillna(True).astype(bool)
    out["eligible"] = out.apply(
        lambda row: current_ticker_is_eligible(
            row.get("price"),
            row.get("avg_volume"),
            row.get("status"),
            snapshot_present=pd.notna(row.get("run_id")) if "run_id" in out.columns else True,
        ),
        axis=1,
    )
    out = out[out["is_active"] & out["eligible"] & out["ticker"].ne("") & out["theme"].ne("")].copy()
    out["dollar_volume"] = out["price"] * out["avg_volume"]
    out["strong_current_move"] = (
        out["perf_1d"].fillna(-np.inf).ge(BROAD_THRUST_1D_PCT)
        | out["perf_1w"].fillna(-np.inf).ge(BROAD_THRUST_1W_PCT)
    )
    return out


def _prepare_theme_metrics(theme_metrics: pd.DataFrame, rankings: pd.DataFrame) -> pd.DataFrame:
    if theme_metrics.empty:
        return pd.DataFrame()
    metrics = theme_metrics.copy()
    for col in (
        "theme_id",
        "ticker_count",
        "eligible_ticker_count",
        "eligible_standardized_count",
        "avg_1w",
        "avg_1m",
        "avg_3m",
        "positive_1w_breadth_pct",
        "positive_1m_breadth_pct",
        "standardized_composite_score",
        "current_momentum_score",
    ):
        if col not in metrics.columns:
            metrics[col] = np.nan
        metrics[col] = pd.to_numeric(metrics[col], errors="coerce")
    for col in ("theme", "category"):
        if col not in metrics.columns:
            metrics[col] = ""
    if "is_active" not in metrics.columns:
        metrics["is_active"] = True
    metrics["is_active"] = metrics["is_active"].astype("boolean").fillna(True).astype(bool)
    metrics = metrics[metrics["is_active"]].copy()

    rank_lookup = _rank_lookup(rankings)
    metrics["rank"] = metrics["theme_id"].map(rank_lookup)
    if metrics["rank"].isna().all():
        score_col = "standardized_composite_score" if "standardized_composite_score" in metrics.columns else "current_momentum_score"
        metrics = metrics.sort_values([score_col, "theme"], ascending=[False, True]).copy()
        metrics["rank"] = range(1, len(metrics) + 1)
    return metrics


def _rank_lookup(rankings: pd.DataFrame) -> dict[float, int]:
    if rankings.empty or "theme_id" not in rankings.columns:
        return {}
    ranked = rankings.copy().reset_index(drop=True)
    if "rank" not in ranked.columns:
        ranked["rank"] = ranked.index + 1
    ranked["theme_id"] = pd.to_numeric(ranked["theme_id"], errors="coerce")
    ranked["rank"] = pd.to_numeric(ranked["rank"], errors="coerce")
    ranked = ranked.dropna(subset=["theme_id", "rank"])
    return {float(row["theme_id"]): int(row["rank"]) for _, row in ranked.iterrows()}


def _theme_row(metrics: pd.DataFrame, theme_id: object) -> pd.Series | None:
    rows = metrics[metrics["theme_id"] == theme_id]
    if rows.empty:
        return None
    return rows.iloc[0]


def _top_tickers(rows: pd.DataFrame, metric: str, limit: int = 4) -> tuple[str, ...]:
    if rows.empty:
        return tuple()
    ranked = rows.copy()
    ranked[metric] = pd.to_numeric(ranked[metric], errors="coerce")
    ranked = ranked.sort_values([metric, "ticker"], ascending=[False, True])
    return tuple(ranked["ticker"].head(limit).tolist())


def _pct(value: object) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{float(value):.1f}%"


def _ratio_pct(value: object) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{float(value) * 100.0:.0f}%"


def _broad_same_theme_thrust(prepared: pd.DataFrame, metrics: pd.DataFrame) -> list[ThemePatternSignal]:
    signals: list[ThemePatternSignal] = []
    for theme_id, rows in prepared.groupby("theme_id"):
        strong = rows[rows["strong_current_move"]].copy()
        if len(strong) < BROAD_THRUST_MIN_TICKERS:
            continue
        theme = _theme_row(metrics, theme_id)
        if theme is None:
            continue
        tickers = _top_tickers(strong, "perf_1w")
        priority = 80.0 + len(strong) * 4.0 + max(float(theme.get("avg_1w") or 0.0), 0.0)
        signals.append(
            ThemePatternSignal(
                signal_type="Broad Same-Theme Thrust",
                theme=str(theme.get("theme") or rows.iloc[0]["theme"]),
                tickers=tickers,
                why_notable=f"{len(strong)} eligible members have strong 1D or 1W moves; theme 1W avg {_pct(theme.get('avg_1w'))}.",
                read="Broad current participation is confirming the theme move.",
                priority=priority,
                metadata={
                    "theme_id": int(theme_id),
                    "rank": _safe_int(theme.get("rank")),
                    "strong_ticker_count": int(len(strong)),
                    "theme_avg_1w": _safe_float(theme.get("avg_1w")),
                },
            )
        )
    return signals


def _extreme_ticker_standouts(prepared: pd.DataFrame, metrics: pd.DataFrame) -> list[ThemePatternSignal]:
    signals: list[ThemePatternSignal] = []
    merged = prepared.merge(
        metrics[["theme_id", "theme", "rank", "avg_1w", "avg_1m"]],
        on="theme_id",
        how="inner",
        suffixes=("", "_theme"),
    )
    merged["excess_1w"] = merged["perf_1w"] - merged["avg_1w"]
    merged["excess_1m"] = merged["perf_1m"] - merged["avg_1m"]
    merged["standout_excess"] = merged[["excess_1w", "excess_1m"]].max(axis=1)
    candidates = merged[merged["standout_excess"].ge(EXTREME_STANDOUT_EXCESS_PCT)].copy()
    if candidates.empty:
        return signals
    candidates = candidates.sort_values(["standout_excess", "perf_1m", "ticker"], ascending=[False, False, True])
    for _, row in candidates.iterrows():
        window = "1M" if pd.notna(row.get("excess_1m")) and float(row.get("excess_1m")) >= float(row.get("excess_1w") or -np.inf) else "1W"
        theme_avg = row.get("avg_1m") if window == "1M" else row.get("avg_1w")
        ticker_perf = row.get("perf_1m") if window == "1M" else row.get("perf_1w")
        signals.append(
            ThemePatternSignal(
                signal_type="Extreme Ticker Standout",
                theme=str(row.get("theme_theme") or row.get("theme") or ""),
                tickers=(str(row["ticker"]),),
                why_notable=f"{row['ticker']} {window} {_pct(ticker_perf)} versus theme avg {_pct(theme_avg)}.",
                read="Single-name strength is materially ahead of the theme basket.",
                priority=70.0 + float(row["standout_excess"]),
                metadata={
                    "theme_id": _safe_int(row.get("theme_id")),
                    "rank": _safe_int(row.get("rank")),
                    "window": window,
                    "ticker_perf": _safe_float(ticker_perf),
                    "theme_avg": _safe_float(theme_avg),
                    "excess_pct": round(float(row["standout_excess"]), 2),
                },
            )
        )
    return signals


def _outlier_led_themes(prepared: pd.DataFrame, metrics: pd.DataFrame) -> list[ThemePatternSignal]:
    signals: list[ThemePatternSignal] = []
    for theme_id, rows in prepared.groupby("theme_id"):
        if len(rows) < 2:
            continue
        theme = _theme_row(metrics, theme_id)
        if theme is None:
            continue
        sorted_rows = rows.sort_values(["perf_1m", "ticker"], ascending=[False, True])
        leader = sorted_rows.iloc[0]
        supporting_tickers = tuple(
            sorted_rows.iloc[1:5]["ticker"].dropna().astype(str).str.upper().str.strip().tolist()
        )
        second_perf = sorted_rows.iloc[1]["perf_1m"] if len(sorted_rows) > 1 else np.nan
        avg_1m = float(theme.get("avg_1m")) if pd.notna(theme.get("avg_1m")) else np.nan
        excess_vs_avg = float(leader["perf_1m"] - avg_1m) if pd.notna(avg_1m) and pd.notna(leader["perf_1m"]) else np.nan
        gap_vs_next = float(leader["perf_1m"] - second_perf) if pd.notna(second_perf) and pd.notna(leader["perf_1m"]) else np.nan
        if (
            pd.isna(excess_vs_avg)
            or pd.isna(gap_vs_next)
            or excess_vs_avg < OUTLIER_STANDOUT_EXCESS_PCT
            or avg_1m < OUTLIER_MIN_THEME_AVG_PCT
        ):
            continue
        strong_count = int(rows["strong_current_move"].sum())
        broad = strong_count >= OUTLIER_BROAD_CONFIRMATION_MIN_TICKERS
        signal_type = "Outlier-Led Theme (Broadly Confirmed)" if broad else "Outlier-Led Theme (Narrow/Fragile)"
        read = "Outlier is leading, but breadth still confirms the theme." if broad else "Outlier is carrying a narrow, fragile theme read."
        signals.append(
            ThemePatternSignal(
                signal_type=signal_type,
                theme=str(theme.get("theme") or rows.iloc[0]["theme"]),
                tickers=(str(leader["ticker"]),),
                why_notable=f"{leader['ticker']} 1M {_pct(leader['perf_1m'])}; next member {_pct(second_perf)} and theme avg {_pct(avg_1m)}.",
                read=read,
                priority=65.0 + excess_vs_avg + (8.0 if not broad else 0.0),
                metadata={
                    "theme_id": int(theme_id),
                    "rank": _safe_int(theme.get("rank")),
                    "broad_confirmation": broad,
                    "strong_ticker_count": strong_count,
                    "leader_perf_1m": _safe_float(leader["perf_1m"]),
                    "next_perf_1m": _safe_float(second_perf),
                    "theme_avg_1m": _safe_float(avg_1m),
                    "excess_vs_avg_pct": round(float(excess_vs_avg), 2),
                    "supporting_tickers": supporting_tickers,
                },
            )
        )
    return signals


def _emerging_clusters(prepared: pd.DataFrame, metrics: pd.DataFrame) -> list[ThemePatternSignal]:
    signals: list[ThemePatternSignal] = []
    rank_by_theme = metrics.set_index("theme_id")["rank"].to_dict()
    for theme_id, rows in prepared.groupby("theme_id"):
        rank = rank_by_theme.get(theme_id)
        if pd.isna(rank) or int(rank) < EMERGING_CLUSTER_MIN_RANK:
            continue
        strong = rows[rows["strong_current_move"]].copy()
        if len(strong) < EMERGING_CLUSTER_MIN_STRONG_TICKERS:
            continue
        theme = _theme_row(metrics, theme_id)
        if theme is None:
            continue
        eligible_count = int(theme.get("eligible_standardized_count") or theme.get("eligible_ticker_count") or len(rows))
        ticker_count = int(theme.get("ticker_count") or len(rows))
        eligible_coverage_ratio = eligible_count / max(ticker_count, 1)
        strong_coverage_ratio = len(strong) / max(eligible_count, 1)
        if (
            eligible_count < EMERGING_CLUSTER_MIN_ELIGIBLE_TICKERS
            or eligible_coverage_ratio < EMERGING_CLUSTER_MIN_ELIGIBLE_COVERAGE_RATIO
            or strong_coverage_ratio < EMERGING_CLUSTER_MIN_STRONG_COVERAGE_RATIO
        ):
            continue
        signals.append(
            ThemePatternSignal(
                signal_type="Emerging Cluster",
                theme=str(theme.get("theme") or rows.iloc[0]["theme"]),
                tickers=_top_tickers(strong, "perf_1w"),
                why_notable=f"Rank {int(rank)} theme has {len(strong)} strong current movers across {eligible_count} eligible contributors before reaching the top cohort.",
                read="Early cluster strength is forming below the main leadership group.",
                priority=60.0 + len(strong) * 5.0 + max(float(theme.get("avg_1w") or 0.0), 0.0),
                metadata={
                    "theme_id": int(theme_id),
                    "rank": int(rank),
                    "strong_ticker_count": int(len(strong)),
                    "eligible_count": eligible_count,
                    "eligible_coverage_ratio": round(float(eligible_coverage_ratio), 2),
                    "strong_coverage_ratio": round(float(strong_coverage_ratio), 2),
                },
            )
        )
    return signals


def _weakening_or_narrowing_themes(metrics: pd.DataFrame) -> list[ThemePatternSignal]:
    signals: list[ThemePatternSignal] = []
    leaders = metrics[pd.to_numeric(metrics["rank"], errors="coerce").le(WEAKENING_TOP_RANK_MAX)].copy()
    for _, row in leaders.iterrows():
        breadth = float(row.get("positive_1w_breadth_pct")) if pd.notna(row.get("positive_1w_breadth_pct")) else 0.0
        eligible = int(row.get("eligible_standardized_count") or row.get("eligible_ticker_count") or 0)
        avg_1w = float(row.get("avg_1w")) if pd.notna(row.get("avg_1w")) else 0.0
        weak_breadth = breadth <= WEAKENING_MAX_POSITIVE_BREADTH_PCT
        thin = eligible <= WEAKENING_THIN_ELIGIBLE_MAX
        current_soft = avg_1w <= 0.0
        if not (weak_breadth or thin or current_soft):
            continue
        reasons = []
        if thin:
            reasons.append(f"only {eligible} eligible contributors")
        if weak_breadth:
            reasons.append(f"1W breadth {_pct(breadth)}")
        if current_soft:
            reasons.append(f"1W avg {_pct(avg_1w)}")
        signals.append(
            ThemePatternSignal(
                signal_type="Weakening / Narrowing Theme",
                theme=str(row.get("theme") or ""),
                tickers=tuple(),
                why_notable=f"Top-{WEAKENING_TOP_RANK_MAX} theme with " + ", ".join(reasons) + ".",
                read="Leadership quality is narrowing or losing current participation.",
                priority=58.0 + max(0, WEAKENING_TOP_RANK_MAX - int(row.get("rank") or WEAKENING_TOP_RANK_MAX)),
                metadata={
                    "theme_id": _safe_int(row.get("theme_id")),
                    "rank": _safe_int(row.get("rank")),
                    "eligible_count": eligible,
                    "positive_1w_breadth_pct": round(breadth, 2),
                    "theme_avg_1w": round(avg_1w, 2),
                },
            )
        )
    return signals


def _safe_int(value: object) -> int | None:
    if value is None or pd.isna(value):
        return None
    return int(value)


def _safe_float(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    return round(float(value), 2)


def _first_present(*values: object) -> float | None:
    for value in values:
        parsed = _safe_float(value)
        if parsed is not None:
            return parsed
    return None
