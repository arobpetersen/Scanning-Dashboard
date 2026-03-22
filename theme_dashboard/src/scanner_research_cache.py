from __future__ import annotations

"""Lightweight cache policy for scanner research.

Policy for now:
- In-memory only. Nothing here is authoritative persisted state.
- Best effort. Cache misses are acceptable and simply recompute/reload.
- Session-oriented. A process restart or explicit clear invalidates entries.
- Invalidation should happen after behavior-shaping refactors, tests that need a
  fresh view, or when profile/theme preprocessing assumptions change.
"""

_PROFILE_CACHE: dict[str, dict[str, object]] = {}
_DESCRIPTION_ANALYSIS_CACHE: dict[tuple[object, ...], dict[str, object]] = {}
_THEME_PREPROCESS_CACHE: dict[tuple[object, ...], dict[str, object]] = {}


def clear_scanner_research_caches() -> None:
    _PROFILE_CACHE.clear()
    _DESCRIPTION_ANALYSIS_CACHE.clear()
    _THEME_PREPROCESS_CACHE.clear()

