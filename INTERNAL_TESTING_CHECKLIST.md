# Scanning-Dashboard Internal Testing Checklist

## Where To Start

Start in `Themes`.

Use `Historical Performance` only as a follow-up page when you want to understand:
- what changed across a historical window
- why a movement row ranked where it did
- whether a surprising historical row is still trustworthy

## Core Workflows To Test

1. Review `Current Market Leadership` and `Current Top Themes By Window`.
2. Click a theme row and confirm the selected-theme detail updates immediately and correctly.
3. Inspect the selected-theme summary card, ticker table, and bottom ticker-composite history chart.
4. Try the main toggles:
   - daily deltas
   - advanced movement context
   - include suppressed tickers
5. Use `Theme Movement Snapshots` to compare current intuition vs historical movement.
6. Use `Historical Performance` only when you want to audit a historical mover in more detail.

## Most Useful Feedback

Please report:
- confusing labels, captions, or page roles
- places where current vs historical meaning is unclear
- drilldown, selection, or stale/off-by-one behavior
- rows that look mathematically wrong or hard to reconcile
- empty or partial-data states that make the app feel broken
- advanced/debug sections that feel too prominent or too hidden

## Known Limitations

- `Themes` is the primary workflow; `Historical Performance` is more specialized.
- Some advanced/debug trust tools are still present because they are useful during internal testing.
- Thin themes can still appear when their row contract is valid; treat them as concentration context, not broad confirmation.
- The app distinguishes current live/preferred-source views from historical movement views, so some current vs historical differences are expected.
