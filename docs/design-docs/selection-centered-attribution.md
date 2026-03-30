# Selection-Centered Attribution

## Why

The previous canonical-refresh design rebuilt session-level truth first and then
tried to infer attribution from that reconstructed session state. That made
late/out-of-order data expensive to repair because a small attribution change
could fan out into large rescans of session and serving tables.

The selection-centered design changes the canonical anchor:

`raw events -> normalized events -> canonical_selection_events -> capability versions/intervals -> attribution decisions/current -> session current -> status-hour and serving stores`

This moves the system onto the real business event: orchestrator selection.

## Core rules

- `canonical_selection_events` is the sparse decision spine.
- `canonical_selection_attribution_decisions` is append-only audit history.
- `canonical_selection_attribution_current` is the latest materialized decision.
- Runtime attribution against raw capability churn is forbidden.
- Capability matching must use `canonical_orch_capability_versions` and
  `canonical_orch_capability_intervals`.
- `canonical_session_current_store` and `canonical_status_hours_store` are
  downstream derivatives, never attribution inputs.

## Freshness policy

For a given `selection_ts`, the resolver applies:

1. exact interval match
2. nearest prior interval within `10m`
3. bounded future interval within `30s`

If only older evidence exists, the result is `stale`.

## Attribution outcomes

The resolver persists both `attribution_status` and `attribution_reason`.

Steady-state status classes are:

- `resolved`
- `hardware_less`
- `stale`
- `ambiguous`
- `unresolved`

Important unresolved subclasses currently include:

- `missing_uri_snapshot_local_alias_present`
- `missing_uri_snapshot_address_match_present`
- `no_selection_no_orch_excused`

`missing_uri_snapshot_local_alias_present` means the session carried an
observed orchestrator URI, no compatible capability snapshot existed for that
exact URI in the attribution window, but a compatible local/proxy identity
match did exist. This stays `unresolved`; it is not promoted to
`hardware_less`.

`hardware_less` is reserved for sessions where canonical attribution succeeded
but GPU or hardware metadata was absent. It is a distinct attribution class,
not a synonym for unresolved alias/URI gaps.

Fact-surface obligations:

- unresolved, stale, ambiguous, and hardware-less sessions remain visible in
  session and status-hour facts
- demand and reliability views must not silently drop these sessions
- only `resolved` counts as fully attributed identity
- GPU-oriented rollups must not treat unresolved URI/local-alias cases as
  hardware-less matches

## Operational seam

- `auto` owns the production scheduler:
  - visible closed historical backlog first
  - dirty closed historical `(org, event_date)` partitions from late accepted
    raw arrivals second
  - live lateness-window `tail` third
- `bootstrap` is a bounded/operator backlog catch-up mode
- `backfill` owns deterministic manual historical replays
- `tail` owns `event_ts > cutoff - lateness_window` when run directly
- `repair-window` claims exclusive ownership for its requested window
- `verify` never writes current-state tables

Automatic historical repair is quiet-period gated. Closed days dirtied by newly
accepted late arrivals are repaired only after that day has gone quiet for the
configured dirty-repair quiet period, and repeated late arrivals for an already
claimed day are coalesced onto the in-flight repair.

All modes share the same canonical tables, claim model, padded-read semantics,
and exact write ownership rules.
