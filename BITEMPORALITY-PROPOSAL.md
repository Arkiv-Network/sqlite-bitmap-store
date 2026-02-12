# Proposal to Add Bitemporality

## Context

The current implementation does not support bitemporality. This causes two problems:

* Paginated queries cannot guarantee stable results across multiple requests, since the underlying data may change between pages.
* Archive nodes cannot query historical data at a specific block height.

The root cause is the nature of bitmap indexes: each bitmap contains a summary of all entities that currently match a given key/value pair. Once computed for the current state, there is no way to reconstruct what the bitmap looked like at an earlier block.

## Proposal

We propose a video-compression-inspired approach to allow querying at any block height.

The core idea is to maintain two types of bitmap entries: **keyframes** (full bitmaps representing the complete state) and **delta frames** (XOR diffs against the previous frame). Applying a delta frame to the previous state reconstructs the bitmap at that point in time.

This approach is efficient because the delta frames are typically much smaller than the full bitmap. To bound the cost of reconstruction, we limit the number of consecutive delta frames to a constant `C` (e.g. 128). After `C` deltas, a new keyframe is stored.

```
Block:  10        11        12        ...       138       139
        ┌───────┐ ┌───────┐ ┌───────┐           ┌───────┐ ┌───────┐
Type:   │  KEY  │ │ DELTA │ │ DELTA │    ...    │ DELTA │ │  KEY  │
        └───────┘ └───────┘ └───────┘           └───────┘ └───────┘
           │         │         │                    │         │
           │         │         │                    │         │
           ▼         ▼         ▼                    ▼         ▼
        Full      XOR with  XOR with            XOR with  Full
        bitmap    block 10  block 11            block 137 bitmap
                  state     state               state

To reconstruct state at block 12:
  state(10) XOR delta(11) XOR delta(12)

To reconstruct state at block 139:
  Just read the keyframe directly
```

### Storing Bitmaps

When storing a bitmap for a given key/value pair at a new block:

1. If no bitmap exists for this key/value pair, the previous state is assumed to be an empty bitmap. The new bitmap is stored as a keyframe.
2. If existing bitmap entries exist (a keyframe and optionally delta frames), they are loaded and the XOR chain is applied to reconstruct the current state.
3. The new bitmap is produced by applying the incoming changes to the reconstructed current state.
4. If the number of delta frames since the last keyframe is less than `C`, the XOR of the old and new bitmaps is stored as a new delta frame.
5. If the number of delta frames has reached `C`, the new bitmap is stored as a fresh keyframe instead.

### Reading Bitmaps

When reading a bitmap for a given key/value pair at a target block `B`:

1. Find the most recent keyframe at or before block `B`.
2. Load all delta frames between that keyframe and block `B` (inclusive).
3. Starting from the keyframe, apply each delta frame in sequence using XOR to reconstruct the bitmap state at block `B`.

If block `B` coincides with a keyframe, no delta application is needed and the keyframe is returned directly. In the worst case, up to `C` XOR operations are required.

## Changes Needed to the Current Schema

- The `payloads` table gains `from_block` and `to_block` columns to track which block range each payload version is valid for.
- Each bitmap table (`string_attributes_values_bitmaps`, `numeric_attributes_values_bitmaps`) gains two columns:
    - `block` — the block height at which this bitmap entry was recorded.
    - `is_full_bitmap` — a boolean flag indicating whether the entry is a keyframe (`true`) or a delta frame (`false`).

## Pruning Old State

To prune all state before a block `B`:

1. Delete all payloads where `to_block <= B`.
2. Find the most recent keyframe at or before block `B` for each key/value pair.
3. Delete all bitmap entries (keyframes and deltas) that precede that keyframe.

The keyframe itself must be kept, as it serves as the base for any subsequent delta frames.

## Support for Reorgs

In the event of a chain reorganization that invalidates all blocks after block `B`:

1. Delete all payloads where `to_block > B`.
2. Delete all bitmap entries where `block > B`.

No reconstruction is needed. The remaining keyframes and delta frames still form a valid chain up to block `B`.

## Consequences

### Query Performance

Querying becomes slower because reading a bitmap now requires reconstructing state from a keyframe and up to `C` delta frames. The performance overhead is directly driven by the choice of `C`. A smaller `C` reduces reconstruction cost but increases storage due to more frequent keyframes.

### Increased Storage

Since delta frames are stored alongside keyframes, the database stores more data than the current single-bitmap-per-key approach. The actual impact depends on the rate of change between blocks and is difficult to predict analytically.

### Increased Block Processing Time

Each new block requires computing and storing a delta (or a new keyframe), which adds overhead compared to the current approach of overwriting the bitmap in place. The real impact should be measured empirically with both synthetic and production data.
