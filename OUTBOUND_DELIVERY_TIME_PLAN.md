# Outbound Delivery Time Investigation Plan

## Scope

Investigate why POS-created orders synced outbound to WooCommerce can end up with the wrong delivery time or only a single start hour instead of a full time slot range.

Anchor orders:

- Bad example: Woo order `14645`
- Good example: Woo order `14643`

## Confirmed Findings

### 1. Outbound reads the wrong ERP delivery-time fields

Current outbound order payload building in `jarz_woocommerce_integration/services/outbound_sync.py` reads:

- `custom_delivery_date` or `delivery_date`
- `custom_delivery_time` or `delivery_time`

But the ERP sales invoices used by this integration store delivery time as:

- `custom_delivery_date`
- `custom_delivery_time_from`
- `custom_delivery_duration`

Inbound order sync already writes and understands that shape, so outbound is not mirroring the actual contract used by the app.

### 2. Outbound invents a noon timestamp when time is missing

If outbound finds a delivery date but no matching time field, it currently falls back to `12:00` and writes `_orddd_timestamp` using noon. It also skips `Time Slot` entirely in that path.

That creates two problems:

- the synced hour is wrong
- Woo receives no delivery slot range

### 3. The good example uses a range, not a single hour

Inbound parsing expects Woo `Time Slot` values like `19:00 - 20:30`, then converts that into:

- start time
- duration

That means the correct outbound contract should be a date plus a time range, not a single start time label.

## Evidence

### Order `14645` (bad)

Production investigation showed the linked ERP invoice carries:

- `custom_delivery_date = 2026-05-02`
- `custom_delivery_time_from = 19:00:00`
- `custom_delivery_duration = 7200`

But Woo metadata for that order contains:

- `_orddd_delivery_date = Saturday, May 02, 2026`
- `_orddd_timestamp = 1777723200`
- no `_orddd_time_slot`
- no `Time Slot`

`1777723200` corresponds to noon for that date, which matches the current outbound fallback behavior rather than the real ERP delivery time.

### Order `14643` (good)

Production investigation showed this order has the expected Woo slot format:

- `Time Slot = 19:00 - 20:30`

That matches the inbound parser contract and demonstrates the desired outbound shape.

## Root Cause

The outbound delivery formatter is using legacy or nonexistent single-time invoice fields instead of the actual POS delivery fields (`custom_delivery_time_from` + `custom_delivery_duration`). When those wrong fields are empty, outbound fabricates noon and omits the slot range.

## Files Involved

- `jarz_woocommerce_integration/services/outbound_sync.py`
- `jarz_woocommerce_integration/services/order_sync.py`

Primary change target:

- `outbound_sync.py`, inside the delivery metadata block in `_build_order_payload(...)`

Reference contract:

- `order_sync.py`, inside `_parse_delivery_parts(...)`

## Implementation Plan

### Phase 1: Fix outbound delivery metadata construction

1. Add a small helper in `outbound_sync.py` that reads:
   - `custom_delivery_date`
   - `custom_delivery_time_from`
   - `custom_delivery_duration`
2. Compute the delivery end time from `custom_delivery_duration`.
3. Build Woo delivery metadata as:
   - `Delivery Date`
   - `_orddd_delivery_date`
   - `Time Slot`
   - `_orddd_time_slot`
4. Format the slot as `HH:MM - HH:MM` using the real ERP start and end time.

### Phase 2: Preserve backward compatibility

1. Keep a narrow fallback for older invoices that may still only have legacy single-time fields.
2. Do not invent a noon hour when only a date is present.
3. If only a date exists, send the date cleanly and omit misleading time metadata.

### Phase 3: Add regression coverage

Add focused tests for outbound payload generation covering:

1. `19:00:00` with `5400` seconds duration -> `19:00 - 20:30`
2. `19:00:00` with `7200` seconds duration -> `19:00 - 21:00`
3. date-only delivery -> no fake noon time slot
4. legacy single-time fallback still behaves predictably if such rows exist

## Validation Plan

### Code-level validation

1. Build a payload fixture from an invoice shaped like order `14645` and verify the slot becomes `19:00 - 21:00`.
2. Build a payload fixture from an invoice shaped like order `14643` and verify the slot remains `19:00 - 20:30`.
3. Confirm outbound metadata keys match what inbound parsing expects.

### Environment validation

1. Deploy the fix to staging through Git.
2. Create or identify one staging POS order with a known delivery window.
3. Run outbound sync.
4. Inspect the Woo order metadata and confirm:
   - correct delivery date
   - correct `Time Slot`
   - no noon fallback when date-only
5. After staging verification, promote the same commit to production through Git.

## Acceptance Criteria

- POS outbound sync uses `custom_delivery_time_from` and `custom_delivery_duration` as the source of truth.
- Woo receives a full delivery slot range when ERP has a start time and duration.
- Outbound no longer writes an incorrect noon time for orders that do not have a valid delivery time.
- A regression test locks the slot formatting behavior.

## Open Question To Verify During Fix

Production investigation showed a second submitted invoice also linked to Woo order `14645` with null delivery fields. That does not appear to be the primary cause of this bug, but it should be checked during implementation to ensure repeat outbound updates cannot overwrite a correct slot with empty delivery metadata.