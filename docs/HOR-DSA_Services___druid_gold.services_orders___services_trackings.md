# DSA Services — `druid_gold.services_orders` + `druid_gold.services_trackings` + `pgdb_gold.ordergroups`

**Purpose:** Reference doc for cheque book service order analytics. Covers order lifecycle, senior vs non-senior citizen identification, payment tracking via ordergroups, and day-on-day order metrics.

**When to use these tables:**
- `druid_gold.services_orders` — what was ordered, when, by whom, and whether the customer is chargeable.
- `druid_gold.services_trackings` — current fulfilment status and reason for that status.
- `pgdb_gold.ordergroups` — payment initiation and outcome. Only exists for chargeable (`payment_eligible = true`) orders.
- Use all three together for any analysis that needs order detail + fulfilment status + payment outcome.

---

## 1. Overall Understanding

Three production tables form the complete cheque book order record:

**`druid_gold.services_orders`** — Order header. One row per order. Created when a customer places a cheque book request. Contains: who, when, delivery details, logistics progress, and payment eligibility.

**`druid_gold.services_trackings`** — Fulfilment status. One row per order, overwritten in place on every status change (not event-sourced). `updated_at` reflects the most recent change.

**`pgdb_gold.ordergroups`** — Payment record. One row per `source_txn_id`. Created **only when `payment_eligible = true`** — senior citizens and pre-cutover free orders are absent from this table. `vertical_id = 19` always identifies cheque book orders within this multi-vertical table.

**Join key:** `ordergroups.source_txn_id` = `services_trackings.order_id` = `services_orders.order_id`

**Query engine:** Trino (Hive metastore)

---

## 2. Pricing Policy & Senior / Non-Senior Citizen Definitions

The new charging policy went live on **20th April 2026 at 9 PM IST**.

| | Before 20 Apr 9 PM IST | From 20 Apr 9 PM IST onwards |
|---|---|---|
| Non-senior, first order | Free (`payment_eligible = false`) | ₹100 (`payment_eligible = true`) |
| Non-senior, repeat order | ₹100 (`payment_eligible = true`) | ₹100 (`payment_eligible = true`) |
| Senior citizen | Always free (`payment_eligible = false`) | Always free (`payment_eligible = false`) |

> **Use April 21st as the clean start date for all analytics.** On April 20th, orders placed before 9 PM IST still ran under the old policy — `payment_eligible = false` for non-senior first-timers — making them indistinguishable from senior citizens without a DOB check. Confirmed via DOB lookup: 99.4% of April 20th "senior-looking" orders were actually non-senior citizens.

### Identifying a successful non-senior citizen order
- `services_orders.payment_eligible = true`
- Entry in `ordergroups` with `order_group_status = 'SUCCESS'`, `amount = 100`, `vertical_id = 19`
- **Date dimension:** `ordergroups.created_at` (when payment was initiated)

### Identifying a successful senior citizen order
- `services_orders.payment_eligible = false`
- **No** entry in `ordergroups` for `vertical_id = 19`
- `services_trackings.reason NOT IN ('SIGNATURE_DECLINED', 'ORDER_ADDRESS_UPDATED')`
- **Date dimension:** `services_orders.created_at` (when order was placed)

---

## 3. User Base & Grain

- **`services_orders`:** One row per order. One customer (`user_id`) can have multiple orders.
- **`services_trackings`:** One row per `order_id`, upserted in place. No history of prior states.
- **`ordergroups`:** One row per `source_txn_id`. Only exists for chargeable orders.
- All three tables currently contain only cheque book orders (`type = 'CHEQUE_BOOK'`).
- **Partitioned by:** `year`, `month`, `day` on all three tables. Always apply partition filters.
- **Soft-delete:** Always apply `__is_deleted = false` on all tables used.

---

## 4. Schema — `druid_gold.services_orders`

| # | Column | Type | Definition | Usage Notes |
|---|--------|------|------------|-------------|
| 1 | `order_id` | VARCHAR | Business-level order identifier. | Primary join key to `services_trackings` and `ordergroups`. |
| 2 | `user_id` | VARCHAR | Customer UUID. | Equivalent to `uuid` in other DSA tables. |
| 3 | `created_at` | TIMESTAMP | Order creation timestamp (UTC). | IST: `created_at + interval '330' minute`. Use as date dimension for senior citizen orders. |
| 4 | `updated_at` | TIMESTAMP | Last update on the order row (UTC). | Reflects when logistics fields were last updated. |
| 5 | `type` | VARCHAR | Service type. | Always `'CHEQUE_BOOK'`. Include as a filter for future-proofing. |
| 6 | `payment_eligible` | BOOLEAN | Whether the customer is chargeable. | `true` = non-senior (chargeable). `false` = senior citizen (free) from April 21st onwards. See Section 2 for pre-cutover nuance. |
| 7 | `amount` | VARCHAR | Charge amount in rupees, stored as VARCHAR. | `'0.00'` (free) or `'100.00'` (charged). Use `CAST(amount AS DOUBLE)` for arithmetic. |
| 8 | `address` | VARCHAR | Delivery address for the cheque book. | |
| 9 | `shipment_id` | VARCHAR | Shipment ID from logistics partner. | NULL/empty = not yet dispatched. Use `CASE WHEN shipment_id IS NOT NULL AND shipment_id <> ''` to tag. |
| 10 | `awb_id` | VARCHAR | Air Waybill number. | NULL/empty = not yet handed to courier. Populated after `shipment_id`. |
| 11 | `courier_id` | VARCHAR | Courier/logistics partner identifier. | |
| 12 | `source_order_group_refund_id` | VARCHAR | Links to an `ordergroups` refund entry when this order has an associated refund. | Defer for now — more context to be added. |
| 13 | `__is_deleted` | BOOLEAN | Soft-delete flag. | Always filter: `__is_deleted = false`. |
| 14 | `year` | INTEGER | Partition column. | Always apply. |
| 15 | `month` | INTEGER | Partition column. | Always apply. |
| 16 | `day` | INTEGER | Partition column. | Always apply. |

**Out of scope:** `id` (row PK), `__source_lsn`, `__source_timestamp`, `__data_partition`, `__data_offset`.

---

## 5. Schema — `druid_gold.services_trackings`

| # | Column | Type | Definition | Usage Notes |
|---|--------|------|------------|-------------|
| 1 | `order_id` | VARCHAR | Join key to `services_orders.order_id`. | |
| 2 | `user_id` | VARCHAR | Customer UUID. Same value as `services_orders.user_id`. | Prefer `services_orders.user_id` as primary source. |
| 3 | `status` | VARCHAR | Current fulfilment status. | See Section 7a. |
| 4 | `reason` | VARCHAR | Reason string for the current status. | Populated on every status. Key exclusions for senior citizen identification: `reason NOT IN ('SIGNATURE_DECLINED', 'ORDER_ADDRESS_UPDATED')`. `ORDER_ADDRESS_UPDATED` = address update before clicking "Order Now" (order not yet placed). `SIGNATURE_DECLINED` = order rejected at signature stage. |
| 5 | `created_at` | TIMESTAMP | When the tracking row was first created (UTC). | |
| 6 | `updated_at` | TIMESTAMP | Most recent status change timestamp (UTC). | Use for SLA / time-to-fulfil analysis. |
| 7 | `__is_deleted` | BOOLEAN | Soft-delete flag. | Always filter: `__is_deleted = false`. |
| 8 | `year` | INTEGER | Partition column. | Always apply. |
| 9 | `month` | INTEGER | Partition column. | Always apply. |
| 10 | `day` | INTEGER | Partition column. | Always apply. |

**Out of scope:** `id` (row PK), `__source_lsn`, `__source_timestamp`, `__data_partition`, `__data_offset`.

---

## 6. Schema — `pgdb_gold.ordergroups`

| # | Column | Type | Definition | Usage Notes |
|---|--------|------|------------|-------------|
| 1 | `order_group_id` | VARCHAR | Primary identifier of the ordergroups row. | |
| 2 | `source_txn_id` | VARCHAR | Join key to `services_orders.order_id` and `services_trackings.order_id`. | Always join on this column. |
| 3 | `customer_id` | VARCHAR | Customer UUID. Same as `user_id` in services tables. | |
| 4 | `created_at` | TIMESTAMP | Payment initiation timestamp (UTC). | IST: `created_at + interval '330' minute`. Use as **date dimension for non-senior citizen orders**. |
| 5 | `amount` | DOUBLE | Charge amount in rupees. | Currently `100.0` for all chargeable orders. DOUBLE type — no casting needed for arithmetic. Note: different type from `services_orders.amount` (VARCHAR). |
| 6 | `order_group_status` | VARCHAR | Payment outcome. | See Section 7b. Always filter `= 'SUCCESS'` for successful order analytics. |
| 7 | `vertical_id` | BIGINT | Product vertical identifier. | Always filter `vertical_id = 19` for cheque book orders. |
| 8 | `refunded_amount` | DOUBLE | Amount refunded, if applicable. | Non-zero on refund flows. |
| 9 | `convenience_charge_applicable_nbapplicable` | BOOLEAN | Convenience charge flag for net banking. | These seven flags indicate whether a surcharge applies on top of the base ₹100 for each payment method. Not typically used in order volume analytics. |
| 10 | `convenience_charge_applicable_sliceupiapplicable` | BOOLEAN | Slice UPI. | |
| 11 | `convenience_charge_applicable_debitcardapplicable` | BOOLEAN | Debit card. | |
| 12 | `convenience_charge_applicable_upiintentapplicable` | BOOLEAN | UPI intent. | |
| 13 | `convenience_charge_applicable_creditcardapplicable` | BOOLEAN | Credit card. | |
| 14 | `convenience_charge_applicable_upicollectapplicable` | BOOLEAN | UPI collect. | |
| 15 | `convenience_charge_applicable_prepaidcardapplicable` | BOOLEAN | Prepaid card. | |
| 16 | `__is_deleted` | BOOLEAN | Soft-delete flag. | Always filter: `__is_deleted = false`. |
| 17 | `year` | INTEGER | Partition column. | Always apply. |
| 18 | `month` | INTEGER | Partition column. | Always apply. |
| 19 | `day` | INTEGER | Partition column. | Always apply. |

**Out of scope:** `id`, `merchant_id`, `has_multiple_merchants`, `created_by`, `split_settlement_details`, `__source_lsn`, `__source_timestamp`, `__data_partition`, `__data_offset`.

---

## 7. Column Value Reference

### 7a. `status` — `services_trackings`

| Value | Meaning |
|-------|---------|
| `CREATED` | Order placed, tracking row initialised. |
| `PENDING` | Being processed by the fulfilment system. |
| `SUCCESS` | Fulfilled and dispatched. |
| `FAILED` | Could not be fulfilled. Check `reason` for detail. |

Lifecycle: `CREATED → PENDING → SUCCESS` or `FAILED`

> `reason` is populated at every status — not only on `FAILED`. For senior citizen identification, exclude both `'SIGNATURE_DECLINED'` (order rejected at signature stage) and `'ORDER_ADDRESS_UPDATED'` (tracking row upserted when user updates address before clicking "Order Now" — not a placed order).

### 7b. `order_group_status` — `pgdb_gold.ordergroups`

| Value | Meaning |
|-------|---------|
| `CREATED` | Payment group initialised. |
| `PENDING` | Payment in progress. |
| `SUCCESS` | Payment completed. Use this for successful non-senior order counts. |
| `FAILED` | Terminal failure — cannot be retried. |
| `NON_TERMINAL_FAILURE` | Payment failed but retriable. Exclude from "final failure" counts; track separately as pending retry. |

### 7c. Delivery progress tags (derived from `services_orders`)

| Tag | Condition | Meaning |
|-----|-----------|---------|
| `'shipment id generated'` | `shipment_id IS NOT NULL AND shipment_id <> ''` | Logistics partner has created a shipment. |
| `'awb id generated'` | `awb_id IS NOT NULL AND awb_id <> ''` | Item handed to courier; AWB tracking active. |
| NULL | Both null/empty | Order not yet dispatched. |

---

## 8. Standard Join Pattern

```sql
SELECT
    so.order_id,
    so.user_id,
    date(so.created_at + interval '330' minute)                     AS order_date_ist,
    so.payment_eligible,
    CAST(so.amount AS DOUBLE)                                       AS so_amount,
    CASE WHEN so.shipment_id IS NOT NULL AND so.shipment_id <> ''
         THEN 'shipment id generated' END                           AS shipment_tag,
    CASE WHEN so.awb_id IS NOT NULL AND so.awb_id <> ''
         THEN 'awb id generated' END                                AS awb_tag,
    st.status,
    st.reason,
    og.order_group_id,
    og.order_group_status,
    og.amount                                                       AS og_amount,
    date(og.created_at + interval '330' minute)                     AS payment_date_ist
FROM druid_gold.services_orders so
JOIN druid_gold.services_trackings st
    ON st.order_id = so.order_id
    AND st.__is_deleted = false
LEFT JOIN pgdb_gold.ordergroups og
    ON og.source_txn_id = so.order_id
    AND og.vertical_id = 19
    AND og.__is_deleted = false
WHERE so.__is_deleted = false
  AND so.type = 'CHEQUE_BOOK'
  AND (so.year * 10000 + so.month * 100 + so.day) >= 20260421
  AND (st.year * 10000 + st.month * 100 + st.day) >= 20260420
```

> Use **`LEFT JOIN`** on `ordergroups` — senior citizens and pre-cutover free orders have no ordergroups entry and must be retained. Partition filters must be applied independently on all three tables.

---

## 9. Common Query Patterns

### Day-on-day non-senior citizen orders (Superset)

```sql
SELECT
    date(og.created_at + interval '330' minute)     AS order_date_ist,
    COUNT(DISTINCT so.order_id)                     AS orders
FROM druid_gold.services_orders so
JOIN pgdb_gold.ordergroups og
    ON og.source_txn_id = so.order_id
WHERE so.payment_eligible = true
  AND so.type = 'CHEQUE_BOOK'
  AND so.__is_deleted = false
  AND og.order_group_status = 'SUCCESS'
  AND og.amount = 100
  AND og.vertical_id = 19
  AND og.__is_deleted = false
  AND (so.year * 10000 + so.month * 100 + so.day) >= 20260421
  AND (og.year * 10000 + og.month * 100 + og.day) >= 20260421
GROUP BY 1
ORDER BY 1
```

### Day-on-day senior citizen orders (Superset)

```sql
SELECT
    date(so.created_at + interval '330' minute)     AS order_date_ist,
    COUNT(DISTINCT so.order_id)                     AS orders
FROM druid_gold.services_orders so
JOIN druid_gold.services_trackings st
    ON st.order_id = so.order_id
LEFT JOIN pgdb_gold.ordergroups og
    ON og.source_txn_id = so.order_id
    AND og.vertical_id = 19
    AND og.__is_deleted = false
WHERE so.payment_eligible = false
  AND so.type = 'CHEQUE_BOOK'
  AND so.__is_deleted = false
  AND st.reason NOT IN ('SIGNATURE_DECLINED', 'ORDER_ADDRESS_UPDATED')
  AND st.__is_deleted = false
  AND og.source_txn_id IS NULL
  AND (so.year * 10000 + so.month * 100 + so.day) >= 20260421
  AND (st.year * 10000 + st.month * 100 + st.day) >= 20260420
GROUP BY 1
ORDER BY 1
```

### Combined stacked chart — both populations (Superset)

Use this as the single query for a stacked bar chart. Set `customer_type` as the series/colour dimension.

```sql
SELECT
    order_date_ist,
    customer_type,
    COUNT(DISTINCT order_id)                        AS orders
FROM (
    SELECT
        date(og.created_at + interval '330' minute) AS order_date_ist,
        so.order_id,
        'non_senior'                                AS customer_type
    FROM druid_gold.services_orders so
    JOIN pgdb_gold.ordergroups og
        ON og.source_txn_id = so.order_id
    WHERE so.payment_eligible = true
      AND so.type = 'CHEQUE_BOOK'
      AND so.__is_deleted = false
      AND og.order_group_status = 'SUCCESS'
      AND og.amount = 100
      AND og.vertical_id = 19
      AND og.__is_deleted = false
      AND (so.year * 10000 + so.month * 100 + so.day) >= 20260421
      AND (og.year * 10000 + og.month * 100 + og.day) >= 20260421

    UNION ALL

    SELECT
        date(so.created_at + interval '330' minute) AS order_date_ist,
        so.order_id,
        'senior'                                    AS customer_type
    FROM druid_gold.services_orders so
    JOIN druid_gold.services_trackings st
        ON st.order_id = so.order_id
    LEFT JOIN pgdb_gold.ordergroups og
        ON og.source_txn_id = so.order_id
        AND og.vertical_id = 19
        AND og.__is_deleted = false
    WHERE so.payment_eligible = false
      AND so.type = 'CHEQUE_BOOK'
      AND so.__is_deleted = false
      AND st.reason NOT IN ('SIGNATURE_DECLINED', 'ORDER_ADDRESS_UPDATED')
      AND st.__is_deleted = false
      AND og.source_txn_id IS NULL
      AND (so.year * 10000 + so.month * 100 + so.day) >= 20260421
      AND (st.year * 10000 + st.month * 100 + st.day) >= 20260420
) combined
GROUP BY 1, 2
ORDER BY 1, 2
```

### Order volume breakdown by status, reason, and delivery progress

```sql
SELECT
    date(so.created_at + interval '330' minute)                     AS created_date,
    st.status,
    st.reason,
    CASE WHEN so.shipment_id IS NOT NULL AND so.shipment_id <> ''
         THEN 'shipment id generated' END                           AS shipment_tag,
    CASE WHEN so.awb_id IS NOT NULL AND so.awb_id <> ''
         THEN 'awb id generated' END                                AS awb_tag,
    COUNT(DISTINCT so.order_id)                                     AS orders
FROM druid_gold.services_orders so
LEFT JOIN druid_gold.services_trackings st
    ON st.order_id = so.order_id
WHERE so.__is_deleted = false
  AND so.type = 'CHEQUE_BOOK'
  AND date(so.created_at + interval '330' minute) >= date('2026-04-21')
GROUP BY 1, 2, 3, 4, 5
ORDER BY orders DESC
```

### Fulfilment success rate (order SR) by day

```sql
SELECT
    date(so.created_at + interval '330' minute)                             AS created_date,
    100.0 * SUM(CASE WHEN st.status = 'SUCCESS' THEN 1 ELSE 0 END)
           / COUNT(DISTINCT so.order_id)                                    AS order_sr_pct
FROM druid_gold.services_orders so
LEFT JOIN druid_gold.services_trackings st
    ON st.order_id = so.order_id
WHERE so.__is_deleted = false
  AND so.type = 'CHEQUE_BOOK'
  AND date(so.created_at + interval '330' minute) >= date('2026-04-21')
GROUP BY 1
ORDER BY 1
```

---

## 10. Nuances & Gotchas

1. **April 20th data is not clean — use April 21st as start date.** The new charging policy went live at 9 PM IST on April 20th. Orders placed before that time still ran under the old policy (`payment_eligible = false` for non-senior first-timers). Confirmed via DOB check: 99.4% of April 20th "senior-looking" orders were actually non-senior citizens who placed their first free order before the cutover.

2. **`payment_eligible = false` does not exclusively mean senior citizen before April 21st.** Under the old policy, non-senior first-time orderers also had `payment_eligible = false` and no ordergroups entry. From April 21st onwards, `payment_eligible = false` reliably identifies senior citizens only.

3. **`services_trackings` is one row per order — no deduplication needed.** The row is upserted in place. A plain `JOIN` (no `ROW_NUMBER()`) is correct and sufficient.

4. **Use `LEFT JOIN` on `ordergroups`, not `INNER JOIN`.** Senior citizens and orders without a payment entry will have no ordergroups row. `INNER JOIN` would silently drop them.

5. **`og.__is_deleted = false` belongs in the `ON` clause for `LEFT JOIN`.** Putting it in `WHERE` converts the left join to an inner join, breaking the "no entry" check.

6. **`amount` is different types across tables.** `services_orders.amount` is VARCHAR — use `CAST(amount AS DOUBLE)`. `ordergroups.amount` is DOUBLE — no casting needed.

7. **`reason` is populated on every status, not only on failures.** Do not assume `reason IS NULL` means success. Always filter by `status` first, then use `reason` for breakdown.

8. **`NON_TERMINAL_FAILURE` ≠ `FAILED` in ordergroups.** `NON_TERMINAL_FAILURE` is retriable; `FAILED` is terminal. Exclude both from success counts but track them separately — do not lump them together.

9. **`vertical_id = 19` is mandatory when querying `ordergroups`.** This table spans multiple product verticals. Without this filter, results will include payments from other products.

10. **Date dimension differs by population.** For non-senior orders, use `ordergroups.created_at` (payment initiation date). For senior orders, use `services_orders.created_at` (order placement date). Mixing them in a combined query will cause date misalignment.

11. **Partition filters on all three tables independently.** Apply `year/month/day` filters on `services_orders`, `services_trackings`, and `ordergroups` separately. A 1-day buffer on `services_trackings` (`>= 20260420` when orders start from `>= 20260421`) is recommended because the tracking row's partition is set at first insert (address update), which can be one day earlier than the order placement date.

12. **`id` ≠ `order_id`** on both services tables. `id` is an auto-generated row PK. `order_id` is the business key used for all joins.

---

## 11. What is NOT Answerable from These Tables

| Question | Where to look instead |
|----------|-----------------------|
| Status transition history for an order | Not available — `services_trackings` only stores current state |
| Whether a senior citizen is genuinely over 60 (pre-April 21st) | DOB check via `uid_db_gold.customers` → `bsgcrm_gold_pii.customer_ind_info` → `decryptFunction(date_of_birth)` |
| Payment retry history | Not available — only current `order_group_status` is stored |
| Account balance at time of order | `dsa_user_journey_tags.cleaned_balance` |
| User journey stage (Activated, Adopted, etc.) | `dsa_user_journey_tags.user_journey_tag` |
| User demographics / acquisition | `cohort_tags` |
| Transaction debit linked to a charged order | `casa_txn_gold.transaction` (correlate via `user_id` and order date) |
| FD / deposit orders | `druid_gold.deposit_orders` |
| Credit card transactions | `lmsdb_gold.ledger_data_credit_ac_txn` |
