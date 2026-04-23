# DSA Services — `druid_gold.services_orders` + `druid_gold.services_trackings`

**Purpose:** Reference doc for individual DSA service order-level analysis. Covers the cheque book order lifecycle, delivery progress tracking, payment eligibility, status classification, and common query patterns.

**When to use these tables:**
- Use `druid_gold.services_orders` when you need order metadata — what was ordered, when, delivery address, shipment progress, and billing eligibility.
- Use `druid_gold.services_trackings` when you need the current status of an order — what lifecycle stage it is in and the reason for that state.
- Use both together (joined on `order_id`) for any analysis that needs order detail + current status in the same query.
- For the debit transaction charged to the customer for a cheque book order, cross-reference `casa_txn_gold.transaction`.

---

## 1. Overall Understanding

Two production database tables that together form the complete cheque book service order record.

**`druid_gold.services_orders`** — The order header table. Created once when a customer places a service request. Contains: who placed the order, when, delivery address, logistics progress (shipment and AWB IDs), courier assignment, and payment eligibility.

**`druid_gold.services_trackings`** — The order status table. Contains **exactly one row per `order_id`**. This row is overwritten in place as the order progresses through its lifecycle — it is not event-sourced. The `updated_at` column reflects the most recent status change time.

They are joined on `order_id` (the business-level order identifier) to get a complete view of each order with its current status.

**Query engine:** Trino (Hive metastore)

> **Note:** These are production database tables. The columns documented below cover those relevant to analytics. Both tables have additional internal/CDC columns (`__source_lsn`, `__source_timestamp`, `__data_partition`, `__data_offset`) which are pipeline metadata and should be ignored for analytics purposes.

---

## 2. User Base & Grain

**`services_orders`:** One row per service order. A single customer (`user_id`) can place multiple orders over time.

**`services_trackings`:** One row per `order_id`. This row is upserted — not appended — as the order status changes. There is never more than one tracking row per order.

Both tables currently contain only cheque book orders (`type = 'CHEQUE_BOOK'`).

**Partitioned by:** `year`, `month`, `day` (on both tables). Always apply partition filters to avoid full table scans.

**Soft-delete filter:** Always apply `__is_deleted = false` on both tables.

---

## 3. Schema — `druid_gold.services_orders`

| # | Column | Data Type | Definition | Usage Notes |
|---|--------|-----------|------------|-------------|
| 1 | `id` | VARCHAR | Auto-generated row primary key. | Not the business key. Do not use for joins. |
| 2 | `order_id` | VARCHAR | Business-level order identifier. | Primary join key to `services_trackings.order_id`. |
| 3 | `user_id` | VARCHAR | UUID of the customer who placed the order. | Equivalent to `uuid` in other DSA tables. Join to `cohort_tags.uuid` or `dsa_user_journey_tags.uuid` for user enrichment. |
| 4 | `created_at` | TIMESTAMP | Order creation timestamp (UTC). | IST: `created_at + interval '330' minute`. Use for date-range filtering and as the canonical order date. |
| 5 | `updated_at` | TIMESTAMP | Last update timestamp on the order row (UTC). | Reflects logistics field updates (e.g. when `shipment_id` was populated). |
| 6 | `type` | VARCHAR | Service type. | Currently always `'CHEQUE_BOOK'`. Always filter `type = 'CHEQUE_BOOK'` for clarity and future-proofing. |
| 7 | `address` | VARCHAR | Delivery address for the physical cheque book. | Free-text field. Not typically used in aggregated analytics. |
| 8 | `shipment_id` | VARCHAR | Shipment ID assigned by the logistics provider. | NULL or empty = shipment not yet generated. Non-null = shipment created. Use: `CASE WHEN shipment_id IS NOT NULL AND shipment_id <> '' THEN 'shipment id generated' END`. |
| 9 | `awb_id` | VARCHAR | Air Waybill number assigned when item is handed to courier. | NULL or empty = not yet dispatched. Non-null = item physically handed to courier. Use: `CASE WHEN awb_id IS NOT NULL AND awb_id <> '' THEN 'awb id generated' END`. |
| 10 | `courier_id` | VARCHAR | Identifier of the courier/logistics partner. | Use to analyse courier-wise fulfilment rates. |
| 11 | `payment_eligible` | BOOLEAN | Whether the customer is chargeable for this order. | `true` = order is billable. Filter `payment_eligible = true` for charge-related analysis. Senior citizen exemption logic uses this flag. |
| 12 | `amount` | VARCHAR | Charge amount in rupees, stored as VARCHAR. | Currently always `'0.00'` (free/waived) or `'100.00'` (charged). Use `CAST(amount AS DOUBLE)` for arithmetic. |
| 13 | `__is_deleted` | BOOLEAN | Soft-delete flag. | Always filter: `__is_deleted = false`. |
| 14 | `year` | INTEGER | Partition column. | Always apply in WHERE clause. |
| 15 | `month` | INTEGER | Partition column. | Always apply in WHERE clause. |
| 16 | `day` | INTEGER | Partition column. | Always apply in WHERE clause. |

**Out of scope for analytics:** `source_order_group_refund_id` (context TBD), `__source_lsn`, `__source_timestamp`, `__data_partition`, `__data_offset`.

---

## 4. Schema — `druid_gold.services_trackings`

| # | Column | Data Type | Definition | Usage Notes |
|---|--------|-----------|------------|-------------|
| 1 | `id` | VARCHAR | Auto-generated row primary key. | Not the business key. Do not use for joins. |
| 2 | `order_id` | VARCHAR | Business-level join key to `services_orders.order_id`. | Always join: `b.order_id = a.order_id`. |
| 3 | `user_id` | VARCHAR | UUID of the customer. | Same value as `services_orders.user_id`. Either can be used; prefer `services_orders.user_id` for user-level analysis since it is always populated at order creation. |
| 4 | `status` | VARCHAR | Current lifecycle status of the order. | See Section 5 for all values. This is the single source of truth for order state. |
| 5 | `reason` | VARCHAR | Reason string for the current status. | Populated on every status — not just failures. Useful for understanding failure modes and processing stages. |
| 6 | `created_at` | TIMESTAMP | UTC timestamp when the tracking row was first created. | Reflects when the order was first tracked, typically close to order creation. |
| 7 | `updated_at` | TIMESTAMP | UTC timestamp of the most recent status change. | This is the "last status change time". Use this for recency and SLA analysis. |
| 8 | `__is_deleted` | BOOLEAN | Soft-delete flag. | Always filter: `__is_deleted = false`. |
| 9 | `year` | INTEGER | Partition column. | Always apply in WHERE clause. |
| 10 | `month` | INTEGER | Partition column. | Always apply in WHERE clause. |
| 11 | `day` | INTEGER | Partition column. | Always apply in WHERE clause. |

**Out of scope for analytics:** `__source_lsn`, `__source_timestamp`, `__data_partition`, `__data_offset`.

---

## 5. Column Value Reference

### 5a. `type` (from `services_orders`)

| Value | Meaning |
|-------|---------|
| `CHEQUE_BOOK` | Physical cheque book request. The only current value in this table. |

### 5b. `status` (from `services_trackings`)

| Value | Meaning | Analytics notes |
|-------|---------|-----------------|
| `CREATED` | Order placed; tracking row initialised. | Starting state. No fulfilment action taken yet. |
| `PENDING` | Order is being processed by the fulfilment system. | In-flight state. |
| `SUCCESS` | Order fulfilled and dispatched successfully. | Filter `status = 'SUCCESS'` for fulfilment rate analysis. |
| `FAILED` | Order could not be fulfilled. | Use `reason` column to understand failure mode. |

**Typical lifecycle:**

```
CREATED → PENDING → SUCCESS
                  ↘ FAILED
```

> `reason` is populated at every status, not just `FAILED`. When analysing failures, group by `reason` to identify the most common failure causes.

### 5c. `amount` (from `services_orders`)

| Value | Meaning |
|-------|---------|
| `'0.00'` | Order is free / charge waived. |
| `'100.00'` | Customer is charged ₹100 for the order. |

Stored as `VARCHAR`. Always `CAST(amount AS DOUBLE)` before arithmetic. Typically `0.00` when `payment_eligible = false` and `100.00` when `payment_eligible = true`.

### 5d. Delivery progress tags (derived from `services_orders`)

These are not stored columns — they are derived in queries to represent the logistics progress stage:

| Tag | Derivation | Meaning |
|-----|-----------|---------|
| `'shipment id generated'` | `shipment_id IS NOT NULL AND shipment_id <> ''` | Logistics partner has accepted the order and created a shipment. |
| `'awb id generated'` | `awb_id IS NOT NULL AND awb_id <> ''` | Item physically handed to the courier; AWB tracking active. |
| NULL | Both `shipment_id` and `awb_id` are null/empty | Order not yet dispatched to logistics. |

---

## 6. Standard Join Pattern

Because `services_trackings` has exactly one row per `order_id`, a plain `LEFT JOIN` produces exactly one row per order — no deduplication required.

Use `LEFT JOIN` (not `INNER JOIN`) to retain orders that have not yet received a tracking entry.

```sql
SELECT
    a.order_id,
    a.user_id,
    date(a.created_at + interval '330' minute)                          AS order_date_ist,
    a.type,
    a.payment_eligible,
    CAST(a.amount AS DOUBLE)                                            AS amount_rupees,
    CASE WHEN a.shipment_id IS NOT NULL AND a.shipment_id <> ''
         THEN 'shipment id generated' END                               AS shipment_id_tag,
    CASE WHEN a.awb_id IS NOT NULL AND a.awb_id <> ''
         THEN 'awb id generated' END                                    AS awb_id_tag,
    b.status,
    b.reason,
    b.updated_at                                                        AS status_last_updated_at
FROM druid_gold.services_orders AS a
LEFT JOIN druid_gold.services_trackings AS b
    ON b.order_id = a.order_id
    AND b.__is_deleted = false
    AND b.year = 2026 AND b.month = 3 AND b.day = 15
WHERE a.__is_deleted = false
  AND a.type = 'CHEQUE_BOOK'
  AND a.year = 2026 AND a.month = 3 AND a.day = 15
```

> Partition filters must be applied on **both** tables independently. Always filter `year`, `month`, `day` on both `a` and `b`.

---

## 7. Common Query Patterns

### Order volume breakdown by date, status, and reason

Replicates the logic from the production analytics query:

```sql
SELECT
    date(a.created_at + interval '330' minute)                              AS created_date,
    b.status,
    b.reason,
    CASE WHEN a.shipment_id IS NOT NULL AND a.shipment_id <> ''
         THEN 'shipment id generated' END                                   AS shipment_id_tag,
    CASE WHEN a.awb_id IS NOT NULL AND a.awb_id <> ''
         THEN 'awb id generated' END                                        AS awb_id_tag,
    COUNT(DISTINCT a.order_id)                                              AS orders
FROM druid_gold.services_orders AS a
LEFT JOIN druid_gold.services_trackings AS b
    ON b.order_id = a.order_id
    AND b.__is_deleted = false
WHERE a.__is_deleted = false
  AND a.type = 'CHEQUE_BOOK'
  AND date(a.created_at + interval '330' minute) >= date('2025-06-13')
GROUP BY 1, 2, 3, 4, 5
ORDER BY orders DESC
```

### Daily order success rate (fulfilment SR)

```sql
SELECT
    date(a.created_at + interval '330' minute)                      AS created_date,
    100.0 * SUM(CASE WHEN b.status = 'SUCCESS' THEN 1 ELSE 0 END)
           / COUNT(DISTINCT a.order_id)                             AS order_sr_pct
FROM druid_gold.services_orders AS a
LEFT JOIN druid_gold.services_trackings AS b
    ON b.order_id = a.order_id
    AND b.__is_deleted = false
WHERE a.__is_deleted = false
  AND a.type = 'CHEQUE_BOOK'
  AND date(a.created_at + interval '330' minute) >= date('2025-06-13')
GROUP BY 1
ORDER BY 1
```

### Failure analysis by reason

```sql
SELECT
    b.reason,
    COUNT(DISTINCT a.order_id) AS failed_orders
FROM druid_gold.services_orders AS a
LEFT JOIN druid_gold.services_trackings AS b
    ON b.order_id = a.order_id
    AND b.__is_deleted = false
WHERE a.__is_deleted = false
  AND a.type = 'CHEQUE_BOOK'
  AND b.status = 'FAILED'
GROUP BY 1
ORDER BY 2 DESC
```

### Chargeable orders for a given time window

```sql
SELECT
    a.order_id,
    a.user_id                                                       AS uuid,
    date(a.created_at + interval '330' minute)                      AS order_date_ist,
    CAST(a.amount AS DOUBLE)                                        AS amount_rupees,
    b.status
FROM druid_gold.services_orders AS a
LEFT JOIN druid_gold.services_trackings AS b
    ON b.order_id = a.order_id
    AND b.__is_deleted = false
WHERE a.__is_deleted = false
  AND a.type = 'CHEQUE_BOOK'
  AND a.payment_eligible = true
  AND date(a.created_at + interval '330' minute) >= date('2026-01-01')
```

### Delivery logistics progress snapshot

```sql
SELECT
    CASE
        WHEN a.awb_id IS NOT NULL AND a.awb_id <> '' THEN '3. AWB generated'
        WHEN a.shipment_id IS NOT NULL AND a.shipment_id <> '' THEN '2. Shipment generated'
        ELSE '1. Not dispatched'
    END                                                             AS logistics_stage,
    b.status,
    COUNT(DISTINCT a.order_id)                                      AS orders
FROM druid_gold.services_orders AS a
LEFT JOIN druid_gold.services_trackings AS b
    ON b.order_id = a.order_id
    AND b.__is_deleted = false
WHERE a.__is_deleted = false
  AND a.type = 'CHEQUE_BOOK'
GROUP BY 1, 2
ORDER BY 1, 2
```

---

## 8. Nuances & Gotchas

1. **`services_trackings` is one row per order, not event-sourced.** The tracking row is overwritten in place on every status change. There is no history of previous states. If you need to know "how long did an order stay in PENDING", this table cannot answer that.

2. **No `ROW_NUMBER()` deduplication needed.** Because there is exactly one tracking row per order, a plain `LEFT JOIN` is correct. Wrapping in `ROW_NUMBER()` (as seen in some older DAG code) works but is unnecessary overhead.

3. **Use `LEFT JOIN`, not `INNER JOIN`.** Orders in `CREATED` state may not have a tracking row yet in some edge cases. `INNER JOIN` would silently drop those orders from counts.

4. **`amount` is `VARCHAR`, not numeric.** Always use `CAST(amount AS DOUBLE)` before arithmetic. The known values are `'0.00'` and `'100.00'` (rupees), but the string type means direct comparison like `amount > 0` will fail or produce wrong results.

5. **`shipment_id` and `awb_id` are on `services_orders`, not `services_trackings`.** Logistics progress lives on the order table. Do not look for these columns in the trackings table.

6. **`reason` is always populated, not just on failures.** Do not assume `reason IS NULL` means success. Always filter by `status` first, then use `reason` for breakdown within that status.

7. **`id` ≠ `order_id`.** `id` is an auto-generated row PK with no business meaning. `order_id` is the business key and the only correct join column between the two tables.

8. **`user_id` exists on both tables and means the same thing.** Use `services_orders.user_id` as the primary source for user-level analysis since it is set at order creation time. Both are equivalent, but orders is the authoritative record.

9. **Partition filters on both tables.** Both tables are independently partitioned by `year`, `month`, `day`. Always apply partition filters on both sides of the join. A filter on only one table will still cause a full scan on the other.

10. **IST conversion for date analysis.** `created_at` is UTC. Always apply `+ interval '330' minute` before extracting a date for IST-based analysis. For partition-friendly date filtering, use UTC boundaries or filter on partition columns directly.

---

## 9. What is NOT Answerable from These Tables

| Question | Where to look instead |
|----------|-----------------------|
| History of status transitions for an order | Not available — `services_trackings` only stores the current state |
| Failure reason / error code detail | `reason` column has a reason string; deeper error codes are in upstream service logs |
| Account balance at time of order | `dsa_user_journey_tags.cleaned_balance` |
| User journey stage (Activated, Adopted, etc.) | `dsa_user_journey_tags.user_journey_tag` |
| User demographics / acquisition | `cohort_tags` |
| Transaction debit linked to a charged order | `casa_txn_gold.transaction` (correlate via `user_id` and order date) |
| FD / deposit orders | `druid_gold.deposit_orders` |
| Credit card transactions | `lmsdb_gold.ledger_data_credit_ac_txn` |
