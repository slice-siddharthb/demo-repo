# DSA Services — `druid_gold.services_orders` + `druid_gold.services_trackings`

**Purpose:** Reference doc for individual DSA service order-level analysis. Covers order lifecycle tracking, service type classification, payment eligibility, status progression, and common patterns.

**When to use this vs other tables:**
- Use these tables when you need individual service order detail — order type, lifecycle status, payment eligibility, or per-user order history.
- Use `druid_gold.services_orders` when you need order metadata: what was ordered, when, and whether it was billable.
- Use `druid_gold.services_trackings` when you need the latest or historical status of an order (e.g. was it fulfilled, is it pending, who does it belong to?).
- For transaction-level debit/credit data tied to these orders, cross-reference `casa_txn_gold.transaction`.

---

## 1. Overall Understanding

Two production database tables that together form the complete service order record for DSA (and DCA) users. `services_orders` holds the order header (what was requested, when, and billing eligibility), while `services_trackings` holds the lifecycle status events (who the order belongs to and what state it is currently in).

**`druid_gold.services_orders`** — The order detail table. Contains metadata: order identifier, service type, timestamps, and payment eligibility flag.

**`druid_gold.services_trackings`** — The order status/lifecycle table. Contains one row per status transition for each order, including the user UUID and current status. Because orders can go through multiple states, this table has multiple rows per `order_id`; always use a `ROW_NUMBER()` window function partitioned by `order_id` ordered by `updated_at DESC, created_at DESC` to get the latest status.

They are typically joined together on `order_id` to get a complete picture of each order with its current status and associated user.

**Query engine:** Spark / Hive (SparkSQL syntax)

> **Note:** These are production database tables (not derived/built tables). The columns documented below are those observed in active analytics usage. Both tables may have additional columns — refer to the Hive metastore schema for the full column list.

---

## 2. User Base & Grain

**`services_orders`:** One row per service order. A single user can place many orders over time.

**`services_trackings`:** One row per status event per order. Multiple rows can exist for the same `order_id` as the order progresses through states (e.g. `PENDING` → `PROCESSING` → `COMPLETED`). To get a single current-state row per order, always deduplicate with a window function.

These tables cover all BSG account service orders — not just DSA. To scope to DSA users, join to `dsa_user_journey_tags` or `cohort_tags` on `user_id = uuid`.

**Soft-delete filter:** Always apply `COALESCE(__is_deleted, false) = false` on both tables.

---

## 3. Schema — `druid_gold.services_orders`

| # | Column | Data Type | Definition | Usage Notes |
|---|--------|-----------|------------|-------------|
| 1 | `order_id` | STRING | Primary identifier for the service order. | Join key to `services_trackings.order_id`. |
| 2 | `type` | STRING | The service type being ordered. | See Section 5a for known values. Primary filter for scoping to a specific service. E.g. `WHERE o.type = 'CHEQUE_BOOK'`. |
| 3 | `created_at` | TIMESTAMP | Order creation timestamp (UTC). | IST: `from_utc_timestamp(created_at, 'Asia/Kolkata')`. Use for date-range filtering; this is the canonical order date. |
| 4 | `payment_eligible` | BOOLEAN | Whether the customer is eligible to be charged for this order. | Critical for billing analytics. Filter `payment_eligible = true` when identifying chargeable orders. Senior citizen charge violation detection uses this flag. |
| 5 | `__is_deleted` | BOOLEAN | Soft-delete flag. | Always filter: `COALESCE(__is_deleted, false) = false`. |

---

## 4. Schema — `druid_gold.services_trackings`

| # | Column | Data Type | Definition | Usage Notes |
|---|--------|-----------|------------|-------------|
| 1 | `order_id` | STRING | Join key to `services_orders.order_id`. | Always join: `st.order_id = o.order_id`. |
| 2 | `user_id` | STRING | The UUID of the user who placed the order. | Join to `cohort_tags.uuid` or `dsa_user_journey_tags.uuid` for user-level enrichment. Filter `user_id IS NOT NULL AND TRIM(user_id) <> ''` to exclude system/unattributed records. |
| 3 | `status` | STRING | Current lifecycle status of the order at this tracking event. | See Section 5b for known values. Use latest-row deduplication to get current status per order. |
| 4 | `created_at` | TIMESTAMP | Timestamp when this tracking event was created (UTC). | Used for ordering to determine the latest event. |
| 5 | `updated_at` | TIMESTAMP | Timestamp when this tracking row was last updated (UTC). | Primary sort key for latest-status deduplication. Use `ORDER BY updated_at DESC, created_at DESC`. |
| 6 | `__is_deleted` | BOOLEAN | Soft-delete flag. | Always filter: `COALESCE(__is_deleted, false) = false`. |

---

## 5. Column Value Reference

### 5a. `type` (from `services_orders`) — Service Type

| Value | Meaning |
|-------|---------|
| `CHEQUE_BOOK` | Customer requested a physical cheque book. Subject to `payment_eligible` charging logic for non-senior citizens. |
| *(other values)* | Additional service types exist (e.g. debit card, statement requests). Refer to metastore for the full enum. |

> The `type` column is the primary filter for service-specific analytics. Always filter by the relevant service type before aggregating.

### 5b. `status` (from `services_trackings`) — Order Lifecycle State

| Value | Meaning |
|-------|---------|
| `PENDING` | Order received, not yet processed. |
| `PROCESSING` | Order is being fulfilled. |
| `COMPLETED` | Order fulfilled successfully. |
| `FAILED` | Order could not be fulfilled. |
| *(other values)* | Additional intermediate states may exist. Use `GROUP BY status` on a recent snapshot to enumerate live values. |

> Because `services_trackings` is event-sourced (one row per state transition), always deduplicate to the latest row per `order_id` before joining to `services_orders` for order-level analysis.

---

## 6. Standard Patterns

### 6a. Getting the Latest Status per Order (Deduplication)

This is the canonical pattern used across all analytics that needs one row per order:

```sql
WITH latest_tracking AS (
    SELECT order_id, user_id, status
    FROM (
        SELECT
            order_id,
            user_id,
            status,
            ROW_NUMBER() OVER (
                PARTITION BY order_id
                ORDER BY updated_at DESC, created_at DESC
            ) AS rn
        FROM druid_gold.services_trackings
        WHERE user_id IS NOT NULL
          AND TRIM(user_id) <> ''
          AND COALESCE(__is_deleted, false) = false
    ) t
    WHERE rn = 1
)
```

### 6b. Standard Join: Orders + Latest Tracking

```sql
SELECT
    o.order_id,
    o.type,
    from_utc_timestamp(o.created_at, 'Asia/Kolkata') AS order_created_at_ist,
    to_date(from_utc_timestamp(o.created_at, 'Asia/Kolkata'))  AS order_date_ist,
    o.payment_eligible,
    lt.user_id  AS uuid,
    lt.status   AS current_status
FROM druid_gold.services_orders o
JOIN latest_tracking lt
  ON lt.order_id = o.order_id
WHERE COALESCE(o.__is_deleted, false) = false
```

### 6c. Scoping to a Specific Service Type with Recency Filter

```sql
SELECT
    o.order_id,
    lt.user_id AS uuid,
    lt.status,
    o.payment_eligible,
    from_utc_timestamp(o.created_at, 'Asia/Kolkata') AS order_created_at_ist
FROM druid_gold.services_orders o
JOIN latest_tracking lt
  ON lt.order_id = o.order_id
WHERE o.type = 'CHEQUE_BOOK'
  AND o.payment_eligible = true
  AND o.created_at >= current_timestamp() - INTERVAL 24 HOURS
  AND COALESCE(o.__is_deleted, false) = false
```

### 6d. Joining to User Demographics (CIF / DOB lookup)

When you need customer-level attributes (e.g. date of birth for age-gating logic):

```sql
SELECT
    o.order_id,
    lt.user_id AS uuid,
    lt.status,
    CAST(c.cif AS BIGINT) AS customer_id,
    decryptFunction(ci.date_of_birth) AS decrypted_dob
FROM druid_gold.services_orders o
JOIN latest_tracking lt
  ON lt.order_id = o.order_id
LEFT JOIN uid_db_gold.customers c
  ON CAST(c.id AS STRING) = CAST(lt.user_id AS STRING)
 AND COALESCE(c.__is_deleted, false) = false
LEFT JOIN bsgcrm_gold_pii.customer_ind_info ci
  ON CAST(c.cif AS BIGINT) = ci.customer_id
 AND COALESCE(ci.__is_deleted, false) = false
WHERE COALESCE(o.__is_deleted, false) = false
```

> `uid_db_gold.customers` maps internal user UUID (`id`) to the bank's CIF (Customer Information File) number. `bsgcrm_gold_pii.customer_ind_info` holds PII fields like `date_of_birth` in encrypted form — use `decryptFunction()` to decrypt.

---

## 7. Nuances & Gotchas

1. **`services_trackings` is multi-row per order.** Every state transition creates a new row. Never join raw `services_trackings` to `services_orders` 1:1 without deduplication — you will get fan-out/duplicate order rows. Always use the `ROW_NUMBER()` deduplication pattern in Section 6a first.

2. **`user_id` can be null or empty in `services_trackings`.** Some tracking rows are system-generated or have no user attribution. Always guard with `user_id IS NOT NULL AND TRIM(user_id) <> ''` before using it as a join key.

3. **`__is_deleted` must be checked on both tables.** Apply `COALESCE(__is_deleted, false) = false` on both `services_orders` and `services_trackings` independently. Missing this on one side will include soft-deleted records from that table.

4. **`created_at` is UTC.** Always convert to IST using `from_utc_timestamp(created_at, 'Asia/Kolkata')` before extracting dates for IST-based date analysis. Use `to_date(...)` on top of that to get a date-only value.

5. **`payment_eligible` is on `services_orders`, not `services_trackings`.** Billing/charge eligibility lives on the order header. Do not look for it in the tracking table.

6. **`updated_at` takes precedence over `created_at` for deduplication ordering.** Use `ORDER BY updated_at DESC, created_at DESC` — not just `created_at DESC` — because a tracking row can be updated in-place after creation.

7. **No built-in partition columns documented.** Unlike `casa_txn_gold` tables (which are partitioned by `year`/`month`), partition strategy for these tables should be confirmed in the Hive metastore. For recency-scoped queries, always apply a time filter on `created_at` to avoid full-table scans.

---

## 8. Common Query Patterns

### Count of chargeable orders by service type in the last 24 hours

```sql
WITH latest_tracking AS (
    SELECT order_id, user_id, status
    FROM (
        SELECT
            order_id, user_id, status,
            ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC, created_at DESC) AS rn
        FROM druid_gold.services_trackings
        WHERE user_id IS NOT NULL AND TRIM(user_id) <> ''
          AND COALESCE(__is_deleted, false) = false
    ) t
    WHERE rn = 1
)
SELECT
    o.type,
    lt.status,
    COUNT(*) AS order_count
FROM druid_gold.services_orders o
JOIN latest_tracking lt ON lt.order_id = o.order_id
WHERE o.payment_eligible = true
  AND o.created_at >= current_timestamp() - INTERVAL 24 HOURS
  AND COALESCE(o.__is_deleted, false) = false
GROUP BY 1, 2
ORDER BY 3 DESC
```

### Order status distribution for a given service type

```sql
WITH latest_tracking AS (
    SELECT order_id, user_id, status
    FROM (
        SELECT
            order_id, user_id, status,
            ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC, created_at DESC) AS rn
        FROM druid_gold.services_trackings
        WHERE user_id IS NOT NULL AND TRIM(user_id) <> ''
          AND COALESCE(__is_deleted, false) = false
    ) t
    WHERE rn = 1
)
SELECT
    lt.status,
    COUNT(*) AS order_count
FROM druid_gold.services_orders o
JOIN latest_tracking lt ON lt.order_id = o.order_id
WHERE o.type = 'CHEQUE_BOOK'
  AND COALESCE(o.__is_deleted, false) = false
GROUP BY 1
ORDER BY 2 DESC
```

### First order date per user for a service type

```sql
WITH latest_tracking AS (
    SELECT order_id, user_id
    FROM (
        SELECT
            order_id, user_id,
            ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC, created_at DESC) AS rn
        FROM druid_gold.services_trackings
        WHERE user_id IS NOT NULL AND TRIM(user_id) <> ''
          AND COALESCE(__is_deleted, false) = false
    ) t
    WHERE rn = 1
)
SELECT
    lt.user_id AS uuid,
    MIN(to_date(from_utc_timestamp(o.created_at, 'Asia/Kolkata'))) AS first_order_date_ist
FROM druid_gold.services_orders o
JOIN latest_tracking lt ON lt.order_id = o.order_id
WHERE o.type = 'CHEQUE_BOOK'
  AND COALESCE(o.__is_deleted, false) = false
GROUP BY 1
```

---

## 9. What is NOT Answerable from These Tables

| Question | Where to look instead |
|----------|-----------------------|
| Transaction debit/credit linked to a service order | `casa_txn_gold.transaction` + `casa_txn_gold.transaction_state` (join via narration or user/date correlation) |
| User journey stage (Activated, Adopted, etc.) | `dsa_user_journey_tags.user_journey_tag` |
| User demographics / acquisition channel | `cohort_tags` |
| Account balance at time of order | `dsa_user_journey_tags.cleaned_balance` |
| Detailed failure reason for a failed order | Not available in these tables — refer to upstream service logs |
| FD / deposit orders | `druid_gold.deposit_orders` (separate doc) |
| Credit card transactions | `lmsdb_gold.ledger_data_credit_ac_txn` (separate doc) |
| Decrypted PII (DOB, name) | `bsgcrm_gold_pii.customer_ind_info` via `decryptFunction()` |
