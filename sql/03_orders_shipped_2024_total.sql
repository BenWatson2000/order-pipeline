-- Companion to 02: distinct orders shipped in 2024 (no category split),
-- how many of those have an inferred ship date (taken from delivered_at or
-- paid_at because the shipped timestamp was missing or unusable), and how
-- many shipped orders still have no ship date at all (excluded from 2024).
SELECT
    COUNT(*) FILTER (WHERE shipped_at >= TIMESTAMP '2024-01-01'
                       AND shipped_at <  TIMESTAMP '2025-01-01') AS orders_shipped_2024,
    COUNT(*) FILTER (WHERE shipped_at_inferred
                       AND shipped_at >= TIMESTAMP '2024-01-01'
                       AND shipped_at <  TIMESTAMP '2025-01-01') AS ship_date_inferred,
    COUNT(*) FILTER (WHERE (has_shipped OR has_delivered)
                       AND shipped_at IS NULL)                   AS shipped_with_unknown_date
FROM dim_orders;
