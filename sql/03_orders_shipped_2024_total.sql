-- Companion to 02: distinct orders shipped in 2024 (no category split),
-- plus how many shipped orders were excluded because the shipped timestamp
-- was unusable (placeholder or malformed in the source).
SELECT
    COUNT(*) FILTER (WHERE shipped_at >= TIMESTAMP '2024-01-01'
                       AND shipped_at <  TIMESTAMP '2025-01-01') AS orders_shipped_2024,
    COUNT(*) FILTER (WHERE has_shipped AND shipped_at IS NULL)   AS shipped_with_unknown_date
FROM dim_orders;
