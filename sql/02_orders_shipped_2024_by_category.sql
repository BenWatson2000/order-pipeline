-- How many orders were shipped in 2024, split by product category?
-- An order containing items from several categories is counted once in each,
-- so the category rows can sum to more than the total number of orders.
-- Lines with a zero/negative quantity are not real items and are excluded.
-- Timestamps are stored in UTC.
SELECT
    i.category,
    COUNT(DISTINCT o.order_id) AS orders_shipped
FROM dim_orders AS o
JOIN fct_order_items AS i
  ON i.order_id = o.order_id
WHERE i.is_valid_line
  AND o.shipped_at >= TIMESTAMP '2024-01-01'
  AND o.shipped_at <  TIMESTAMP '2025-01-01'
GROUP BY i.category
ORDER BY orders_shipped DESC, i.category;
