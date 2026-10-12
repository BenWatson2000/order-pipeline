-- How many customers currently have an active order?
-- Active = paid, but not yet delivered, cancelled or refunded.
-- (is_active is derived in dim_orders from which lifecycle events exist,
--  so an order with an unusable timestamp still counts correctly.)
SELECT
    COUNT(DISTINCT customer_id) AS customers_with_active_order,
    COUNT(*)                    AS active_orders
FROM dim_orders
WHERE is_active;
