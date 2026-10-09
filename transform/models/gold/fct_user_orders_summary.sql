SELECT
    u.user_uuid,
    u.name,
    u.email,
    COUNT(o.order_uuid) AS total_orders,
    SUM(o.quantity) AS total_items_ordered
FROM {{ ref('stg_users') }} u
LEFT JOIN {{ ref('stg_orders') }} o ON u.user_uuid = o.user_uuid
GROUP BY u.user_uuid, u.name, u.email
