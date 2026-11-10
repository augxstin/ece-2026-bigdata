{{ config(severity = 'warn') }}

-- A user cannot place an order before their birthdate.
-- This test returns orders with ordered_at < birthdate (should return 0 rows).
select
    o.order_id,
    o.ordered_at,
    u.birthdate
from {{ ref('stg_orders') }} as o
inner join {{ ref('stg_users') }} as u on u.user_id = o.user_id
where o.ordered_at < u.birthdate
