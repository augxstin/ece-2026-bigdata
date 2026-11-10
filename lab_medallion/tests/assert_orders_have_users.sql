-- Every order must reference an existing user.
-- This test returns orders with no matching user (should return 0 rows).
select
    o.order_id,
    o.user_id
from {{ ref('stg_orders') }} as o
left join {{ ref('stg_users') }} as u on u.user_id = o.user_id
where u.user_id is null
