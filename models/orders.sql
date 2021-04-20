with orders as (
    select * from {{ ref('stg_orders')}}
),

payments as (
    select * from {{ ref('stg_payments')}}
),

final as (
    select 
        o.order_id,
        o.customer_id,
        o.order_date,
        sum(p.amount) as total_amount
    from orders as o
    left join payments as p
        on o.order_id = p.order_id
    where p.status <> 'fail'
    group by o.order_id, o.customer_id, o.order_date

)

select * from final