with orders as
(
  SELECT *
  FROM {{ref('stg_orders')}}
  WHERE
    state != 'canceled'
    AND extract(year FROM completed_at) < '2018'
    AND email NOT LIKE '%company.com'
),

order_items as
(
  SELECT * FROM {{ref('stg_order_items)}}')}}
),

order_totals as
(
  SELECT

     order_id
    ,number
    ,completed_at
    ,completed_at::DATE AS completed_at_date
    ,SUM(total) AS net_rev
    ,SUM(item_total) AS gross_rev
    ,COUNT(id) AS order_count

    FROM
      orders
    GROUP BY 1,2,3
),

orders_complete as
(
  SELECT

     order_items.order_id
    ,orders.completed_at::date as completed_at_date
    ,sum(order_items.quantity) as qty

  FROM
    order_items
    LEFT JOIN orders USING (order_id)
  WHERE
    (orders.is_cancelled_order = false OR orders.is_pending_order != true)
  GROUP BY 1,2
)

joined as
(
SELECT
   order_totals.completed_at_date
  ,order_totals.gross_rev, a.net_rev
  ,orders_complete.qty, a.order_count AS orders
  ,orders_complete.qty/a.distinct_orders AS avg_unit_per_order
  ,orders_complete.Gross_Rev/a.distinct_orders AS aov_gross
  ,orders_complete.Net_Rev/a.distinct_orders AS aov_net
FROM
  orders_complete USING (completed_at_date)
WHERE order_totals.net_rev >= 150000
)

SELECT *
FROM joined
