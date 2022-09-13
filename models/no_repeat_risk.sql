WITH
  cummulative_ordered_cte AS(
  SELECT
    user_id,
    created_at AS second_order_created_at,
    RANK() OVER (PARTITION BY user_id ORDER BY created_at) AS cummulative_orders
  FROM
    bigquery-public-data.thelook_ecommerce.orders
  WHERE
    status='Complete'),
  second_orders_created_cte AS(
  SELECT
    *
  FROM
    cummulative_ordered_cte
  WHERE
    cummulative_orders=2),
  first_orders_delivered_cte AS (
  SELECT
    user_id,
    MIN(delivered_at) AS first_order_delivered_at
  FROM
    bigquery-public-data.thelook_ecommerce.orders AS o
  WHERE
    status='Complete'
  GROUP BY
    user_id),
  wrapped_orders_cte AS (
  SELECT
    fo.user_id,
    fo.first_order_delivered_at,
    so.second_order_created_at
  FROM
    first_orders_delivered_cte AS fo
  LEFT JOIN
    second_orders_created_cte AS so
  ON
    fo.user_id=so.user_id ),
  users_order_dates_cte AS (
  SELECT
    u.*,
    w.first_order_delivered_at,
    w.second_order_created_at,
  FROM
    bigquery-public-data.thelook_ecommerce.users AS u
  JOIN
    wrapped_orders_cte AS w
  ON
    u.id=w.user_id ),
  users_order_date_diff_cte AS (
  SELECT
    *,
    TIMESTAMP_DIFF(second_order_created_at, first_order_delivered_at, DAY) AS first_second_order_days_diff
  FROM
    users_order_dates_cte ),
  users_order_stats_cte AS (
  SELECT
    *,
    AVG(first_second_order_days_diff) OVER (PARTITION BY traffic_source) AS avg_first_second_order_days_diff,
    STDDEV(first_second_order_days_diff) OVER (PARTITION BY traffic_source) AS stddev_first_second_order_days_diff,
  FROM
    users_order_date_diff_cte )
SELECT
  *,
  CASE
    WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), first_order_delivered_at, DAY) > (avg_first_second_order_days_diff + 1.5*stddev_first_second_order_days_diff) THEN 1
  ELSE
  0
END
  AS at_risk
FROM
  users_order_stats_cte
WHERE
  first_second_order_days_diff IS NULL ;