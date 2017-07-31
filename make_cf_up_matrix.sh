# remove if exists
if bq ls instacart | grep -q "cf_up_matrix"; then
  bq rm -f -t instacart.cf_up_matrix
fi

# create dataset
bq query --use_legacy_sql=false --destination_table=instacart.cf_up_matrix "
  SELECT orders.user_id, op.product_id, COUNT(DISTINCT orders.order_id)/ANY_VALUE(orders.n_user_orders) AS order_freq
  FROM (
    SELECT *, MAX(order_number) OVER (PARTITION BY user_id) AS n_user_orders
    FROM instacart.orders
  ) AS orders
  INNER JOIN (
    SELECT user_id, RAND() AS random FROM instacart.orders GROUP BY 1 HAVING random <= 1 
  ) AS sample ON orders.user_id = sample.user_id
  INNER JOIN instacart.order_products__prior AS op ON orders.order_id = op.order_id
  INNER JOIN (
    SELECT product_id FROM (
      SELECT product_id, n_orders,
        SUM(n_orders) OVER (ORDER BY n_orders DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS n_orders_cumulative,
        (SELECT COUNT(*) FROM instacart.order_products__prior) AS n_orders_total
      FROM (
        SELECT product_id, COUNT(*) AS n_orders FROM instacart.order_products__prior GROUP BY 1
      ) AS x
    ) AS x WHERE n_orders_cumulative / n_orders_total < 0.95
  ) AS top_products ON op.product_id = top_products.product_id
  GROUP BY 1,2"

# export to gs
bq extract instacart.cf_up_matrix gs://kaggle-instacart-172517/pyspark/cf_up_matrix.csv
