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
    SELECT user_id, RAND() AS random FROM instacart.orders GROUP BY 1 HAVING random < 0.05
  ) AS sample ON orders.user_id = sample.user_id
  INNER JOIN instacart.order_products__prior AS op ON orders.order_id = op.order_id
  GROUP BY 1,2"

# export to gs
bq extract instacart.cf_up_matrix gs://kaggle-instacart-172517/pyspark/cf_up_matrix.csv
