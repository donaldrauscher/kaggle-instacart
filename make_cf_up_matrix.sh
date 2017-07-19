# remove if exists
if bq ls instacart | grep -q "cf_user_prod_matrix"
then
  bq rm -f -t instacart.cf_user_prod_matrix
fi

# create dataset
bq query --use_legacy_sql=false --destination_table=instacart.cf_user_prod_matrix "
  SELECT orders.user_id, op.product_id, COUNT(DISTINCT orders.order_id)/ANY_VALUE(orders.n_user_orders) AS order_freq
  FROM (
    SELECT *, MAX(order_number) OVER (PARTITION BY user_id) AS n_user_orders
    FROM instacart.orders
  ) AS orders
  INNER JOIN (
    SELECT DISTINCT user_id FROM instacart.orders WHERE RAND()<0.05
  ) AS sample ON orders.user_id = sample.user_id
  INNER JOIN instacart.order_products__prior AS op ON orders.order_id = op.order_id
  GROUP BY 1,2"

# export to gs
bq extract instacart.cf_user_prod_matrix gs://kaggle-instacart-172517/collaborative_filtering/cf_user_prod_matrix.csv
