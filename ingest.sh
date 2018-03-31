gsutil cp data/*.csv gs://instacart-data/raw/

bq load --replace --skip_leading_rows 1 \
  instacart.order_products__prior gs://instacart-data/raw/order_products__prior.csv \
  order_id:INTEGER,product_id:INTEGER,add_to_cart_order:INTEGER,reordered:INTEGER

bq load --replace --skip_leading_rows 1 \
  instacart.order_products__train gs://instacart-data/raw/order_products__train.csv \
  order_id:INTEGER,product_id:INTEGER,add_to_cart_order:INTEGER,reordered:INTEGER

bq load --replace --skip_leading_rows 1 \
  instacart.orders gs://instacart-data/raw/orders.csv \
  order_id:INTEGER,user_id:INTEGER,eval_set:STRING,order_number:INTEGER,order_dow:INTEGER,order_hour_of_day:INTEGER,days_since_prior_order:FLOAT

bq load --replace --skip_leading_rows 1 \
  instacart.aisles gs://instacart-data/raw/aisles.csv \
  aisle_id:INTEGER,aisle:STRING

bq load --replace --skip_leading_rows 1 \
  instacart.aisles gs://instacart-data/raw/departments.csv \
  department_id:INTEGER,department:STRING

bq load --replace --skip_leading_rows 1 \
  instacart.products gs://instacart-data/raw/products.csv \
  product_id:INTEGER,product_name:STRING,aisle_id:INTEGER,department_id:INTEGER
