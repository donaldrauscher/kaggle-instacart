from google.cloud import bigquery
import yaml, time, hashlib

# load the config and meta
with open('config.yaml', 'r') as f:
    config = yaml.load(f)

with open('meta.yaml', 'r') as f:
    meta = yaml.load(f)

# set up client and dataset
client = bigquery.Client(project=config['project_id'])
dataset = client.dataset(dataset_name=meta['bq_dataset'])

# set up query and parameters
prop_users = bigquery.ScalarQueryParameter('prop_users', 'FLOAT', meta['prop_users'])
prop_products = bigquery.ScalarQueryParameter('prop_products', 'FLOAT', meta['prop_products'])
params = [prop_users, prop_products]
sql = """
  SELECT orders.user_id, op.product_id, COUNT(DISTINCT orders.order_id)/ANY_VALUE(orders.n_user_orders) AS order_freq
  FROM (
    SELECT *, MAX(order_number) OVER (PARTITION BY user_id) AS n_user_orders
    FROM instacart.orders
  ) AS orders
  INNER JOIN (
    SELECT user_id, RAND() AS random FROM instacart.orders GROUP BY 1 HAVING random <= @prop_users
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
    ) AS x WHERE n_orders_cumulative / n_orders_total <= @prop_products
  ) AS top_products ON op.product_id = top_products.product_id
  GROUP BY 1,2
"""

# function to wait for asynchronous jobs
def wait_for_async_job(job):
    while True:
        job.reload()
        if job.state == "DONE":
            break
        time.sleep(30)

# to generate slug for jobs based on timestamp
def slug_generator():
    hash = hashlib.sha1()
    hash.update(str(time.time()))
    return hash.hexdigest()[:10]

# generate table
print('Create cf_up_matrix table in BQ...')
table = dataset.table(name='cf_up_matrix')
slug = slug_generator()
print('Random slug for jobs is %s.' % slug)
job = client.run_async_query(job_name='create-cf-up-matrix-'+slug, query=sql, query_parameters=params)
job.destination = table
job.use_legacy_sql = False
job.write_disposition = 'WRITE_TRUNCATE'
job.begin()
wait_for_async_job(job)
assert job.error_result is None
print('Done.')

# download table
print('Exporting cf_up_matrix table to GCS...')
export_path = config['pyspark_filepath'] + '/cf_up_matrix.csv'
job2 = client.extract_table_to_storage('extract-cf-up-matrix-'+slug, table, export_path)
job2.destination_format = 'CSV'
job2.print_header = True
job2.write_disposition = 'WRITE_TRUNCATE'
job2.begin()
wait_for_async_job(job2)
assert job2.error_result is None
print('Done.')
