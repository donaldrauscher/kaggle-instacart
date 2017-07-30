from __future__ import print_function
from pyspark.rdd import RDD
from pyspark import SparkContext, SparkConf
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from subprocess import call
import math, csv, heapq, sys

conf = SparkConf().setAppName("cf_model")
sc = SparkContext(conf=conf)
sc.setCheckpointDir('checkpoint/') # checkpointing helps prevent stack overflow errors

# pull in data
num_partitions = int(sys.argv[1])
data_dir = sys.argv[2]
data_dir = data_dir + '/' if data_dir[-1] != '/' else ''
cf_up_matrix = sc.textFile(data_dir + "/cf_up_matrix.csv", num_partitions)

# extract header
header = cf_up_matrix.first() #extract header
cf_up_matrix = cf_up_matrix.filter(lambda row: row != header)
cf_up_matrix = cf_up_matrix.map(lambda l: l.split(','))
cf_up_matrix = cf_up_matrix.map(lambda x: (int(x[0]), int(x[1]), float(x[2])))

# extract ratings
cf_up_matrix_ratings = cf_up_matrix.map(lambda l: Rating(l[0], l[1], l[2]))

# recommendations
print("Training model...")
params = {'rank' : 20, 'iterations' : 20, 'alpha' : 0.01, 'lambda_' : 0.01}
model = ALS.trainImplicit(cf_up_matrix_ratings, **params)

# create a full of user/product combos
print("Making all user-product combinations...")
users = cf_up_matrix.map(lambda x: x[0]).distinct()
products = cf_up_matrix.map(lambda x: x[1]).distinct().cache()
print(products.take(5)) # saves on every node
up_combo_full = users.cartesian(products).coalesce(num_partitions)

# filter out existing combos
print("Filtering out existing combos...")
up_combo_existing = sc.broadcast(cf_up_matrix.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda a,b: a + b).collectAsMap())
up_combo_potential = up_combo_full.filter(lambda x: x[1] not in up_combo_existing.value.get(x[0]))

# generate predictions
print("Generating predictions...")
up_rec = model.predictAll(up_combo_potential)

# take top 100 for each user
#print("Isolating top 100 products for each user...")
#up_rec_top = up_rec.map(lambda x: (x[0], (x[1], x[2]))).groupByKey().mapValues(lambda x: [i for i in sorted(x, key = lambda k: -k[1])][:100])
#up_rec_top = up_rec_top.flatMapValues(lambda x: x).map(lambda x: (x[0], x[1][0], x[1][1]))

# take top 100 for each user; use custom 'ByKey' RDD function
def takeOrderedByKey(self, num, sortKey = None):

    def init(a):
        a = [(sortKey(a), a)]
        heapq.heapify(a)
        return a

    def combine(agg, a):
        heapq.heappush(agg, a if type(a) == tuple else (sortKey(a), a))
        if len(agg) > num:
            heapq.heappop(agg)
        return agg

    def merge(agg1, agg2):
        for i in agg2:
            agg1 = combine(agg1, i)
        return agg1

    return self.combineByKey(init, combine, merge)

RDD.takeOrderedByKey = takeOrderedByKey

print("Isolating top 100 products for each user...")
up_rec_top = up_rec.map(lambda x: (x[0], x)).takeOrderedByKey(100, sortKey = lambda x: x[2])
up_rec_top = up_rec_top.flatMapValues(lambda x: x).map(lambda x: x[1][1])

# save
print("Exporting...")
export = 'cf_up_rec_top.csv'
with open(export, 'wb') as csvfile:
    csvwriter = csv.writer(csvfile, delimiter=',')
    csvwriter.writerow(['user_id', 'product_id', 'order_freq'])
    rows = up_rec_top.collect()
    for row in rows:
        csvwriter.writerow(row)
call(["gsutil", "rm", data_dir + export])
call(["gsutil", "cp", export, data_dir + export])
