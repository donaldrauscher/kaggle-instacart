
from __future__ import print_function
from pyspark import SparkContext, SparkConf
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating

conf = SparkConf().setAppName("train_model")
sc = SparkContext(conf=conf)
sc.setCheckpointDir('checkpoint/') # checkpointing helps prevent stack overflow errors

# pull in data
cf_up_matrix = sc.textFile("gs://kaggle-instacart-172517/pyspark/cf_up_matrix.csv")

# extract header
header = cf_up_matrix.first() #extract header
cf_up_matrix = cf_up_matrix.filter(lambda row: row != header)
cf_up_matrix = cf_up_matrix.map(lambda l: l.split(','))

# extract ratings
cf_up_matrix_ratings = cf_up_matrix.map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2])))

# recommendations
params = {
    'rank' : 20,
    'iterations' : 20,
    'alpha' : 0.01,
    'lambda_' : 0.01
}
model = ALS.trainImplicit(cf_up_matrix_ratings, **params)

# create a list of users and current combo
users = cf_up_matrix.map(lambda x: x[0]).distinct().collect()
products = cf_up_matrix.map(lambda x: x[1]).distinct().collect()
up_combo = cf_up_matrix.map(lambda x: (x[0], x[1])).collect()

# generate predictions for each
up_recommendations = None
for user in users:
    up_combo_i = [i[1] for i in up_combo if i[0] == user]
    up_potential = [i for i in products if i not in up_combo_i]
    up_potential_pairs = sc.parallelize(up_potential).map(lambda x: (i, x[0]))
    up_recommendations_i = model.predictAll(up_potential_pairs).map(lambda p: (str(p[0]), str(p[1]), float(p[2])))
    up_recommendations_i = up_recommendations_i.takeOrdered(100, key=lambda x: -x[2])
    if (up_recommendations == None):
        up_recommendations = up_recommendations_i
    else:
        up_recommendations.extend(up_recommendations_i)

# save
up_recommendations.saveAsTextFile('gs://kaggle-instacart-172517/pyspark/cf_up_recommendations.csv')
