import os
import sys
import urllib
import zipfile
import math
from time import time
sys.path.append("/home/kaelssss/Downloads/spark-2.0.1-bin-hadoop2.7/python")
from pyspark import SparkContext, SparkConf
from pyspark.mllib.recommendation import ALS
os.environ["SPARK_HOME"] = "/home/kaelssss/Downloads/spark-2.0.1-bin-hadoop2.7"

## functions
def get_counts_and_averages(ID_and_ratings_tuple):
    nratings = len(ID_and_ratings_tuple[1])
    return ID_and_ratings_tuple[0], (nratings, float(sum(x for x in ID_and_ratings_tuple[1]))/nratings)

## establish sparkcontext
conf = SparkConf().setMaster("local").setAppName("MovieLenTrial")
sc = SparkContext(conf = conf)

## define paths
datasets_path = os.path.join('.', 'datasets')
small_datasets_path = os.path.join(datasets_path, 'ml-latest-small')

## testing on small dataset
small_ratings_file = os.path.join(small_datasets_path, 'ratings.csv')
small_ratings_raw_data = sc.textFile(small_ratings_file)
small_ratings_raw_data_header = small_ratings_raw_data.take(1)[0]
small_ratings_data = small_ratings_raw_data.filter(lambda line: line!=small_ratings_raw_data_header)\
                     .map(lambda line: line.split(",")).map(lambda tokens: (tokens[0],tokens[1],tokens[2])).cache()

small_movies_file = os.path.join(datasets_path, 'ml-latest-small', 'movies.csv')
small_movies_raw_data = sc.textFile(small_movies_file)
small_movies_raw_data_header = small_movies_raw_data.take(1)[0]
small_movies_data = small_movies_raw_data.filter(lambda line: line!=small_movies_raw_data_header)\
    .map(lambda line: line.split(",")).map(lambda tokens: (tokens[0],tokens[1])).cache()
    
## already chosen parameters, using it on small set
training_div = 9
#validation_div = 1
test_div = 1
#training_RDD, validation_RDD, test_RDD = small_ratings_data.randomSplit([training_div, validation_div, test_div], seed=0L)
training_RDD, test_RDD = small_ratings_data.randomSplit([training_div, test_div], seed=0L)
print 'training: %s' % training_div
#print 'validating: %s' % validation_div
print 'testing: %s' % test_div
#validation_for_predict_RDD = validation_RDD.map(lambda x: (x[0], x[1]))
test_for_predict_RDD = test_RDD.map(lambda x: (x[0], x[1]))

seed = 5L
iterations = 9
regularization_parameter = 0.1
best_rank = 12
err = 0
tolerance = 0.02
t0 = time()
print 'iterations: %s' % iterations
print 'ranks: %s' % best_rank

model = ALS.train(training_RDD, best_rank, seed=seed, iterations=iterations, lambda_=regularization_parameter)
print 'training complete in %s seconds' % round(time()-t0, 3)
predictions = model.predictAll(test_for_predict_RDD).map(lambda r: ((r[0], r[1]), r[2]))
print 'prediction complete in %s seconds' % round(time()-t0, 3)
rates_and_preds = test_RDD.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)
error = math.sqrt(rates_and_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean())
print 'calculation complete in %s seconds'% round(time()-t0, 3)
print 'For testing data the RMSE is %s' % (error)
