import os
import sys
import urllib
import zipfile
import math
import random
from time import time
sys.path.append("/home/kaelssss/Downloads/spark-2.0.1-bin-hadoop2.7/python")
from pyspark import SparkContext, SparkConf
from pyspark.mllib.recommendation import ALS
os.environ["SPARK_HOME"] = "/home/kaelssss/Downloads/spark-2.0.1-bin-hadoop2.7"
# For running the engine, please set the paths properly in your directory


class RecommendationEngine:
    ## These are parameters used for ALS recommendation
    rank = 2
    seed = 1L
    iterations = 2
    regularParameter = 0.1
    shotsPerRound = 5
    VIP = set()



    def getCountsandAverages(self, tuple_IDRatings):
        ## Input: (musicID, Iterable<its ratings>);
        ## Output: (musicID, (number of its ratings, its average rating))
        nratings = len(tuple_IDRatings[1])
        return tuple_IDRatings[0], (nratings, float(sum(x for x in tuple_IDRatings[1]))/nratings)



    def getCounts(self, tuple_IDRatings):
        ## Input: (musicID, Iterable<its ratings>);
        ## Output: (musicID, number of its ratings)
        nratings = len(tuple_IDRatings[1])
        return tuple_IDRatings[0], nratings



    def helper__CountandAverageRatings(self):
        ## Helper function: update RDD_musicRatingCounts: (musicID, number of its ratings)
        RDD_musicIDwithRatings = self.RDD_ratings.map(lambda x: (x[1], x[2])).groupByKey()
        #RDD_musicIDwithAverageRatings = RDD_musicIDwithRatings.map(lambda x: self.getCountsandAverages(x))
        RDD_musicIDwithCountsRatings = RDD_musicIDwithRatings.map(lambda x: self.getCounts(x))
        self.RDD_musicRatingCounts = RDD_musicIDwithCountsRatings.map(lambda x: (x[0], x[1]))


  
    def helper__TrainModel(self):
        ## Helper function:
        currTime = time()
        print 'training started'
        self.model = ALS.train(self.RDD_ratings, self.rank, seed=self.seed, iterations=self.iterations, lambda_=self.regularParameter)
        print 'training complete: %s' %round(time()-currTime, 3)
        


    def __init__(self):
        ## Init the engine:
        ## just loading the original ratings and items csv;
        ## and calculate the original RDDs

        # establish sparkcontext
        conf = SparkConf().setMaster("local").setAppName("MusicRecc")
        self.sc = SparkContext(conf = conf)
        # define paths
        # current version: testing on small ml dataset because of limited computing resources
        datasetPathOrg = os.path.join('.', 'datasets')
        datasetPath = os.path.join(datasetPathOrg, 'ml-latest-small')
        # Load ratings data for later use
        ratingFilePath = os.path.join(datasetPath, 'ratings.csv')
        RDD_ratingsWithHeader = self.sc.textFile(ratingFilePath)
        ratingsHeader = RDD_ratingsWithHeader.take(1)[0]
        self.RDD_ratings = RDD_ratingsWithHeader.filter(lambda line: line!=ratingsHeader)\
            .map(lambda line: line.split(","))\
            .map(lambda tokens: (int(tokens[0]),int(tokens[1]),float(tokens[2]))).cache()
        # Load music data for later use
        # For real use, change another title file; For testing, small ml dataset again
        titlesetPath = os.path.join(datasetPath, 'movies.csv')
        RDD_titlesWithHeader = self.sc.textFile(titlesetPath)
        titlesHeader = RDD_titlesWithHeader.take(1)[0]
        self.RDD_music = RDD_titlesWithHeader.filter(lambda line: line!=titlesHeader)\
            .map(lambda line: line.split(","))\
            .map(lambda tokens: (int(tokens[0]),tokens[1],tokens[2])).cache()
        self.RDD_musicTitles = self.RDD_music.map(lambda x: (int(x[0]),x[1])).cache()
        # Pre-calculate music ratings counts
        self.helper__CountandAverageRatings()



    def add_ratings(self, ratings):
        ## Input: New ratings in list of 3-elem lists
        ## Behavior: Update counting info and retrain the model

        # Update ratings from the new incoming ratings
        RDD_newRatings = self.sc.parallelize(ratings)
        self.RDD_ratings = self.RDD_ratings.union(RDD_newRatings)
        # Update music ratings count
        self.helper__CountandAverageRatings()
        # Update the ALS model with the new ratings
        self.helper__TrainModel()



    def helper__predictRatings(self, RDD_userandMusic):
        ## Input: (userID, musicID)s in RDD
        ## Behavior: use current model to predict all undone ratings
        ## Output: (musicID, musicTitle, musicRating) in RDD from this user(predicted) to music
        
        RDD_predicted = self.model.predictAll(RDD_userandMusic)
        RDD_predictedRatings = RDD_predicted.map(lambda x: (x.product, x.rating))
        RDD_predictedRatingswithTitle = RDD_predictedRatings.join(self.RDD_musicTitles)
        #RDD_predictedRatingswithCount = RDD_predictedRatings.join(self.RDD_musicRatingCounts)
        #RDD_predictedRatingswithTitleandCount = RDD_predictedRatingswithTitleandCount.join(self.RDD_musicRatingCounts)
        #RDD_predictedRatingswithTitleandCount = RDD_predictedRatingswithTitleandCount.map(lambda r: (r[0], r[1][0][1], r[1][0][0], r[1][1]))
        RDD_predictedRatingswithTitle = RDD_predictedRatingswithTitle.map(lambda r: (r[0], r[1][1], r[1][0]))
        return RDD_predictedRatingswithTitle



    def get_top_ratings(self, userID):
        ## Input: userID
        ## Output: RDD of the highest *shotsPerRound* unrated music given in top-down order
        
        # Get pairs of (userID, musicID) for userID's unrated music
        RDD_unrated = self.RDD_ratings.filter(lambda rating: not rating[0]==userID).map(lambda x: (userID, x[1]))
        # Get predicted ratings
        RDD_predictedRatingswithTitleandCount = self.helper__predictRatings(RDD_unrated)
        #topRatingswithRatings = RDD_predictedRatingswithTitleandCount.takeOrdered(self.shotsPerRound, key=lambda x: -x[2])
        topRatingswithRatings = RDD_predictedRatingswithTitleandCount.takeOrdered(20, key=lambda x: -x[2])
        seen = set()
        topRatingswithRatingsDistinct = []
        cnt = 0
        for entry in topRatingswithRatings:
            if cnt >= self.shotsPerRound:
                break
            if entry[0] not in seen:
                topRatingswithRatingsDistinct.append(entry)
                seen.add(entry[0])
                cnt += 1
                
        RDD_topRatings = self.sc.parallelize(topRatingswithRatingsDistinct)
        return RDD_topRatings



    def get_predicted_ratings_for_musicID(self, userID, musicIDs):
        ## Input: userID, a list of musicIDs
        ## Output: List of predictions from this user to those music
        
        RDD_requestedID = self.sc.parallelize(musicIDs).map(lambda x: (userID, x))
        # Get predicted ratings
        ratings = self.helper__predictRatings(RDD_requestedID).collect()
        return ratings



    def get_url_for_musicID(self, titles):
        ## Input: a list of titles
        ## Output: List of url strings in Google for these titles

        urls = []
        for title in titles:
            urls.append("https://www.google.com/search?q="+title)
        return urls



    def get_proposed_ID_and_title(self, userID):
        ## The ultimate function aside from establishing RecommendationEnigine itself
        # Server only needs to call this from outside

        if(userID in self.VIP):
            print 'Here are %s recommended music for you' % self.shotsPerRound
            return self.get_top_ratings(userID).map(lambda x: (x[0], x[1])).collect()
        else:
            self.VIP.add(userID)
            print 'Welcome new user! Here are %s randomly selected music for you to start' % self.shotsPerRound
            return self.RDD_musicTitles.takeSample(False, self.shotsPerRound)


    






    
