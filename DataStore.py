from math import sqrt
from time import time

from pyspark.sql.types import StructType, StructField, DoubleType, LongType
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.mllib.recommendation import ALS
from pyspark.sql import SparkSession

from AnalyseRatings import getCountsAndAverages
from Constants import *
from operator import add
import itertools
import re


class DataStore:
    # It's ugly I know..replace with SQL join in the future
    genres_map = {}

    def __init__(self, ratingsFile, moviesFile):
        print("Starting Spark...")
        self.dataLoaded = False
        self.recommendationModelBuilt = False
        self.ratingsFile = ratingsFile
        self.moviesFile = moviesFile
        self.spark = SparkSession.builder.appName("MovieLens").getOrCreate()
        print("Started successfully.")

    # Loading RDDs/DataFrames
    def loadData(self):
        if self.dataLoaded:
            print("Data has been already loaded.")
            return

        print("Loading...")
        self.loadRatings()
        self.loadMovies()

        self.loadGenres()
        self.loadGenresAssignment()

        self.changeColumnTypes()
        self.cacheData()

        self.dataLoaded = True

        print("Finished loading.")

    # Converts ratings.csv into a DataFrame
    def loadRatings(self):
        print("Loading ratings.csv...")
        self.ratings_with_timestamp_df = self.spark.read.csv(self.ratingsFile, header='true')
        self.ratings_df = self.ratings_with_timestamp_df.drop(FIELD_TIMESTAMP)

    # Converts movies.csv into a DataFrame
    def loadMovies(self):

        # Find all occurrences of 4 digits (i.e year)
        def extractYearFromMovie(movie):
            result = re.findall('\((\d{4})\)', movie)
            return ''.join(result)

        # Remove those 4 digits (i.e. year)
        def removeYearFromMovie(movie):
            return re.sub('\((\d{4})\)', '', movie).strip()

        print("Loading movies.csv...")
        # Create a temporary DataFrame from movies.csv
        temp_movies_df = self.spark.read.csv(self.moviesFile, header='true')
        # Create an RDD from the above DataFrame and remove all year occurrences from the title
        output_rdd = temp_movies_df.rdd.map(list).map(
            lambda x: [x[0], removeYearFromMovie(x[1]), x[2], extractYearFromMovie(x[1])])
        # Now create a new DataFrame with MovieID, MovieTitle, MovieGenre and MovieYear columns
        self.movies_df = self.spark.createDataFrame(output_rdd, [FIELD_MOVIE_ID, FIELD_MOVIE_TITLE, FIELD_MOVIE_GENRES,
                                                                 FIELD_MOVIE_YEAR])

    # Create a separate SQL Table for all the genres...
    def loadGenres(self):
        print("Creating Genres Table...")

        # Extract Genres from the Movies DataFrame...
        output_rdd = self.movies_df.select(FIELD_MOVIE_GENRES).rdd.map(list) \
            .flatMap(lambda x: x[0].split('|')) \
            .map(lambda x: (x, 1)) \
            .reduceByKey(add) \
            .filter(lambda x: x[0] != NO_GENRE)

        # Create the Genres DataFrame: GenreID Genre
        self.genres_df = self.spark.createDataFrame(output_rdd, [FIELD_GENRE]).withColumn(FIELD_GENRE_ID,
                                                                                          monotonically_increasing_id())

        # For easier processing the Genre mapping is also stored in-memory...
        # Spark uses lazy initialisation i.e. performs calculations only when required which means
        # all tasks in the program are performed only when the data is requested. This is not very user friendly.
        # Hence, the below function manually triggers the calculation on the Data Frame to save into memory.
        # The downside is that the start-up time of the program is increased, the upside is the performance once
        # the program loaded.
        count = 0
        for entry in self.genres_df.select(FIELD_GENRE).rdd.flatMap(lambda x: x).collect():
            self.genres_map[entry] = count
            count += 1

    # Create a separate SQL Table where movieIds are mapped to their corresponding genreIds
    def loadGenresAssignment(self):
        print("Mapping Movie Table to Genres Table...")

        # Local copy so that Spark does not have to send the whole DataStore object
        local_genres_map = self.genres_map

        # Finds the IDs of all genres and puts them in a list
        def findGenresIDs(genres):
            genres_ids = []
            for genre in genres:
                if genre == NO_GENRE:
                    continue
                genres_ids.append(local_genres_map[genre])
            return genres_ids

        # Turns a key and a list of values into
        def returnKeyValuePairs(key, values):
            kv_pairs = []
            for value in values:
                kv_pairs.append([long(key), long(value)])
            return kv_pairs

        # Extract the genres list from the Movies
        # Search for the correct genreId
        # Put movieId and all genreIds into an association table
        schema = StructType(
            [StructField(FIELD_MOVIE_ID, LongType(), False), StructField(FIELD_GENRE_ID, LongType(), False)])
        self.genre_assignment_df = self.spark.createDataFrame(
            self.movies_df.rdd
                .map(list)
                .map(lambda x: [x[0], x[2].split("|")])  # split genre names into separate entries
                .map(lambda x: [x[0], findGenresIDs(x[1])])  # replace genre name with the corresponding id
                .flatMap(lambda x: returnKeyValuePairs(x[0], x[1]))  # map into a list
            , schema)

        # Now remove the "genres" column from the Movies
        self.movies_df = self.movies_df.drop(FIELD_MOVIE_GENRES)

    # BigInt is used for columns as the method monotonically_increasing_id needs 64bits.
    # To have the IDs unified they are all converted to BigInt userId:bigint, movieId: bigint, rating: double
    def changeColumnTypes(self):
        self.ratings_df = self.ratings_df.withColumn(FIELD_USER_ID, self.ratings_df[FIELD_USER_ID].cast(LongType())) \
            .withColumn(FIELD_MOVIE_ID, self.ratings_df[FIELD_MOVIE_ID].cast(LongType())) \
            .withColumn(FIELD_RATING, self.ratings_df[FIELD_RATING].cast(DoubleType()))

        # movieId:bigint, title:string, year:string
        self.movies_df = self.movies_df.withColumn(FIELD_MOVIE_ID, self.movies_df[FIELD_MOVIE_ID].cast(LongType()))

    # As previously mentioned in loadGenres() the below will manually trigger caching in Spark as the cache()
    # instruction does not actively cache, it only tells Spark to cache the table after it has been used...
    def cacheData(self):
        print("Caching data...")

        self.movies_df.cache()
        self.ratings_df.cache()
        self.genres_df.cache()
        self.genre_assignment_df.cache()

        movie_count = self.movies_df.count()
        rating_count = self.ratings_df.count()
        genre_count = self.genres_df.count()
        genre_assignment_count = self.genre_assignment_df.count()

        total_count = str(movie_count + rating_count + genre_count + genre_assignment_count)
        print("Cached " + total_count + " data units")

    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    # Movie Recommendations...

    def buildRecommendationModel(self):

        if self.recommendationModelBuilt:
            print("Recommendation Model has been already built!")
            return

        # Some basic info about the data...note there are lots of many different ways in which you can count fields...
        numRatings = self.ratings_df.count()
        numUsers = self.ratings_df.select(self.ratings_df.userId).distinct().count()
        numMovies = self.ratings_df.select(self.ratings_df.movieId).distinct().count()
        print("There are " + str(numRatings) + " ratings from " + str(numUsers) + " users on " + str(
            numMovies) + " movies.")

        print("Building Recommendation Model...")

        # We need to create three subsets: training, validation and test
        # Im splitting the data set into the above three based on a new column
        ratings_df = self.ratings_df.withColumn(FIELD_ID, monotonically_increasing_id())
        ratings_df = ratings_df.withColumn(FIELD_ID, ratings_df[FIELD_ID] % 10)

        # Now I will use most of the data for training 60%
        training_rdd = ratings_df \
            .filter(ratings_df[FIELD_ID] < 6) \
            .drop(FIELD_ID) \
            .rdd.map(list) \
            .cache()

        # 20% for validation
        validation_rdd = ratings_df \
            .filter(ratings_df[FIELD_ID] >= 6) \
            .filter(ratings_df[FIELD_ID] < 8) \
            .drop(FIELD_ID) \
            .rdd.map(list) \
            .cache()

        # And 20% for test
        test_rdd = ratings_df \
            .filter(ratings_df[FIELD_ID] >= 8) \
            .drop(FIELD_ID) \
            .rdd.map(list) \
            .cache()

        # Trigger caching by running the count calculation on the above sets i.e.
        # printing out the number of data entries for each set
        training_count = training_rdd.count()
        validation_count = validation_rdd.count()
        test_count = test_rdd.count()
        print("Using " + str(training_count) + " entries for training, " + str(validation_count) +
              " entries for validation and " + str(test_count) + " entries for test")

        # RMSE - Root Mean Square Error
        # model - MatrixFactorizationModel (predicted RDD)
        # data  - RDD[userId, movieId, rating] (actual RDD)
        # n     - the number of entries
        # Use this method to compute the RMSE on the validation set for each model.
        # The model with the smallest RMSE on the validation set becomes the one selected
        # and its RMSE on the test set is used as the final metric.
        def computeError(model, data, n):
            predictions = model.predictAll(data.map(lambda x: (x[0], x[1])))
            predictionsAndRatings = predictions.map(lambda x: ((x[0], x[1]), x[2])) \
                .join(data.map(lambda x: ((x[0], x[1]), x[2]))) \
                .values()
            return sqrt(predictionsAndRatings.map(lambda x: (x[0] - x[1]) ** 2).reduce(add) / float(n))

        # Now we will use ALS.train to train a bunch of models, and select and evaluate the best one.
        # Among the training parameters of ALS, the most important ones are rank, lambda (regularization constant),
        # and number of iterations
        # For ml-latest-small:
        ranks = [8, 12]
        lambdas = [0.1, 10.0]
        numIters = [10, 20]

        bestModel = None
        bestValidationError = float("inf")
        bestRank = 0
        bestLambda = -1.0
        bestNumIter = -1

        # The train() method of ALS we are going to use is defined as follows:
        # def train(cls, ratings, rank, iterations=5, lambda_=0.01, blocks=-1, nonnegative=False, seed=None):
        #   (...)
        #   return MatrixFactorizationModel(model)
        #
        # Train a matrix factorization model given an RDD of ratings by users
        # for a subset of products. The ratings matrix is approximated as the
        # product of two lower-rank matrices of a given rank (number of features)
        # To solve for these features, ALS is run iteratively with a configurable level of parallelism.
        # :param ratings:
        #           RDD of `Rating` or (userID, productID, rating) tuple.
        # :param rank:
        #           Number of features to use (also referred to as the number of latent factors).
        # :param iterations:
        #           Number of iterations of ALS (default: 5)
        # :param lambda_:
        #           Regularization parameter (default: 0.01)
        # :param blocks:
        #           Number of blocks used to parallelize the computation.
        #           A value of -1 will use an auto-configured number of blocks. (default: -1)
        # :param nonnegative:
        #           A value of True will solve least-squares with nonnegativity constraints (default: False)
        # :param seed:
        #           Random seed for initial matrix factorization model.
        #           A value of None will use system time as the seed (default: None)

        # Ideally, we want to try a large number of combinations in order to find the best one.
        # Due to time constraint, we will test only 8 combinations resulting from the cross product
        # of 2 different ranks (8 and 12), 2 different lambdas (1.0 and 10.0),
        # and two different numbers of iterations (10 and 20)
        # We use the computeError method to compute the RMSE on the validation set for each model.
        # As previously mentioned the model with the smallest RMSE on the validation set
        # becomes the one selected and its RMSE on the test set is used as the final metric.

        # Train the model (Spark might take a minute or two to train the models...)
        for rank, lmbda, numIter in itertools.product(ranks, lambdas, numIters):
            model = ALS.train(training_rdd, rank, numIter, lmbda)
            validationError = computeError(model, validation_rdd, validation_count)
            print("For rank " + str(rank) + " the RMSE is " + str(validationError))
            if validationError < bestValidationError:
                bestModel = model
                bestValidationError = validationError
                bestRank = rank
                bestLambda = lmbda
                bestNumIter = numIter

        # --------------------------------------------------------------------------------------------------------------
        # Testing the model...
        # Compute RMSE for the test sub-set...
        testError = computeError(bestModel, test_rdd, test_count)

        # Evaluate the best model on the test sub-set... i.e. testing your model
        print("The best model was trained with rank " + str(bestRank) + " and lambda " + str(bestLambda)
              + " and numIter " + str(bestNumIter) + " and its RMSE on the test sub-set is " + str(testError))

        # Compare the best model with the naive baseline that always returns average rating
        averageRating = training_rdd.union(validation_rdd).map(lambda x: x[2]).mean()
        baselineError = sqrt(test_rdd.map(lambda x: (averageRating - x[2]) ** 2).reduce(add) / test_count)
        improvement = (baselineError - testError) / baselineError * 100
        print("The best model improves the baseline by %.2f" % improvement + "%.")
        # --------------------------------------------------------------------------------------------------------------

        # Now we are ready to make personalized recommendations...
        # testRDDForPredictions = test_rdd.map(lambda x: (x[0], x[1]))
        # predictions = bestModel.predictAll(testRDDForPredictions).map(lambda r: ((r[0], r[1]), r[2]))
        # In the above snippet of code basically we have the UserID, the MovieID, and the Rating.
        # In this case the predictions third element, the rating for that movie and user,
        # is one predicted by our ALS model, so predictions.take(3) would return
        # [((32, 4018), 3.280114696166238),
        #  ((375, 4018), 2.7365714977314086),
        #  ((674, 4018), 2.510684514310653)]

        # Free the RDDs...
        training_rdd.unpersist()
        validation_rdd.unpersist()
        test_rdd.unpersist()

        # Set the best model for further use...
        print("The Movie Recommendation Model has been built successfully!")
        self.bestRank = bestRank
        self.bestLambda = bestLambda
        self.bestNumIter = numIter
        self.recommendationModel = bestModel
        self.recommendationModelBuilt = True

    # Provides movie recommendations given a list of new ratings below (myRatedMovies)...
    # Put them in a new RDD and assign the user ID 0, that is not assigned in the MovieLens data-set.
    def recommendMovies(self):

        if not self.recommendationModelBuilt:
            return "You must build a recommendation model first!"

        # The format of each line is (userID, movieID, rating)
        myUserID = 0
        myRatedMovies = [
            (myUserID, 72998, 2),  # Avatar
            (myUserID, 318, 5),  # Shawshank Redemption
            (myUserID, 2628, 2),  # Star Wars: Episode I - The Phantom Menace
            (myUserID, 5378, 5),  # Star Wars: Episode II - Attack of the Clones
            (myUserID, 33493, 1),  # Star Wars: Episode III - Revenge of the Sith
            (myUserID, 260, 2),  # Star Wars: Episode IV - A New Hope
            (myUserID, 1196, 4),  # Star Wars: Episode V - The Empire Strikes Back
            (myUserID, 1210, 5),  # Star Wars: Episode VI - Return of the Jedi
            (myUserID, 1088, 4),  # Dirty Dancing
            (myUserID, 4993, 3),  # Lord of the Rings: The Fellowship of the Ring
            (myUserID, 5952, 2),  # Lord of the Rings: The Two Towers
            (myUserID, 7153, 3)]  # Lord of the Rings: The Return of the King

        # Create a new RDD (userID, movieID, rating)
        myRatings_rdd = self.spark.sparkContext.parallelize(myRatedMovies)

        # --------------------------------------------------------------------------------------------------------------

        # ratings_df -> ratings_rdd
        ratings_rdd = self.ratings_df.rdd.map(list)

        # movies_df -> movies_rdd
        movies_rdd = self.movies_df.rdd.map(list)

        # Add myRatings_rdd to ratings_rdd to form complete_rating_rdd ...
        complete_ratings_rdd = ratings_rdd.union(myRatings_rdd)

        # Train the ALS model using the best parameters ...
        # It will take some time and we will need to repeat that every time a user adds new ratings ...
        # Ideally we will do this in batches, and not for every single rating that comes into the system for every user
        newRecommendationModel = ALS.train(complete_ratings_rdd, self.bestRank, self.bestNumIter, self.bestLambda)

        # Use myRatedMovies to transform the movies_rdd into an RDD with entries that are pairs of the form
        # (myUserID, Movie ID) also leave only movies that we have not rated yet...
        myRatedMoviesIDs = map(lambda x: x[1], myRatedMovies)
        myUnratedMovies_rdd = (movies_rdd.filter(lambda x: x[0] not in myRatedMoviesIDs)) \
            .map(lambda x: (myUserID, x[0]))

        # Use myUnratedMovies_rdd, with myRatingsModel.predictAll() to predict your ratings for the movies ...
        # [Rating(user=0, product=161944, rating=2.998018770230485),
        #  Rating(user=0, product=43832, rating=2.4365307166512036), ...]
        predictedRatings_rdd = newRecommendationModel.predictAll(myUnratedMovies_rdd)

        # With the above we pretty much have our recommendations ready! Now we can simply print n movies
        # with the highest predicted ratings.
        # --------------------------------------------------------------------------------------------------------------
        # However,
        # 1) let's join predictedRatings_rdd with the movies_rdd to get the titles ...
        # 2) and let's get the ratings count for each movie so that we can recommend movies
        # with only a certain minimum number of ratings ... e.g. no less than 100

        # From ratings_rdd with tuples of (UserID, MovieID, Rating) create an RDD with tuples of
        # the (MovieID, iterable of Ratings for that MovieID)
        movieIDsWithRatings_RDD = (ratings_rdd.map(lambda rating: (rating[1], rating[2])).groupByKey())

        # Using `movieIDsWithRatingsRDD`, compute the number of ratings and average rating for each movie to
        # yield tuples of the form (MovieID, (number of ratings, average rating))
        movieIDsWithAverageRatings_RDD = movieIDsWithRatings_RDD.map(getCountsAndAverages)

        # Transform movieIDsWithAverageRatings_RDD into an RDD of the form (MovieID, number of ratings)
        moviesRatingCounts_RDD = movieIDsWithAverageRatings_RDD.map(
            lambda (MovieID, (NumRating, AvgRating)): (MovieID, NumRating))

        # Transform predictedRatings_rdd into an RDD with entries that are pairs of (Movie ID, Predicted Rating)
        movieIDsAndPredictedRatings_RDD = predictedRatings_rdd.map(lambda (UserID, MovieID, Rating): (MovieID, Rating))

        # Use RDD transformations with movieIDsAndPredictedRatings_RDD and moviesRatingCounts_RDD
        # to yield an RDD with tuples of the form (Movie ID, (Predicted Rating, number of ratings))
        predictedWithCounts_RDD = (movieIDsAndPredictedRatings_RDD.join(moviesRatingCounts_RDD))

        # Use RDD transformations with PredictedWithCountsRDD and moviesRDD to yield an RDD with tuples of the form
        # (Predicted Rating, Movie Name, number of ratings), for movies with more than 75 ratings
        ratingsWithNamesRDD = (predictedWithCounts_RDD.join(movies_rdd).map(
            lambda (m, ((PredictedRating, NumRating), MovieName)): (PredictedRating, MovieName, NumRating)).filter(
            lambda (PredictedRating, MovieName, NumRating): NumRating > 75))

        predictedHighestRatedMovies = ratingsWithNamesRDD.takeOrdered(20, key=lambda x: -x[0])

        print('My highest rated movies as predicted (for movies with more than 75 reviews):\n%s' % \
              '\n'.join(map(str, predictedHighestRatedMovies)))
