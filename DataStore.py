from pyspark.sql.types import StructType, StructField, DoubleType, LongType
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import SparkSession
from Constants import *
from operator import add
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
        self.ratings_with_timestamp_df =  self.spark.read.csv(self.ratingsFile, header='true')
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
        output_rdd = temp_movies_df.rdd.map(list).map(lambda x: [x[0], removeYearFromMovie(x[1]), x[2], extractYearFromMovie(x[1])])
        # Now create a new DataFrame with MovieID, MovieTitle, MovieGenre and MovieYear columns
        self.movies_df = self.spark.createDataFrame(output_rdd, [FIELD_MOVIE_ID, FIELD_MOVIE_TITLE, FIELD_MOVIE_GENRES, FIELD_MOVIE_YEAR])

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
        self.genres_df = self.spark.createDataFrame(output_rdd, [FIELD_GENRE]).withColumn(FIELD_GENRE_ID, monotonically_increasing_id())

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
        schema = StructType([StructField(FIELD_MOVIE_ID, LongType(), False), StructField(FIELD_GENRE_ID, LongType(), False)])
        self.genre_assignment_df = self.spark.createDataFrame(
            self.movies_df.rdd
            .map(list)
            .map(lambda x:  [x[0], x[2].split("|")])
            .map(lambda x: [x[0], findGenresIDs(x[1])])
            .flatMap(lambda x: returnKeyValuePairs(x[0], x[1]))
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