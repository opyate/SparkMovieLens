from Constants import *
from AnalyseRatings import findMovieAverageRating, findMovieViewCount
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType


# Given movieId, check if it exists in the database, find out some useful info such as
# - the number of viewings,
# - average rating
# - and obviously the title
def findMovieById(datastore, movieId):
    if not checkIfMovieIdExist(datastore, movieId):
        return "Unknown Movie ID"
    viewCount = findMovieViewCount(datastore, movieId)
    rating = findMovieAverageRating(datastore, movieId)
    title = findMovieTitleById(datastore, movieId)
    return "ID: " + str(movieId) + "; Title: " + title + "; Average Rating: " + str(rating) + "; View Count: " + str(
        viewCount)


def checkIfMovieIdExist(datastore, movieId):
    movies_df = datastore.movies_df
    return movies_df.where(movies_df[FIELD_MOVIE_ID] == movieId).count() == 1


# Quite simply filtering this time using where function
# The optimization here is that I fetch the title from only the first entry I encounter
# which is stored in the second field, hence collect()[0][1]
def findMovieTitleById(datastore, movieId):
    movies_df = datastore.movies_df
    return movies_df \
        .where(movies_df[FIELD_MOVIE_ID] == movieId) \
        .rdd \
        .map(list) \
        .collect()[0][1]


# Given a genre or a list of genres, find the corresponding movies :)
# First of all, make the list unique and lower case to ease the search
def findMoviesByGenre(datastore, genres):
    genresList = list(set(genres.split()))
    genresList = map(lambda x: x.lower(), genresList)

    # Some movies may not have a genre, weird I know but what genre would you give to Borat
    # ...some find it too offensive to be a comedy ... jokes xD
    def checkGenreExists(genre):
        if genre == '': return False
        return genre.lower() in genresList

    genreFilter = udf(checkGenreExists, BooleanType())
    numOfGenres = len(genresList)

    movies_df = datastore.movies_df
    genres_df = datastore.genres_df
    assignments_df = datastore.genre_assignment_df

    # Contains movieId and the number of genres they are assigned to
    # Only genres we are searching for are taken into consideration
    movies_genre_df = genres_df \
        .filter(genreFilter(genres_df[FIELD_GENRE])) \
        .join(assignments_df, FIELD_GENRE_ID) \
        .join(movies_df, FIELD_MOVIE_ID) \
        .groupBy(FIELD_MOVIE_ID) \
        .count()

    # Search for the remaining movies based on the movieId ...
    toPrint = movies_genre_df \
        .filter(movies_genre_df[FIELD_COUNT] == numOfGenres) \
        .join(movies_df, FIELD_MOVIE_ID) \
        .orderBy(FIELD_MOVIE_ID) \
        .rdd \
        .map(list) \
        .map(lambda x: "ID: " + str(x[0]) + "; Title: " + x[2]) \
        .collect()

    # Pretty print please...
    if len(toPrint) == 0:
        toPrint = "No movies match the criteria " + str(genres)
    else:
        toPrint.insert(0, "The folowing were found:")
    return toPrint


# Returns a specified number of the most watched movies
# Create a temporary DataFrame with n most watched movies ordered by the number of viewings
# Join the resulting table with with the movie table, and since the join destroys the order, reorder the resulting table
def getMostWatchedMovies(datastore, n):
    ratings_df = datastore.ratings_df
    movies_df = datastore.movies_df

    topNMostWatchedMovies_df = datastore.spark.createDataFrame(
        ratings_df.groupBy(FIELD_MOVIE_ID).count().orderBy(FIELD_COUNT, ascending=False).head(n)
    )

    topNMovies_df = topNMostWatchedMovies_df.join(movies_df, FIELD_MOVIE_ID).orderBy(FIELD_COUNT, ascending=False)

    # Extract the most important information to create the below :) fun!
    # +-------+-----+--------------------+----+
    # |movieId|count|               title|year|
    # +-------+-----+--------------------+----+
    return topNMovies_df \
        .rdd \
        .map(list) \
        .map(lambda x: "Title: " + x[2] + " ID: " + str(x[0]) + " Views: " + str(x[1])) \
        .collect()
