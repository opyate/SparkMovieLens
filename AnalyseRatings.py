from Constants import *
# Important!
# When you convert data frame back to rdd: rdd = df.rdd.map(list)
# collect() returns a list that contains all of the elements in the provided RDD
# This method should only be used if the resulting array is expected to be small, as all the data is loaded in memory
# So basically, after a filter or other operation that returns a sufficiently small subset of the data
# .head()[0] / .collect()[0][0] are equivalent in this case I think, they only fetch the first element


# Given movieId, find out the number of occurrences i.e. the number of viewings in the ratings_df
def findMovieViewCount(datastore, movieId):
    ratings_df = datastore.ratings_df
    return ratings_df \
        .filter(ratings_df[FIELD_MOVIE_ID] == movieId) \
        .count()


# Given movieId, find out the average rating by filtering the ratings_df for that id
# and taking average over the existing ratings (groupBy avg i.e. group by average)
def findMovieAverageRating(datastore, movieId):
    ratings_df = datastore.ratings_df
    rating = ratings_df \
        .filter(ratings_df[FIELD_MOVIE_ID] == movieId) \
        .groupBy() \
        .avg(FIELD_RATING) \
        .rdd \
        .map(list) \
        .collect()[0][0]

    if rating is None:
        return "NOT RATED"
    else:
        return str(round(rating, 2))


# Return n movies with highest average rating...
def getHighestRatedMovies(datastore, n):

    ratings_df = datastore.ratings_df
    movies_df = datastore.movies_df

    # Create a temporary DataFrame with the N best movies ordered by their average rating
    # The order is not deterministic. Two movies with the same rating can have a different position every time
    topNMovieRatings_df = datastore.spark.createDataFrame(ratings_df.groupBy(FIELD_MOVIE_ID)
                                                          .avg(FIELD_RATING)
                                                          .orderBy(FIELD_AVERAGE_RATING, ascending=False).head(n))

    # Join average ratings table with the movies table...
    # The join destroys the order. Therefore we also need to reorder it...
    topNMovies_df = topNMovieRatings_df.join(movies_df, FIELD_MOVIE_ID).orderBy(FIELD_AVERAGE_RATING, ascending=False)

    # Extract the interesting information from this table. Round rating to two decimal points
    # +-------+-----------+--------------------+----+
    # |movieId|avg(rating)|               title|year|
    # +-------+-----------+--------------------+----+

    return topNMovies_df.rdd.map(list)\
        .map(lambda x: "Title: " + x[2] + " ID: " + str(x[0]) + " Average Rating: " + str(round(x[1], 2)))\
        .collect()


# Return n movies with highest average rating (only movies with more than 100 reviews are considered)...
# This time operating on RDDs, out of curiosity...
def getTopRatedMovies(datastore, n):

    ratings_df = datastore.ratings_df
    movies_df = datastore.movies_df

    ratingsRDD = ratings_df.rdd.map(list)
    moviesRDD = movies_df.rdd.map(list)

    ratingsCount = ratingsRDD.count()
    moviesCount = moviesRDD.count()

    # From ratingsRDD with tuples of (UserID, MovieID, Rating) create an RDD with tuples of
    # the (MovieID, iterable of Ratings for that MovieID)
    movieIDsWithRatingsRDD = (ratingsRDD.map(lambda rating: (rating[1], rating[2])).groupByKey())

    # Using `movieIDsWithRatingsRDD`, compute the number of ratings and average rating for each movie to
    # yield tuples of the form (MovieID, (number of ratings, average rating))
    movieIDsWithAvgRatingsRDD = movieIDsWithRatingsRDD.map(getCountsAndAverages)

    # To `movieIDsWithAvgRatingsRDD`, apply RDD transformations that use `moviesRDD` to get the movie
    # names for `movieIDsWithAvgRatingsRDD`, yielding tuples of the form (average rating, movie name, number of ratings)
    movieNameWithAvgRatingsRDD = (moviesRDD.join(movieIDsWithAvgRatingsRDD).map(lambda movie: (movie[1][1][1], movie[1][0], movie[1][1][0])))
    print 'movieNameWithAvgRatingsRDD: %s\n' % movieNameWithAvgRatingsRDD.take(3)

    # Movies with Highest Average Ratings and more than 500 reviews
    # Apply an RDD transformation to `movieNameWithAvgRatingsRDD` to limit the results to movies with
    # ratings from more than 500 people.
    # We then need sort by the average rating to get the movies in order of their rating (highest rating first)
    movieLimitedAndSortedByRatingRDD = movieNameWithAvgRatingsRDD.filter(lambda movie: movie[2] > 100).sortBy(lambda movie: movie[0], False)
    return movieLimitedAndSortedByRatingRDD.map(lambda x: "Title: " + x[1] + " Average Rating: " + str(round(x[0], 2))).take(n)


# Args: IDandRatingsTuple: a single tuple of (MovieID, (Rating1, Rating2, Rating3, ...))
# Returns: tuple: a tuple of (MovieID, (number of ratings, averageRating))
def getCountsAndAverages(IDandRatingsTuple):
    result = (IDandRatingsTuple[0], (len(IDandRatingsTuple[1]), float(sum(IDandRatingsTuple[1])) / len(IDandRatingsTuple[1])))
    return result
