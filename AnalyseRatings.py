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