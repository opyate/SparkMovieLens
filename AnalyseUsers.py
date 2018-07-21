from Constants import *


# Here, a simple example of finding some basic information about the specified user.
# Based on the user ID the function will calculate:
# - the number of movies watched by the user
# - the number of genres watched
def shortUserInformation(datastore, userId):
    if not checkIfUserExists(datastore, userId):
        return "Unknown User"

    # Reassign the necessary data frames...
    movies_df = datastore.movies_df
    ratings_df = datastore.ratings_df
    genre_assignment_df = datastore.genre_assignment_df

    # Find out how many movies the user has watched...
    movie_data_df = ratings_df.filter(ratings_df[FIELD_USER_ID] == userId).join(movies_df, FIELD_MOVIE_ID)
    movie_count = movie_data_df.count()

    # An alternative approach to the above: movie_count = ratings_df.filter(ratings_df[FIELD_USER_ID] == userId).count()

    # Find out how many different genres the user has seen...
    genre_count = movie_data_df \
        .join(genre_assignment_df, FIELD_MOVIE_ID) \
        .select(FIELD_GENRE_ID) \
        .distinct() \
        .count()

    # In the above piece of code, I can get the number of different genres by joining movie_data_df data frame with
    # genre_assignment_df data frame, both containing movieId, and then counting the number of distinct genreId entries

    return "UserId: " + str(userId) + "; Movies Watched: " + str(movie_count) + "; Different Genres Watched: " + str(
        genre_count)


def checkIfUserExists(datastore, userId):
    ratings_df = datastore.ratings_df
    return ratings_df.where(ratings_df[FIELD_USER_ID] == userId).count() > 0
