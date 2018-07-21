from Constants import *


# An example of listing all the existing genres :)
def listAllGenres(datastore):
    genres_df = datastore.genres_df
    genres = sorted([i[FIELD_GENRE] for i in genres_df.collect()])
    return genres
