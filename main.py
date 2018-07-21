#!/usr/bin/python
# Console-based Interactive Application, using Python and Apache Spark.

import os.path
from cmd import Cmd
from DataStore import DataStore
from InteractivePrinter import *
from AnalyseUsers import shortUserInformation
from AnalyseMovies import findMovieById, findMoviesByGenre, getMostWatchedMovies
from AnalyseGenres import listAllGenres

# Specify paths to the ratings.csv and movies.csv
ratingsFile = "./ml-latest-small/ratings.csv"
moviesFile = "./ml-latest-small/movies.csv"

#  Exit the program if ratings.csv is missing
if not os.path.isfile(ratingsFile):
    print("Could not find ratings.csv in current directory. Exiting")
    raise SystemExit

# Exit the program if movies.csv is missing
if not os.path.isfile(moviesFile):
    print("Could not find movies.csv in current directory. Exiting")
    raise SystemExit


# Print the license content
def printLicense():
    license = """
Copyright (c) 2018 Kamil Kolosowski

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE."""
    print(license)


def welcomeMessage():
    print("")
    print("")
    print("Welcome to Movielens. An interactive movies database built with Apache Spark.")


############################################################################

printLicense()
welcomeMessage()

############################################################################

dataStore = DataStore(ratingsFile, moviesFile)
dataStore.loadData()

############################################################################


class Prompt(Cmd):

    def do_exit(self, args):
        print("Quitting.")
        raise SystemExit

    def do_quit(self, args):
        print("Quitting.")
        raise SystemExit

    # ----------------------------------------------------------------------

    def help_shortUserInformation(self):
        print("Usage: shortUserInformation [userId]")
        print("")
        print("Searches for a specific user and prints the number of movies/genres watched.")

    def do_shortUserInformation(self, args):
        if args == '':
            print("Missing Argument")
            return
        printInteractive(shortUserInformation(dataStore, args))

    # ----------------------------------------------------------------------

    def help_searchMovieById(self):
        print("Usage: searchMovieById [id]")
        print("")
        print("Searches for a specific movie by ID and prints the average rating and the number of views.")

    def do_searchMovieById(self, args):
        printInteractive(findMovieById(dataStore, args))

    # ----------------------------------------------------------------------

    def help_showGenres(self):
        print("Usage: showGenres")
        print("")
        print("Lists all Genres.")

    def do_showGenres(self, args):
            printInteractive(listAllGenres(dataStore))

    # ----------------------------------------------------------------------

    def help_searchMovieByGenre(self):
        print("Usage: searchMoviesByGenre [genre]...")
        print("")
        print("Searches for movies that belong to the given genres.")

    def do_searchMovieByGenre(self, args):
        if args == '':
            print("Missing Argument")
            return
        printInteractive(findMoviesByGenre(dataStore, args))

    # ----------------------------------------------------------------------

    def help_listMostViewedMovies(self):
        print("Usage: listMostViewedMovies [n]")
        print("")
        print("List [n] most viewed movies. If [n] is not given the top 20 are returned.")

    def do_listMostViewedMovies(self, args):
        n = 20
        if args == '':
            print("Missing [count]. Only printing top 20.")
        else:
            try:
                n = int(args)
            except ValueError:
                print("Illegal argument: " + args + " is not a valid number.")
                return
        printInteractive(getMostWatchedMovies(dataStore, n))

    # ----------------------------------------------------------------------


if __name__ == '__main__':
    prompt = Prompt()
    prompt.prompt = '> '
    prompt.cmdloop('Ready. Type \'help\' or \'?\' to print the help.')
