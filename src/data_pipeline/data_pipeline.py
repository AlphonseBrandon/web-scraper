'''
Author: Alphonse Brandon
Date created: 11/19/2022

Last Updated Date: 11/19/2022
Last Updated Time: 3:26 PM

Description: This script contains the datapipeline steps to scrap wiki data and process it. Just run the script changing the movie_year variable of the movies you want to scrap
'''
import sys
sys.path.insert(1, 'D:/github-repos/web-crawler/src/01_utils')
import utils

import pandas as pd
import numpy as np
import requests
import bs4 as bs
import urllib.request
from tmdbv3api import TMDb
import json
import requests
tmdb = TMDb()
tmdb.api_key = '663ff3650c35de4553dbd25b8eb2de0d'
from tmdbv3api import Movie
tmdb_movie = Movie()

movie_year = 2021

link = "https://en.wikipedia.org/wiki/List_of_American_films_of_{}".format(movie_year)

save_cleaned_data_path = 'D:/github-repos/web-crawler/data/02_intermediate/{}movies.csv'.format(movie_year)

utils.datapipeline()