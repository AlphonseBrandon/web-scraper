'''
Author: Alphonse Brandon
Date created: 11/19/2022

Last Updated Date: 11/19/2022
Last Updated Time: 2:57 PM

Description: This script contains all the function used in building the web scrapper for hollywood movies from wikipedia
'''

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

def insert_link_to_the_wiki_webpage(link):
    '''This function points to the webpage where the data is found
    :param: the link to the webpage'''
    global url
    url = link

def fetch_tabular_data():
    '''This function gets all the tabular data from the wikipedia link with the class name wikitable sortable'''
    global tables
    souce = urllib.request.urlopen(url).read()
    soup = bs.BeautifulSoup(souce, 'lxml')
    tables = soup.find_all('table', class_='wikitable sortable')

def make_dataframe_of_tabular_data():
    '''This function converts the different fetched table into  pandas dataframes'''
    global df1, df2, df3, df4
    df1 = pd.read_html(str(tables[0]))[0]
    df2 = pd.read_html(str(tables[1]))[0]
    df3 = pd.read_html(str(tables[2]))[0]
    df4 = pd.read_html(str(tables[3]).replace("'1\"\'",'"1"'))[0] # avoided "ValueError: invalid literal for int() with base 10: '1"'

def combine_dataframes():
    '''This function combines all the dataframes into one'''
    global data 
    data = pd.concat([df1, df2, df3, df4])    

def saving_data_to_disk():
    '''This function saves the scrapped data to dara/01_raw directory'''

    path = 'D:/github-repos/web-crawler/data/01_raw/{}wiki_data.csv'.format(movie_year)
    data.to_csv(path, index=False)

def load_data():
    '''This function loads the wiki data from directory data/01_raw'''
    global data
    path = 'D:/github-repos/web-crawler/data/01_raw/{}wiki_data.csv'.format(movie_year)
    data = pd.read_csv(path)

def extracting_features():
    '''This function extracts the features from our scraped dataframe'''
    global data 
    data = data[['Title', 'Cast and crew']]

def get_genre(x):
    '''This function introduces the lofic to get genres for each movie title by making an API call to the TMDB
    :param: title column from the scrapped wiki data'''
    genres = []
    result = tmdb_movie.search(x)
    if not result:
      return np.NaN
    else:
      movie_id = result[0].id
      response = requests.get('https://api.themoviedb.org/3/movie/{}?api_key={}'.format(movie_id,tmdb.api_key))
      data_json = response.json()
      if data_json['genres']:
          genre_str = " " 
          for i in range(0,len(data_json['genres'])):
              genres.append(data_json['genres'][i]['name'])
          return genre_str.join(genres)
      else:
          return np.NaN   

def make_genres_column():
    '''This function creates a new column called genres in our dataset with the genres for each movie'''
    data['genres'] = data['Title'].map(lambda x: get_genre(str(x))) 

def get_director(x):
    '''This function introduces the logic to filter directors from the scrapped wiki data
    :param: cast and crew column to filter directors from'''
    if " (director)" in x:
        return x.split(" (director)")[0]
    elif " (directors)" in x:
        return x.split(" (directors)")[0]
    else:
        return x.split(" (director/screenplay)")[0]

def make_directors_column():
    '''This function gets directors from the cast and crew column of the scrapped data'''
    data['director_name'] = data['Cast and crew'].map(lambda x: get_director(str(x)))

def get_actor1(x):
    '''This function gets the principal actor in the movie
    :param: cast and crew column from data'''
    return ((x.split("screenplay); ")[-1]).split(", ")[0])

def make_actor1_column():
    '''This function makes a new column with names of pricipal actors'''
    data['actor_1_name'] = data['Cast and crew'].map(lambda x: get_actor1(str(x)))

def get_actor2(x):
    '''This function gets the supporting actor in the movie
    :param: cast and crew column from data'''
    if len((x.split("screenplay); ")[-1]).split(", ")) < 2:
        return np.NaN
    else:
        return ((x.split("screenplay); ")[-1]).split(", ")[1])

def make_actor2_column():
    '''This function makes a new column with names of supporting actors'''
    data['actor_2_name'] = data['Cast and crew'].map(lambda x: get_actor2(str(x)))


def get_actor3(x):
    '''This function makes a new column with names of supporting actors'''
    if len((x.split("screenplay); ")[-1]).split(", ")) < 3:
        return np.NaN
    else:
        return ((x.split("screenplay); ")[-1]).split(", ")[2])

def make_actor3_column():
    '''This function makes a new column with names of supporting actors'''
    data['actor_3_name'] = data['Cast and crew'].map(lambda x: get_actor3(str(x)))

def renaming_title_column():
    '''This function renames the title column to movie_title'''
    global data
    data = data.rename(columns={'Title':'movie_title'})    

def extract_features():
    '''This function extracts features to be used later in this project'''
    global cleaned_data
    cleaned_data = data.loc[:,['director_name','actor_1_name','actor_2_name','actor_3_name','genres','movie_title']]

def combining_features():
    '''This function combines all the features into one column'''
    cleaned_data['comb'] = cleaned_data['actor_1_name'] + ' ' + cleaned_data['actor_2_name'] + ' '+ cleaned_data['actor_3_name'] + ' '+ cleaned_data['director_name'] +' ' + cleaned_data['genres']

def removing_nulls():
    global cleaned_data
    '''This function removes missing values from the dataset'''
    cleaned_data = cleaned_data.dropna(how='any')

def make_movie_title_lowercase():
    '''This function makes all movie titles in the movie column lowercase'''
    cleaned_data['movie_title'] = cleaned_data['movie_title'].str.lower()

def save_the_processed_data():
    '''This function saves the process data to data/02_intermediate directory'''
    cleaned_data.to_csv(save_cleaned_data_path, index=False)

def datapipeline():
    '''This function is a compilation of all  the steps in from getting the data to processing it'''
    insert_link_to_the_wiki_webpage(link)
    fetch_tabular_data()
    make_dataframe_of_tabular_data()
    combine_dataframes()
    saving_data_to_disk()
    load_data()
    extracting_features()
    make_genres_column()
    make_directors_column()
    make_actor1_column()
    make_actor2_column()
    make_actor3_column()
    renaming_title_column()
    extract_features()
    combining_features()
    removing_nulls()
    make_movie_title_lowercase()
    save_the_processed_data()
