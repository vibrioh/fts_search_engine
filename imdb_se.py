#! /usr/bin/python3
# -*- coding: utf-8 -*-

__author__ = "Jun Hu <jun.hu@columbia.edu>"
__date__ = "Nov 23, 2017"

import os
import csv
import ast
from elasticsearch import Elasticsearch
import pyprind

fence = "-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-"

path_movie_metadata = os.path.abspath("MovieSummaries/movie.metadata.tsv")
movie_metadata = {}
with open(path_movie_metadata, "r", encoding="utf-8") as file:
    text = csv.reader(file, delimiter="\t")
    n = 81741
    bar = pyprind.ProgBar(n, bar_char="█", title="Loading movie metadata")
    for line in text:
        id = line[0]
        name = line[2] if line[2] != "" else "None"
        date = line[3] if line[3] != "" else "1800-01-01"
        runtime = line[5] if line[5] != "" else "0.0"
        languages = ", ".join(ast.literal_eval(line[6]).values()) if line[6] != "" else "N/A"
        countries = ", ".join(ast.literal_eval(line[7]).values()) if line[7] != "" else "N/A"
        genres = ", ".join(ast.literal_eval(line[8]).values()) if line[8] != "" else "N/A"
        movie_metadata[id] = {"name": name, "date": date, "runtime": runtime, "languages":languages, "countries": countries, "genres": genres}
        bar.update()
    print(bar)
    print("Totally " + str(len(movie_metadata)) + " movie metadata loaded")
    print(fence)


path_plot_summaries = os.path.abspath("MovieSummaries/plot_summaries.txt")
ids = []
movie_search_contents = []
n = 81741
bar = pyprind.ProgBar(n, bar_char="█", title="Loading movie summaries")
with open(path_plot_summaries, "r", encoding="utf-8") as file:
    for line in file:
        id = line.split('\t')[0]
        if id in movie_metadata:
            ids.append(id)
            summaries = line.split('\t')[1]
            movie_metadata[id]["summaries"] = summaries
            movie_search_contents.append(
                movie_metadata[id]["name"] + " " + movie_metadata[id]["genres"] + " " + summaries)
            bar.update()
    print(bar)
    print("Totally " + str(len(movie_metadata)) + " movie summaries loaded")
    print(fence)


n = 81741
bar = pyprind.ProgBar(n, bar_char="█", title="Creating indices for Elasticsearch")
# import elasticsearch_dsl
es = Elasticsearch()
es.indices.delete(index="imdb-index", ignore=[400, 404])
es.indices.create(index="imdb-index", body={
    "settings": {
        "number_of_shards": 1,
        "analysis": {
            "filter": {
                "my_shingle_filter": {
                    "type": "shingle",
                    "min_shingle_size": 2,
                    "max_shingle_size": 2,
                    "output_unigrams": False
                }
            },
            "analyzer": {
                "my_shingle_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "my_shingle_filter"
                    ]
                }
            }
        }
    }
}, ignore=400)
for key, value in movie_metadata.items():
    res = es.index(index="imdb-index", doc_type="movies", id=key, body=value)
    n -= 1
    bar.update()
print(bar)
count = es.count(index="imdb-index", doc_type='movies', body={ "query": {"match_all" : { }}})
print("Now. We've created the indices of {0} documents. Search engine is ready to use...\n".format(count['count']))
print(fence)

yes = True
while yes:
    query = input("Enter something for search: ")
    result = es.search(index="imdb-index", doc_type="movies", body={"query": {"bool":   { "should": [{"match": {"title": query}}, {"match": {"summaries": query}}, {"match": {"genres": query}}, {"match": {"title.shingles": query}}, {"match": {"summaries.shingles": query}}, {"match": {"genres.shingles": query}}]}}})
    if result.get('hits') is not None and result['hits'].get('hits') is not None:
        rank = 1
        print("You are requiring movies about: " + query)
        print("Here are the most relevant movies (up to 10): " + query)
        for dict in result['hits']['hits']:
            print("Ranking: \t# " + str(rank) + "\n" + "R-score: \t" + str(dict['_score']))
            print("Title: \t<< " + str(dict["_source"]["name"]) + " >>")
            print("Date: \t" + str(dict["_source"]["date"]))
            print("Runtime: \t" + str(dict["_source"]["runtime"]))
            print("Genres: \t" + str(dict["_source"]["genres"]))
            print("Languages: \t" + str(dict["_source"]["languages"]))
            print("Countries: \t" + str(dict["_source"]["countries"]))
            print("Summary: \t" + str(dict["_source"]["summaries"]))
            print(fence)
            rank += 1
    feedback = input("Another search ([Y]/N)?")
    assert (feedback == '' or feedback.lower() in 'yn')
    if feedback == '' or feedback.lower() == 'y':
        yes = True
    else:
        yes = False



