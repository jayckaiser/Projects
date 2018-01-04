#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

"""
city_state_division.py

This extracts the cities or states from the business csv files and then divides the reviews csv file by city/state.

(C) 2017 by Jay Kaiser <jayckaiser.github.io>
Updated Nov. 27, 2017

 """

import dask.dataframe as dd
import pandas as pd
import numpy as np
import pickle, os

def main():

    # change this path to your own, where the CSV file made in the last step has been stored.
    path_to_files = "E:/yelp/"


    business_csv = os.path.join(path_to_files, "business.csv")
    output_file = os.path.join(path_to_files, "cities_and_businesses.pkl")

    # Run this line as well to create a pickle file from the previously made csv file.
    # Save this file to allow easy read-in if necessary (instead of creating over again each iteration).
    cities_and_businesses = extract_cities(business_csv, output_file)

    review_csv = os.path.join(path_to_files, "review.csv")
    cities_path = os.path.join(path_to_files, "Cities")
    states_path = os.path.join(path_to_files, "States")

    # Fill this with whatever city_state values you'd like. To congregate misspellings, set combine=True.
    city_states_to_find = [
    ]
    find_city_reviews(output_file, city_states_to_find, review_csv, cities_path, combine=True)


    states_to_find = []  # if you leave this empty, ALL states will be found. VERY TIME CONSUMING!!!
    find_state_reviews(output_file, states_to_find, review_csv, states_path)


def extract_cities(business_csv, output_file):
    """
    Given the business.csv file, extract each unique city and all businesses found in them

    :param business_csv: a string representing the path to the business.csv file
    :param output_file: a string representing the path for the new business.pkl file
    :return cities_and_businesses: a dict, formatted as {"city_state": ["business_id", etc]}
    """
    cities_and_businesses = {}

    businesses = pd.read_csv(business_csv, header=0, sep="\t")

    count = 0
    for index, row in businesses.iterrows():
        city_state = "{}_{}".format(row['city'], row['state'])

        if city_state not in cities_and_businesses.keys():
            cities_and_businesses[city_state] = [row['business_id']]
        else:
            cities_and_businesses[city_state].append(row['business_id'])

        count += 1

        if count % 100000 == 0:
            print("\rFinished {} businesses.".format(count), end="")

    pickle.dump(cities_and_businesses, open(output_file, 'wb'))
    return cities_and_businesses


def find_city_reviews(cities_and_businesses, city_states, review_csv, output_directory, combine=False):
    """
    Given a list of state_city pairs, create a separate csv files solely consisting of reviews for those cities.

    :param cities_and_businesses: either a dict or a string representing the path to the pickled dict
    :param city_states: a list of strings representing the city, state combo to look up (in "city, state_code" format)
    :param review_csv: a string representing the path to review.csv
    :param output_directory: a string representing the path to the output csv file
    :param combine: treat all strings in the list as the same city (due to misspellings, etc)
    :return:
    """

    # helper functions to allow easier readability of code.
    def raiseError(city_state):
        print('{} is not present in the Yelp Database.'.format(city_state))

    def computeCity(city_state):
        relevant_businesses = cities_and_businesses[city_state]
        city_reviews = reviews.map_partitions(lambda x: x[x['business_id'].isin(relevant_businesses)])
        city_reviews = city_reviews.compute()

        print("Found all reviews from {}.".format(city_state))
        return city_reviews

    # make the cities directory if not already existent
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
        print("Cities folder does not exist... one will be made.")

    # use a premade pickled cities_and_businesses file at the path if not provided
    if isinstance(cities_and_businesses, str):
        cities_and_businesses = pickle.load(open(cities_and_businesses, 'rb'))
        print("Dictionary not provided. Uploading pickled dictionary file instead.")

    reviews = dd.read_csv(review_csv, header=0, sep='\t')

    if combine:
        # note that this one MUST exist in the csv or an error will be raised
        city_reviews = computeCity(city_states[0])

        for i in range(len(city_states) - 1):
            try:
                more_reviews = computeCity(city_states[i + 1])
                city_reviews = pd.concat([city_reviews, more_reviews])
            except KeyError:
                raiseError(city_states[i + 1])

        city_reviews.to_pickle(os.path.join(output_directory, "{}.pkl".format("&".join(city_states))))

    else:
        for city_state in city_states:
            try:
                city_reviews = computeCity(city_state)
                city_reviews.to_pickle(os.path.join(output_directory, "{}.pkl".format(city_state)))
            except KeyError:
                raiseError(city_state)


def find_state_reviews(cities_and_businesses, states, review_csv, output_directory):
    """
    Given a list of states, create a separate csv files solely consisting of reviews for those states.

    :param cities_and_businesses: either a dict or a string representing the path to the pickled dict
    :param states: a list of state codes to search for. If empty, input all.
    :param review_csv: a string representing the path to review.csv
    :param output_directory: a string representing the path to the output csv file
    :return:
    """

    # helper functions to allow easier readability of code.
    def raiseError(state):
        print('{} is not present in the Yelp Database.'.format(state))

    def findStateCode(city_state):
        hyphen = city_state.rfind("_")
        return city_state[hyphen + 1 :]

    def computeState(state):

        number_of_cities = 0
        total_businesses = []
        for city_state in cities_and_businesses.keys():
            if findStateCode(city_state) == state:
                total_businesses += cities_and_businesses[city_state]
                number_of_cities += 1

        print("\rThere are {} cities in {}...".format(number_of_cities, state), end=' ')

        state_reviews = reviews.map_partitions(lambda x: x[x['business_id'].isin(total_businesses)])
        state_reviews = state_reviews.compute()

        print("Found all reviews from {}.".format(state))
        return state_reviews

    # make the cities directory if not already existent
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
        print("States folder does not exist... one will be made.")

    # use a premade pickled cities_and_businesses file at the path if not provided
    if isinstance(cities_and_businesses, str):
        cities_and_businesses = pickle.load(open(cities_and_businesses, 'rb'))
        print("Dictionary not provided. Uploading pickled dictionary file instead.")

    reviews = dd.read_csv(review_csv, header=0, sep='\t')

    if len(states) == 0:  # do ALL states.
        print("No states were specified. All states will be done instead.")
        unique_states = set()
        for city_state in cities_and_businesses.keys():
            unique_states.add(findStateCode(city_state))

        states = list(unique_states)

    for state in states:
        try:
            state_reviews = computeState(state)
            state_reviews.to_pickle(os.path.join(output_directory, "{}.pkl".format(state)))
        except KeyError:
            raiseError(state)


if __name__ == "__main__":
    main()
