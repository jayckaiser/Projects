#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

"""
IsolateUsers.py

This extracts the number of users per join-date, given the user csv file, and returns it as a pickled DataFrame.

(C) 2017 by Jay Kaiser <jayckaiser.github.io>
Created Nov. 27, 2017
Updated Nov. 27, 2017

 """

import pandas as pd
import os, pickle


def main():

    # change this to the directory where you've stored your CSV files.
    path_to_files = "E:/yelp/"

    user_path = os.path.join(path_to_files, "user.csv")
    user_pkl = os.path.join(path_to_files, "usercounts.pkl")

    reviews = os.path.join(path_to_files, "user.csv")
    cities_and_businesses = os.path.join(path_to_files, "cities_and_businesses.pkl")
    cities_output = os.path.join(path_to_files, "Cities/")
    states_output = os.path.join(path_to_files, "States/")

    isolate_by_day(user_path, user_pkl)

    # Leave this blank to parse all states. This is surprisingly fast.
    states_to_parse = []
    isolate_users_by_state(user_path, states_to_parse, states_output)

    # Leave this blank to parse all cities. This is surprisingly fast.
    cities_to_parse = []
    isolate_users_by_city(user_path, cities_to_parse, cities_output)

def isolate_by_day(user_path, user_pkl):
    users_df = pd.read_csv(user_path, header=0, sep="\t")
    user_sorted_dates = users_df.sort_values('yelping_since')
    new_members_per_day = user_sorted_dates.groupby(['yelping_since'], as_index=False).count()
    pd.to_pickle(new_members_per_day, user_pkl)
    return new_members_per_day


def isolate_users_by_state(user_path, states, output_directory):
    def raiseError(state):
        print('{} is not present in the supplied .pkl files.'.format(state))

    def computeState(state_path):
        state_df = pd.read_pickle(state_path)
        unique_users = state_df['user_id'].unique()

        print("\rFound all unique users from {}...".format(state_path), end=" ")
        return unique_users.tolist()

    def findStateUserCounts(list_of_users):
        relevant_users = users[users['user_id'].isin(list_of_users)].sort_values('yelping_since')
        sorted_users = relevant_users.groupby(['yelping_since'], as_index=False).count()
        return sorted_users[['yelping_since', 'user_id']]

    def extractStateCode(state_path):
        return state_path[:-4]


    # make the states directory if not already existent
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
        print("States folder does not exist... one will be made.")

    if not os.path.exists(os.path.join(output_directory, "Users/")):
        os.makedirs(os.path.join(output_directory, "Users/"))
        print("Users folder does not exist... one will be made.")

    state_files = []

    if len(states) == 0:  # do ALL states.
        print("No states were specified. All states will be done instead.")
        state_files = [f for f in os.listdir(output_directory) if os.path.isfile(os.path.join(output_directory, f))]
    else:
        for state in states:
            state_files.append("{}.pkl".format(state))

    users = pd.read_csv(user_path, header=0, sep='\t')
    for state_file in state_files:

        state_code = extractStateCode(state_file)
        file_path = os.path.join(output_directory, state_file)

        if os.path.isfile(file_path):
            state_users = computeState(file_path)
            sorted_users = findStateUserCounts(state_users)
            pd.to_pickle(sorted_users, os.path.join(output_directory, "Users/", "{}_users.pkl".format(state_code)))
            print("Found number of new users per day for {}.".format(state_code))

        else:
            print("There is no file labelled {}.".format(file_path))


def isolate_users_by_city(user_path, cities, output_directory):
    def extractCityCodes(city_path):
        return city_path[:-4]

    def raiseError(city):
        print('{} is not present in the supplied .pkl files.'.format(city))

    def extractUniqueCities(city_path):
        Cities_index = city_path.index("/Cities/")
        actual_city_names = city_path[Cities_index + 1: -4]
        return actual_city_names.split("&")

    def computeCity(city_path):
        cities_to_parse = extractUniqueCities(city_path)
        unique_users = []

        for city in cities_to_parse:
            city_df = pd.read_pickle(city_path)
            city_unique_users = city_df['user_id'].unique().tolist()
            unique_users += city_unique_users
            print("\rFound all unique users from {}...".format(city), end=" ")

        print("Found all unique users from {}.".format(city_path))
        return unique_users

    def findCityUserCounts(list_of_users):
        relevant_users = users[users['user_id'].isin(list_of_users)].sort_values('yelping_since')
        sorted_users = relevant_users.groupby(['yelping_since'], as_index=False).count()
        return sorted_users[['yelping_since', 'user_id']]


    # make the states directory if not already existent
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
        print("Cities folder does not exist... one will be made.")

    if not os.path.exists(os.path.join(output_directory, "Users/")):
        os.makedirs(os.path.join(output_directory, "Users/"))
        print("Users folder does not exist... one will be made.")

    city_files = []

    if len(cities) == 0:  # do ALL states.
        print("No cities were specified. All cities will be done instead.")
        city_files = [f for f in os.listdir(output_directory) if os.path.isfile(os.path.join(output_directory, f))]
    else:
        for city in cities:
            city_files.append("{}.pkl".format(city))

    users = pd.read_csv(user_path, header=0, sep='\t')
    for city_file in city_files:

        city_code = extractCityCodes(city_file)
        file_path = os.path.join(output_directory, city_file)

        if os.path.isfile(file_path):
            city_users = computeCity(file_path)
            sorted_users = findCityUserCounts(city_users)
            pd.to_pickle(sorted_users, os.path.join(output_directory, "Users/", "{}_users.pkl".format(city_code)))

        else:
            print("There is no file labelled {}.".format(file_path))


if __name__ == '__main__':
    main()