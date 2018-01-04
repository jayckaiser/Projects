#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

"""
yelp2csv.py

This converts the JSON format of the yelp reviews files to CSV.

(C) 2017 by Jay Kaiser <jayckaiser.github.io>
Updated Nov. 12, 2017

 """

import csv, json, codecs, os

def main():

    # change this path to where you're keeping the Yelp JSON files.
    path_to_directory = "E:/yelp/"

    # if you run all four blocks below, it'll make CSV files for each of the Yelp JSON files. Comment out as needed.

    # reviews
    review_path = os.path.join(path_to_directory, "review.json")
    review_output_path = os.path.join(path_to_directory, "review.csv")
    review_fields = ['review_id', 'user_id', 'business_id', 'stars', 'date', 'text', 'useful', 'funny', 'cool']
    convertJSON2CSV(review_path, review_output_path, review_fields)

    # tips
    tip_path = os.path.join(path_to_directory, "tip.json")
    tip_output_path = os.path.join(path_to_directory, "tip.csv")
    tip_fields = ['text', 'date', 'likes', 'business_id', 'user_id']
    convertJSON2CSV(tip_path, tip_output_path, tip_fields)

    # businesses
    business_path = os.path.join(path_to_directory, "business.json")
    business_output_path = os.path.join(path_to_directory, "business.csv")
    business_fields = ['business_id', 'name', 'neighborhood', 'address', 'city', 'state', 'postal_code',
                       'stars', 'review_count', 'is_open']
    convertJSON2CSV(business_path, business_output_path, business_fields)

    # users
    user_path = os.path.join(path_to_directory, "user.json")
    user_output_path = os.path.join(path_to_directory, "user.csv")
    user_fields = ['user_id', 'review_count', 'yelping_since']
    convertJSON2CSV(user_path, user_output_path, user_fields)


def convertJSON2CSV(input_path, output_path, fields):

    out = open(output_path, 'w', encoding='utf8')

    with open(input_path, 'r', encoding='utf8') as f:

        formatted_line = '{}' + '\t{}' * (len(fields) - 1) + '\n'

        out.write(formatted_line.format(*fields))

        n = 0
        for line in f:

            parsed_line = json.loads(line)

            retrieval = []

            for field in fields:
                if field == "text":
                    retrieval.append(r'''{}'''.format(parsed_line.get(field).replace("\n", " ").replace("\r", " ")))
                else:
                    retrieval.append(parsed_line.get(field))

            # print(parsed_line)
            csv_line = formatted_line.format(*retrieval)
            out.write(csv_line)
            n += 1

            if n % 100000 == 0:
                print("\rParsed {} entities.".format(n), end="")

    out.close()


if __name__ == "__main__":
    main()