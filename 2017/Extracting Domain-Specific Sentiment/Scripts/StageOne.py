#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

"""
StageOne.py

This code takes the raw Reddit data from "http://files.pushshift.io/reddit/comments/" and converts them to a csv format,
then isolates the comments from a given list of subreddits to allow further, easier parsing.

(C) 2017 by Jay Kaiser <jayckaiser.github.io>
 """

import json, sys, os
from glob import glob
import dask.dataframe as dd

def main():
    output_location = "D:/Reddit/{}/{}"
    datastore_location = "D:/Reddit/{}/RC_{}"

    to_work_on_csv_files = [
        #"2017-09"
        ]

    subreddits_to_do = [
        # "democrats", "hillaryclinton", "Republican", "mylittlepony",
        # "TwoXChromosomes", "TheRedPill", "sports", "The_Donald",
        'personalfinance', 'books'
    ]

    for csv in to_work_on_csv_files:
        print("Rewriting {}.".format(csv))
        parseDS(datastore_location.format(csv, csv), output_location.format(csv, csv + ".csv"))

    csv = "2017-09"
    for subreddit in subreddits_to_do:
        isolate_subreddit(output_location.format(csv, csv), subreddit)
        print("\rParsed {} for {}.".format(csv, subreddit), end='')


# these are the fields to take from the JSON lines of Reddit data.
fields = ['subreddit',
          'subreddit_id',
          'created_utc',
          'id',
          'parent_id',
          'body',
          'author',
          'score',
          'link_id',
          'edited',
          'gilded',
          'retrieved_on',
          'distinguished',
          'controversiality',
          ]


def parseDS(datastore_location, output_location):
    """
    Streams in the JSON comment data dump file and outputs it as a CSV file.

    :param datastore_location: string representing the path to the datastore
    :param output_location: string representing the path to the output file
    :return: write a csv file that holds all the information from the JSON file
    """

    with open(datastore_location, 'r') as f:
        with open(output_location, 'w') as o:

            output_string = "\t".join('{}'.format(field) for field in fields)
            o.write(output_string + "\n")

            n = 1
            for line in f:

                if n % 100000 == 0:
                    print("\rFinished {:,}.".format(n), end='')

                post = json.loads(line)
                # print(post)

                post['body'] = post['body'].encode(sys.stdout.encoding, errors='replace')

                output_string = "\t".join('{}'.format(post[field]) for field in fields)
                o.write(output_string + "\n")

                n += 1
    print()


def isolate_subreddit(csv_location, subreddit):
    """
    Given a premade CSV file, isolate and print separately only comments from the given subreddit

    :param csv_location: string representing the path to the csv file made previously
    :param output_location: string representing the path to the output file
    :param subreddit: string representing a particular subreddit to extract
    :return: write a csv file that holds all the information from the given subreddit comments
    """

    individual_subreddit_csvs = csv_location + "_" + subreddit + '.*.csv'

    df = dd.read_csv(csv_location + ".csv", header=0, sep='\t')
    sub_df = df.loc[df['subreddit'] == subreddit]

    sub_df.to_csv(individual_subreddit_csvs)
    filenames = glob(individual_subreddit_csvs)
    with open(csv_location + "_" + subreddit + '.csv', 'w') as out:
        for fn in filenames:
            with open(fn) as f:
                out.write(f.read())
            os.remove(fn)


if __name__ == "__main__":
    main()