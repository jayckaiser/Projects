#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

"""
StageTwo.py

Given converted and isolated Subreddit files from StageOne, create LineSentence and Word2Vec files for each across
four different criteria: raw, popular, lemma+POS, and popular lemma+POS.

(C) 2017 by Jay Kaiser <jayckaiser.github.io>
 """

import pandas as pd
import en_core_web_sm
from gensim.models import Word2Vec
from gensim.models.word2vec import LineSentence
import multiprocessing


def main():

    date = "2017-09"
    subreddits_to_do = [
        "democrats", "hillaryclinton", "Republican", "mylittlepony", "TwoXChromosomes", "TheRedPill", "sports",
        "personalfinance", "books", "The_Donald",
    ]

    for subreddit in subreddits_to_do:

        pathString = "D:/Reddit/{0}/SentProp/{1}/{0}_{1}".format(date, subreddit)

        csv_file = "D:/Reddit/{0}/{0}_{1}".format(date, subreddit) + ".csv"
        lineSentsFile = pathString + "{}.ls"
        modelFile = pathString + "{}.word2vec"

        """
        # subreddit_comments = purify_comments(csv_file)
        # printCommentsAsLineSentences(subreddit_comments, lineSentsFile.format("RAW"))
        # buildWord2VecModel(lineSentsFile.format("RAW"), modelFile.format("RAW"))
        #
        # subreddit_comments = purify_comments(csv_file, popular=10)
        # printCommentsAsLineSentences(subreddit_comments, lineSentsFile.format("POP"))
        # buildWord2VecModel(lineSentsFile.format("POP"), modelFile.format("POP"))
        #
        # subreddit_comments = purify_comments(csv_file, POS=True, lemmatize=True)
        # printCommentsAsLineSentences(subreddit_comments, lineSentsFile.format("POS"))
        # buildWord2VecModel(lineSentsFile.format("POS"), modelFile.format("POS"))
        #
        # subreddit_comments = purify_comments(csv_file, POS=True, lemmatize=True, popular=10)
        # printCommentsAsLineSentences(subreddit_comments, lineSentsFile.format("ALL"))
        # buildWord2VecModel(lineSentsFile.format("ALL"), modelFile.format("ALL"))
        """

        subreddit_comments = purify_comments(csv_file, keep_stops=True)
        printCommentsAsLineSentences(subreddit_comments, lineSentsFile.format("RAWS"))
        buildWord2VecModel(lineSentsFile.format("RAWS"), modelFile.format("RAWS"))

        subreddit_comments = purify_comments(csv_file, popular=10, keep_stops=True)
        printCommentsAsLineSentences(subreddit_comments, lineSentsFile.format("POPS"))
        buildWord2VecModel(lineSentsFile.format("POPS"), modelFile.format("POPS"))

        # subreddit_comments = purify_comments(csv_file, POS=True, lemmatize=True, keep_stops=True)
        # printCommentsAsLineSentences(subreddit_comments, lineSentsFile.format("POSS"))
        # buildWord2VecModel(lineSentsFile.format("POSS"), modelFile.format("POSS"))
        #
        # subreddit_comments = purify_comments(csv_file, POS=True, lemmatize=True, popular=10, keep_stops=True)
        # printCommentsAsLineSentences(subreddit_comments, lineSentsFile.format("ALLS"))
        # buildWord2VecModel(lineSentsFile.format("ALLS"), modelFile.format("ALLS"))
        #
        # subreddit_comments = purify_comments(csv_file, lemmatize=True, keep_stops=True)
        # printCommentsAsLineSentences(subreddit_comments, lineSentsFile.format("LEMS"))
        # buildWord2VecModel(lineSentsFile.format("LEMS"), modelFile.format("LEMS"))


def purify_comments(csv_file, keep_stops=False, POS=False, lemmatize=False, popular=0):
    """
    Create a pandas Series of comments from a given subreddit csv_file, given certain parameters.
    Stop words are also removed.

    :param csv_file: a string representing the path to the subreddit csv file
    :param keep_stops: if True, keep stop words in the parse, default to False
    :param POS: if True, translate comments to token-POS tag combinations instead of raw tokens, default to False
    :param lemmatize: if True, translate comments to their lemmas, instead of raw tokens, default to False
    :param popular: only take comments that have a score greater than popular, default to 0 (or all comments)
    :return comments: a pandas Series of comments, after which parameters have been applied
    """

    df = pd.read_csv(csv_file)
    df = df.loc[df["author"] != "[deleted]"] # trim out comments whose authors have deleted their accounts
    df = df.loc[df["score"] != "score"] # this is an error in the code when building new csv_files from dask

    # extracts only the popular comments
    if popular > 0:
        df = df.loc[pd.to_numeric(df["score"]) > popular]

    comments = df["body"]
    del df  # no need for this anymore, and it'll merely eat up memory

    nlp = en_core_web_sm.load()

    revised_comments = []
    for comment in comments.astype('unicode').values:
        comment = comment[1:]  # remove the initial 'b' bytes-representation character
        comment = comment.encode("utf-8-sig").decode("utf-8-sig")  # get rid of BOM character
        comment = comment.lower().replace(r"\n", r"").replace(r'"', r'')

        tokens = nlp(comment)

        # actual specification section
        for sent in tokens.sents:

            if POS:  # conversion of comments to tokens/lemmas-POS tags
                if lemmatize:
                    if keep_stops:
                        revised_tokens = ["{}-{}".format(token.lemma_, token.tag_) for token in sent
                                        if not token.is_punct]
                    else:
                        revised_tokens = ["{}-{}".format(token.lemma_, token.tag_) for token in sent
                                        if not token.is_stop and not token.is_punct
                                        and not token.orth_ == "n't" and not token.orth_ == "'s"]
                else:
                    if keep_stops:
                        revised_tokens = ["{}-{}".format(token.orth_, token.tag_) for token in sent
                                          if not token.is_punct]
                    else:
                        revised_tokens = ["{}-{}".format(token.orth_, token.tag_) for token in sent
                                        if not token.is_stop and not token.is_punct
                                        and not token.orth_ == "n't" and not token.orth_ == "'s"]

            elif lemmatize: # just lemmatization
                if keep_stops:
                    revised_tokens = [token.lemma_ for token in sent
                                      if not token.is_punct]
                else:
                    revised_tokens = [token.lemma_ for token in sent
                                        if not token.is_stop and not token.is_punct
                                        and not token.orth_ == "n't" and not token.orth_ == "'s"]

            else: # nothing but removal of stop words (or not)
                if keep_stops:
                    revised_tokens = [token.orth_ for token in sent
                                      if not token.is_punct]
                else:
                    revised_tokens = [token.orth_ for token in sent
                                    if not token.is_stop and not token.is_punct
                                    and not token.orth_ == "n't" and not token.orth_ == "'s"]

            revised_comments.append(" ".join(revised_tokens))

    return pd.Series(revised_comments)


def printCommentsAsLineSentences(comments, lineSentencesPath):
    """
    Prints the comments one-by-one into a LineSentence format.

    :param comments:
    :param lineSentencesPath:
    :return:
    """
    lineSentencesFile = open(lineSentencesPath, "w", encoding='utf-8')
    for comment in comments:
        lineSentencesFile.write(comment + "\n")
    lineSentencesFile.close()
    print("Printed as lineSentences the comments from {}.".format(lineSentencesPath))


def buildWord2VecModel(lineSentencesPath, modelPath):
    """
    Builds a Word2Vec Model for the given line sentences and saves it to be able to be viewed later.

    :param lineSentencesPath:
    :param modelPath:
    :return:
    """

    lineSentences = LineSentence(lineSentencesPath)

    workers = multiprocessing.cpu_count()
    # initiate the model and perform the first epoch of training
    terms2vec = Word2Vec(lineSentences, size=100, window=5, min_count=2, sg=1, workers= workers - 1)
    terms2vec.save(modelPath)
    print("\rFinished 1 epoch for {}.".format(modelPath), end="")

    # perform another 11 epochs of training
    for i in range(1, 12):
        terms2vec.train(lineSentences, total_examples=terms2vec.corpus_count, epochs=terms2vec.iter)
        terms2vec.save(modelPath)
        print("\rFinished {} epochs for {}.".format(i + 1, modelPath), end="")
    print()
    return terms2vec


if __name__ == "__main__":
    main()