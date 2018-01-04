#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

"""
StageThree.py

Given the word2vec files made in StageTwo, create weighted graphs and apply random-walk methods to create sentiment
lexicons unique to each subreddit and NLP method.

(C) 2017 by Jay Kaiser <jayckaiser.github.io>
 """

import pandas as pd
import numpy as np
import networkx as nx
from gensim.models import Word2Vec
from sklearn import preprocessing
from scipy import sparse
import scipy
import pickle, os


def main():
    directory_path = "D:/Reddit/2017-09/SentProp/{0}/2017-09_{0}{1}"

    subreddits = [
        #"democrats", "hillaryclinton", "Republican", "mylittlepony", "TwoXChromosomes",
        #"TheRedPill", "sports", "The_Donald", "personalfinance", "books"
    ]

    file_types = [
                'LEMS', 'RAWS', 'POPS',
                # 'RAW', 'POP',
                'POSS', 'ALLS',
                #'POS', 'ALL',
                ]

    graphs_path = "C:/Users/jayka/OneDrive/Documents/SMM Final Project/WeightedGraphs/"
    lexicons_path = "C:/Users/jayka/OneDrive/Documents/SMM Final Project/Lexicons/"

    for subreddit in subreddits:

        #build the weighted graphs for each type of processing for each subreddit
        for ft in file_types:
            path = directory_path.format(subreddit, ft)

            # save the graph in an easily-accessible format for the random-walk
            DG = buildWeightedGraphs(path + '.word2vec')
            pickle.dump(DG, open(os.path.join(graphs_path, subreddit + ft + ".pkl"), 'wb'))

    buildLexiconsViaRandomWalk(lexicons_path, graphs_path)


### Building the weighted graph

def findEdgeWeight(vector1, vector2):
    """
    Find the weight for a given edge in the graph, measured as in Hamilton et al., (2016).

    :param vector1:
    :param vector2:
    :return:
    """
    dotProduct = np.dot(vector1, vector2)
    v1norm = np.linalg.norm(vector1)
    v2norm = np.linalg.norm(vector2)
    return np.arccos(- (dotProduct / (v1norm * v2norm)))


def buildWeightedGraphs(word2vec_file, n=25):
    """
    Build the weighted graph of nodes connected by most-similar neighbors based on the word2vec file.

    :param word2vec_file: the path to the file from which the graph will be constructed
    :param n: the number of neighbors to be included from each vocab element of the file in the graph
    :return:
    """
    DG = nx.DiGraph()

    model = Word2Vec.load(word2vec_file)
    model.init_sims()

    for vocab in model.wv.vocab.keys():
        DG.add_node(vocab)
        main_word_vector = model.wv[vocab]

        most_similar = model.most_similar(positive=[vocab], topn = n)
        for sim_word, sim_value in most_similar:
            DG.add_node(sim_word)
            sim_word_vector = model.wv[sim_word]

            e_weight = findEdgeWeight(main_word_vector, sim_word_vector)
            DG.add_edge(vocab, sim_word, weight=e_weight)

    print("Finished the graph for {}.".format(word2vec_file))
    return DG


### Building the lexicons

positive_seed_words = ["good", "lovely", "excellent", "fortunate", "pleasant", "delightful",
                       "perfect", "loved", "love", "happy"]
negative_seed_words = ["bad", "horrible", "poor", "unfortunate", "unpleasant", "disgusting",
                       "evil", "hated", "hate", "unhappy"]

positive_seeds_POS = ["good-JJ", "lovely-JJ", "excellent-JJ", "fortunate-JJ", "pleasant-JJ", "delightful-JJ",
                      "perfect-JJ", "love-JJ", "love-VB", "love-VBP", "love-NN", "happy-JJ"]
negative_seeds_POS = ["bad-JJ", "horrible-JJ", "poor-JJ", "unfortunate-JJ", "unpleasant-JJ", "disgusting-JJ",
                      "evil-JJ", "hate-JJ", "hate-VB", "hate-VBP", "hate-NN", "unhappy-JJ"]

# The code below is taken directly (with minor changes from myself) from Hamilton et al.'s SentProp instantiation.
# This posed way too complicated for me to code by hand. Note that although this code is copied, I do understand the
# mechanics being used, and with enough practice and review I could probably replicate this myself.


def run_iterative(M, r, update_seeds, max_iter=50, epsilon=1e-6):
    for i in range(max_iter):
        last_r = np.array(r)
        r = M @ r
        update_seeds(r)

        # run until convergence
        if np.abs(r - last_r).sum() < epsilon:
            break
    return r


def weighted_teleport_set(words, seed_weights):
    return np.array([seed_weights[word] if word in seed_weights else 0.0 for word in words])


def random_walk(DG, positive_seeds, negative_seeds, beta=0.95):
    """
    Runs the random-walk technique described in Hamilton et al., (2016).
    Note that most of the code is copied directly (see note above).

    :param DG: the directed graph from which random-walking will be done
    :param positive_seeds: positive seed words to base positive polarity from
    :param negative_seeds: negative seed words to base negative polarity from
    :param beta: a parameter to favor local consistency across seeds and neighbors
    :return:
    """
    def run_random_walk(M, teleport, beta):

        def update_seeds(r):
            r += (1 - beta) * teleport / np.sum(teleport)

        return run_iterative(M * beta, np.ones(number_of_words) / number_of_words, update_seeds)

    if not type(positive_seeds) is dict:
        positive_seeds = {word: 1.0 for word in positive_seeds}
        negative_seeds = {word: 1.0 for word in negative_seeds}

    number_of_words = len(DG.nodes())

    # creating the symmetric transition matrix needed for the propagation
    E = nx.to_scipy_sparse_matrix(DG)
    column_sums = E.sum(axis=0)
    D = sparse.dia_matrix((column_sums, 0), shape=(number_of_words, number_of_words))
    T = np.sqrt(D) @ np.sqrt(E @ D)

    del E, D # assisted in running out of memory before using sparse graphs remedied it

    words = DG.nodes()

    rpos = run_random_walk(T, weighted_teleport_set(words, positive_seeds), beta)
    rneg = run_random_walk(T, weighted_teleport_set(words, negative_seeds), beta)

    # ran into problems with their code, so deconstructing lexicon-building for ease of understanding the problem.
    lexicon = {}
    for i, w in enumerate(words):
        rpos_i = rpos[i]
        rneg_i = rneg[i]
        total = rpos_i + rneg_i
        if total != 0:
            value = rpos_i / (rpos_i + rneg_i)
        else:
            value = 0

        lexicon[w] = value

    return lexicon


### End of code duplication from Hamilton et al.'s SentProp instantiation.

def standardizeLexicon(lex):
    """
    Converts a polarity lexicon into a Pandas Series, normalizes mean and unit variance, and returns as a dict.

    :param lex: the dict representing words and their polarities
    :return:
    """
    converted_lex = pd.DataFrame.from_dict(lex, orient='index')
    converted_lex[0] = preprocessing.scale(converted_lex[0])
    return converted_lex.to_dict()


def buildLexiconsViaRandomWalk(lexicons_path, graphs_path):
    # first try using only lemmatized or raw stops-included files.
    files_to_consider = [file for file in os.listdir(graphs_path)]

    count = 0
    for file in files_to_consider:
        if count == 1000:
            break

        weighted_graph_path = os.path.join(graphs_path, file)
        new_lexicon_path = os.path.join(lexicons_path, file[:-4] + "_LEX.pkl")

        DG = pickle.load(open(weighted_graph_path, 'rb'))

        if "ALL" in file or "POS" in file:  # use the POS-aligned seeds
            lex = random_walk(DG, positive_seeds_POS, negative_seeds_POS)
        else:
            lex = random_walk(DG, positive_seed_words, negative_seed_words)

        scaled_lex = standardizeLexicon(lex)
        pickle.dump(scaled_lex, open(new_lexicon_path, 'wb'))
        print("Finished building the lexicon for {}.".format(file))
        count += 1


if __name__ == "__main__":
    main()