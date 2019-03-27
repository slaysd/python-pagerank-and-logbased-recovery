import math
import collections

from nltk.tokenize import word_tokenize
from itertools import groupby, chain
from datasource import Datasource


class TF_IDF(object):
    def __init__(self):
        self.db = Datasource.instance()

    def __call__(self, terms):
        tfidf_scores = self.tf_idf(terms)

    def tf_idf(self, terms):
        rank = []
        score_results = []

        results = self.db.get_document_containing_terms(terms)
        for term, docs in groupby(results, lambda x: x[0]):
            nt = set()
            tmp_rank = []
            for _, freq, doc_id, text in docs:
                tokens = word_tokenize(text)
                nd = len(tokens)
                ndt = freq
                nt.add(doc_id)
                tmp_rank.append((doc_id, self._tf(ndt, nd)))
            tmp_rank = map(lambda x: (
                x[0], x[1] * self._idf(len(nt))), tmp_rank)
            rank += tmp_rank
        for doc_id, scores in groupby(rank, lambda x: x[0]):
            total = sum(list(map(lambda x: x[1], scores)))
            score_results.append((doc_id, total))
        score_results = sorted(score_results, key=lambda x: x[1], reverse=True)

        return score_results

    def _tf(self, ndt, nd):
        return math.log(1 + (ndt / nd))

    def _idf(self, nt):
        return 1 / nt
