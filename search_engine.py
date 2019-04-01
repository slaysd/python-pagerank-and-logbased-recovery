import math
import collections

from operator import itemgetter
from itertools import groupby
from datasource import Datasource


class SearchEngine(object):
    def __init__(self):
        self.db = Datasource.instance()

    def __call__(self, terms):
        return self._calculate_rank(terms)

    def _calculate_rank(self, terms, topk=10):
        tfidf = []
        search_results = {}

        results = self.db.get_document_containing_terms(terms)
        for term, docs in groupby(results, lambda x: x[2]):
            nt = 0
            tmp_tfidf = []
            for doc_id, doc_title, _, ndt, nd, rank in docs:
                if doc_id not in search_results.keys():
                    search_results[doc_id] = {
                        'title': doc_title,
                        'rank': rank
                    }
                nt += 1
                tmp_tfidf.append((doc_id, self._tf(ndt, nd)))

            tmp_tfidf = map(lambda x: (
                x[0], x[1] * self._idf(nt)), tmp_tfidf)
            tfidf += tmp_tfidf

        tfidf = sorted(tfidf, key=lambda x: x[0])
        for doc_id, scores in groupby(tfidf, lambda x: x[0]):
            total = sum(list(map(lambda x: x[1], scores)))
            search_results[doc_id]['tfidf'] = total
            search_results[doc_id]['score'] = total * \
                search_results[doc_id]['rank']

        docs = [(v['score'], k, v['title'], v['tfidf'], v['rank'])
                for k, v in search_results.items()]
        docs = sorted(docs, key=lambda x: (-x[0], x[1]))

        return docs[:topk]

    def _tf(self, ndt, nd):
        return math.log(1 + (ndt / nd))

    def _idf(self, nt):
        return 1 / nt
