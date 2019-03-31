import math
import collections
import numpy as np

from nltk.tokenize import word_tokenize
from itertools import groupby, chain
from datasource import Datasource


class PageRank(object):
    def __init__(self, jump_prob=0.15, e=math.exp(-8)):
        self.db = Datasource.instance()
        self.jump_prob = jump_prob
        self.e = e

        self.doc_ids = sorted([int(v[0]) for v in self.db.get_all_link_docs()])
        self.id2idx = {v: idx for idx, v in enumerate(self.doc_ids)}

    def __call__(self):
        forward_link = self._linkdb_to_dict(self.db.get_forward_link())
        doc_size = len(self.doc_ids)

        link_matrix = np.ones((doc_size, doc_size),
                              dtype=np.float32) * (self.jump_prob / doc_size)

        scores = np.ones(doc_size, dtype=np.float32) / doc_size
        for i in range(doc_size):
            doc_id = self.doc_ids[i]
            if doc_id in forward_link.keys():
                links = list(
                    map(lambda x: self.id2idx[x], forward_link[doc_id]))
                link_matrix[i, links] += (1 - self.jump_prob) / len(links)
            else:
                link_matrix[i, :] += (1 - self.jump_prob) / doc_size
        link_matrix = np.transpose(link_matrix)
        scores = np.transpose(scores)

        delta = 1
        while delta > self.e:
            prev = scores.copy()
            scores = np.matmul(link_matrix, scores)
            delta = np.sum(np.abs(prev - scores))

        return list(zip(self.doc_ids, scores.tolist()))

    def _linkdb_to_dict(self, data):
        return {k: [int(tmp) for tmp in v.split(',')] + [k] if v is not None else [] for k, v in data}
