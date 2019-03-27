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
        back_link = self._linkdb_to_dict(self.db.get_back_link())
        forward_link = self._linkdb_to_dict(self.db.get_forward_link())
        doc_size = len(self.doc_ids)

        scores = np.ones(doc_size, dtype=np.float32) / doc_size

        delta = 1
        while delta > self.e:
            for idx, doc_id in enumerate(self.doc_ids):
                sum_scores = 0
                if doc_id in back_link.keys():
                    target = list(map(
                        lambda x: self.id2idx[x], back_link[doc_id]))
                    for score_idx, score in enumerate(scores[target]):
                        target_doc_id = self.doc_ids[target[score_idx]]
                        if target_doc_id in forward_link.keys():
                            num_forward = len(forward_link[target_doc_id])
                            target_score = scores[self.id2idx[target_doc_id]]
                            sum_scores += target_score / num_forward

                scores[idx] = (self.jump_prob / doc_size) + \
                    (1 - self.jump_prob) * sum_scores
            print(np.sum(scores))
            break

        return scores

    def _page_rank(self):
        pass
        # doc_ids = sorted(doc_ids)
        # doc_size = len(doc_ids)
        # link = self._tuple_to_dict(self.db.get_document_relate_link(doc_ids))

        # matrix = np.ones(doc_size, dtype=np.float32) * (1 / doc_size)

        # epsilon = 1
        # while epsilon > self.e:
        #     for idx, doc_id in enumerate(doc_ids):
        #         matrix[idx] = (self.jump_prob / doc_size) + (1 - self.jump_prob) * sum([score / (
        #             link[doc_ids[inner_idx]] if doc_ids[inner_idx] in link.keys() else 0) for inner_idx, score in enumerate(matrix) if inner_idx != idx])
        #     print(np.sum(matrix))
        #     epsilon = abs(epsilon - np.sum(matrix))

        # print(matrix)

    def _linkdb_to_dict(self, data):
        return {k: [int(tmp) for tmp in v.split(',')] + [k] if v is not None else [] for k, v in data}
