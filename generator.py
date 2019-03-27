import sys
import time
import numpy as np

from nltk.tokenize import word_tokenize
from collections import Counter

from datasource import Datasource
from tf_idf import TF_IDF
from pagerank import PageRank


class SearchEngineGenerator(object):
    def __init__(self):
        self.db = Datasource.instance()
        self.num_doc = self.db.get_num_doc()
        self.inverted_index = {}
        self.doc_info = {}
        self.pagerank = PageRank()

        if not self.num_doc:
            print("There are no documuents. Bye!")
            sys.exit()

    def __call__(self):

        start = time.time()
        print("building tables...")

        # self._generate_inverted_index()
        self._generate_pagerank()

        # self._update_database()

        print("Done... {:02f}s".format(time.time() - start))
        print("Ready to search")

    def _generate_inverted_index(self):
        '''
        Inverted Index 생성
        '''
        result = self.db.get_all_text()

        for doc_id, text in result:
            if doc_id not in self.doc_info.keys():
                self.doc_info[doc_id] = {}
            terms = word_tokenize(text.lower().strip())
            self.doc_info[doc_id]['nd'] = len(terms)
            num_terms_in_doc = Counter(terms)

            for term, freq in num_terms_in_doc.items():
                if term not in self.inverted_index.keys():
                    self.inverted_index[term] = list()
                self.inverted_index[term].append((doc_id, freq))

    def _generate_pagerank(self):
        '''
        Page Rank 생성
        '''
        rank_vector = self.pagerank()

    def _update_database(self):
        self.db.bulk_update_inverted_index(self.inverted_index)
