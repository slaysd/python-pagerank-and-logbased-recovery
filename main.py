from generator import SearchEngineGenerator
from pagerank import PageRank
from nltk.tokenize import word_tokenize

if __name__ == '__main__':
    generator = SearchEngineGenerator()

    # Generate invert index
    generator()

    while True:
        query = input('2018-26161> ')
        if query in '/quit':
            print("Okay bye!")
            break

        query_terms = word_tokenize(query.lower().strip())
