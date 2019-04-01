
from generator import SearchEngineGenerator
from search_engine import SearchEngine
from nltk.tokenize import word_tokenize


def main():
    # Instance
    generator = SearchEngineGenerator()
    search = SearchEngine()

    # Generate invert index
    ans = input('Do you want to build scores?[type \'y\' to build or pass]: ')
    if ans == 'y':
        generator()

    while True:
        query = input('2018-26161> ')
        if query in '/quit':
            print("Okay bye!")
            break

        query_terms = set(word_tokenize(query.lower().strip()))
        results = search(query_terms)

        for doc in results:
            print("{}, {}, {}, {}".format(*doc[1:]))
        print()


if __name__ == '__main__':
    main()
