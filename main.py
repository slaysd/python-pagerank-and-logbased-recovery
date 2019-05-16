import os
from generator import SearchEngineGenerator
from search_engine import SearchEngine
from simulator import ScheduleSimulator


def main():
    # Instance
    generator = SearchEngineGenerator()
    search = SearchEngine()

    # Generate invert index
    ans = input('Do you want to build scores?[type \'y\' to build or pass]: ')
    if ans == 'y':
        generator()
    # generator()

    while True:
        query = input('2018-26161> ')
        if query in '/quit':
            print("Okay bye!")
            break
        elif query.startswith('-run'):
            path = query.split()[1].strip()
            if not os.path.exists(path):
                print("경로가 올바르지 않습니다.")
                continue
            # path = 'data/prj2.sched'
            simulator = ScheduleSimulator(path)
            simulator()

        results = search(query)

        for doc in results:
            print(search.result_formatting(doc))


if __name__ == '__main__':
    main()
