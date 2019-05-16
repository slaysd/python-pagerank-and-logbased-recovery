from singleton import SingletonInstance
from schdule import ScheduleParser
from log import LogWriter, LogParser
from datasource import Datasource
from recovery_mangement import RecoveryManagement
from search_engine import SearchEngine
from generator import SearchEngineGenerator


class ScheduleSimulator(SingletonInstance):
    def __init__(self, sche_path, log_path='prj2.log', search_path='search.txt'):
        self.search_path = search_path

        self.db = Datasource()
        self.scheduler = ScheduleParser(sche_path)
        self.recovery = RecoveryManagement(log_path)
        self.log_writer = LogWriter(log_path)
        self.search = SearchEngine()
        self.generator = SearchEngineGenerator()

    def __call__(self):
        transaction = {}
        for idx, schdule in enumerate(self.scheduler()):
            groups = schdule.groups()
            command_type = groups[0]
            if 'recover' in command_type:
                transaction = {}
                self.recovery(idx)
                self.log_writer.set_checkpoint([])
                self.generator()
            else:
                self.log_writer(idx, schdule, transaction)
                if command_type.startswith("<T"):
                    if command_type not in transaction.keys():
                        transaction[command_type] = []
                    if groups[1] in 'commit':
                        for sql in transaction[command_type]:
                            self.db.free_sql(sql)
                        transaction[command_type] = []
                        self.generator()
                    elif groups[1] in 'rollback':
                        transaction[command_type] = []
                    else:
                        transaction[command_type].append(groups[1])
                elif 'search' in command_type:
                    query = groups[1].replace("\n", "").strip()
                    with open(self.search_path, "a") as f:
                        f.write(f"search {idx+1}\n")
                        f.write(f"query {query}\n")
                        results = self.search(query)
                        for doc in results:
                            f.write(f"{self.search.result_formatting(doc)}\n")
