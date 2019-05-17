import re
import shutil
import os
from singleton import SingletonInstance
from datasource import Datasource


class LogParser(SingletonInstance):
    TRANSACTION_REGEX = r'^(<T\d+>)\s+(start|commit|abort)$'
    TRANSACTION_INSERT_REGEX = r'^(<T\d+>),\s+<(\w+)>\.<(\w+):(\d+)>,\s+<(None)>,\s+<(.+?)>$'
    TRANSACTION_DELETE_REGEX = r'^(<T\d+>),\s+<(\w+)>\.<(\w+):(\d+)>,\s+<(.+?)>,\s+<(None)>$'
    TRANSACTION_UPDATE_REGEX = r'^(<T\d+>),\s+<(\w+)>\.<(\w+):(\d+)>\.<(\w+)>,\s+<(.+)>,\s+<(.+)>$'
    CHECKPOINT_REGEX = r'^(checkpoint)\s*(<.+>,?)?$'
    RECOVER_REGEX = r'^(recover)\s+(\d+)$'

    def __init__(self, path):
        self.path = path
        self.tmp = f"{path}.tmp"
        self.transaction_pattern = re.compile(self.TRANSACTION_REGEX)
        self.insert_pattern = re.compile(self.TRANSACTION_INSERT_REGEX)
        self.delete_pattern = re.compile(self.TRANSACTION_DELETE_REGEX)
        self.update_pattern = re.compile(self.TRANSACTION_UPDATE_REGEX)
        self.recover_pattern = re.compile(self.RECOVER_REGEX)
        self.checkpoint_pattern = re.compile(self.CHECKPOINT_REGEX)

    def forward(self, n_line=None):
        with open(self.tmp, "r") as f:
            for idx, line in enumerate(f.readlines()):
                if n_line:
                    if idx < n_line:
                        continue
                for p in [self.transaction_pattern, self.delete_pattern, self.update_pattern, self.recover_pattern, self.insert_pattern, self.checkpoint_pattern]:
                    m = p.match(line)
                    if m:
                        yield m
                        break
                else:
                    print(line)
                    raise NotImplemented

    def backward(self):
        logs = reversed(open(self.tmp).readlines())
        for idx, line in enumerate(logs):
            for p in [self.transaction_pattern, self.insert_pattern, self.delete_pattern, self.update_pattern, self.recover_pattern, self.checkpoint_pattern]:
                m = p.match(line)
                if m:
                    yield m
                    break
            else:
                if not line.startswith("#"):
                    print(line)
                    raise NotImplemented

    def find_checkpoint(self):
        shutil.copyfile(self.path, self.tmp)

        logs = list(reversed(open(self.tmp).readlines()))
        for idx, line in enumerate(logs):
            m = self.checkpoint_pattern.match(line)
            if m:
                if m.groups()[1]:
                    return len(logs) - idx, m.groups()[1].split(',')
                else:
                    return len(logs) - idx, []
        return 0, []

    def find_transaction(self, t_id):
        start_flag = True

        result = []
        for line in reversed(open(self.tmp).readlines()):
            for p in [self.transaction_pattern, self.insert_pattern, self.update_pattern, self.delete_pattern]:
                m = p.match(line)
                if m and m.groups()[0] in t_id:
                    if 'start' in m.groups()[1]:
                        start_flag = False
                    if 'commit' in m.groups()[1] or 'abort' in m.groups()[1]:
                        continue
                    if not start_flag:
                        return list(reversed(result))

                    result.append(m.groups()[1:])
        return list(reversed(result))


class LogWriter(SingletonInstance):
    UPDATE_REGEX = r'^(UPDATE) (wiki) SET (title|text) = \'(.+)\' WHERE id = (\d+);$'
    DELETE_WIKI_REGEX = r'^(DELETE) FROM (wiki) WHERE (id) = (\d+);$'
    DELETE_LINK_REGEX = r'^(DELETE) FROM (link) WHERE (id_from|id_to) = (\d+);$'
    COMMIT_REGEX = r'^(commit)$'
    ROLLBACK_REGEX = r'^(rollback)$'

    def __init__(self, path):
        self.path = path
        self.update_pattern = re.compile(self.UPDATE_REGEX)
        self.delete_wiki_pattern = re.compile(self.DELETE_WIKI_REGEX)
        self.delete_link_pattern = re.compile(self.DELETE_LINK_REGEX)
        self.commit_pattern = re.compile(self.COMMIT_REGEX)
        self.rollback_pattern = re.compile(self.ROLLBACK_REGEX)
        self.db = Datasource()

        self.processing_transaction = []

    def __call__(self, line_number, command, transaction):
        line_number += 1

        command_type = command.groups()[0].strip()
        with open(self.path, 'a') as f:
            # 트랜잭션인 경우
            if command_type.startswith("<T"):
                if command_type not in self.processing_transaction:
                    f.write(f"{command_type} start\n")
                    self.processing_transaction.append(command_type)
                sql = command.groups()[1].strip()
                for p in [self.update_pattern, self.delete_wiki_pattern, self.delete_link_pattern, self.commit_pattern, self.rollback_pattern]:
                    m = p.match(sql)
                    if m:
                        sql_type = m.groups()[0]
                        if sql_type in 'UPDATE':
                            table, column, new_value, key = m.groups()[1:]
                            old_value = self.db.get_old_value_for_log(table, column, key)
                            f.write(f"{command_type}, <{table}>.<id:{key}>.<{column}>, <{old_value}>, <{new_value}>\n")
                        elif sql_type in 'DELETE':
                            table, target_column, key = m.groups()[1:]
                            old_tuple = self.db.get_all_tuple_for_log(table, target_column, key)
                            f.write(f"{command_type}, <{table}>.<{target_column}:{key}>, <{old_tuple}>, <None>\n")
                        elif sql_type in 'commit':
                            f.write(f"{command_type} commit\n")
                            try:
                                self.processing_transaction.remove(command_type)
                            except ValueError:
                                print(f"{command_type}\t스케쥴에 시작하지 않았던 트랜잭션입니다.")
                        elif sql_type in 'rollback':
                            self.undo_sql(f, command_type, transaction[command_type])
                            f.write(f"{command_type} abort\n")
                            try:
                                self.processing_transaction.remove(command_type)
                            except ValueError:
                                print(f"{command_type}\t스케쥴에 시작하지 않았던 트랜잭션입니다.")
                        else:
                            raise NotImplemented
                        break
                else:
                    raise NotImplemented
            # 체크포인트인 경우
            elif command_type in 'checkpoint':
                f.write(f"{command_type} {','.join(self.processing_transaction)}\n")
            elif command_type in 'recover':
                f.write(f"{command_type} {line_number}\n")

    def recover(self, recover_line):
        with open(self.path, "a") as f:
            f.write(f"recover {recover_line + 1}\n")

    def checkpoint(self):
        with open(self.path, "a") as f:
            f.write("checkpoint\n")

    def set_checkpoint(self, checkpoints):
        self.processing_transaction = checkpoints

    def undo_sql(self, f, t_id, transaction):
        for t in reversed(transaction):
            log = self.convert_to_undo_log_from_sql(t_id, t)
            f.write(f"{log}\n")

    def convert_to_undo_log_from_sql(self, t_id, t):
        for p in [self.update_pattern, self.delete_link_pattern, self.delete_wiki_pattern]:
            m = p.match(t)
            if m:
                sql_type = m.groups()[0]
                if sql_type in 'UPDATE':
                    table, column, new_value, key = m.groups()[1:]
                    old_value = self.db.get_old_value_for_log(table, column, key)
                    return f"{t_id}, <{table}>.<id:{key}>.<{column}>, <{new_value}>, <{old_value}>"
                elif sql_type in 'DELETE':
                    table, target_column, key = m.groups()[1:]
                    old_tuple = self.db.get_all_tuple_for_log(table, target_column, key)
                    return f"{t_id}, <{table}>.<{target_column}:{key}>, <None>, <{old_tuple}>"
                else:
                    raise NotImplemented

    def free_write(self, msg):
        with open(self.path, 'a') as f:
            f.write(f"{msg}\n")
