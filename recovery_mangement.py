from datasource import Datasource
from log import LogParser, LogWriter
from generator import SearchEngineGenerator


# #TODO undo redo 과정 입력하는거 잊지 말기
class RecoveryManagement(object):
    def __init__(self, log_path, path='recovery.txt'):
        self.path = path
        self.db = Datasource()
        self.log_parser = LogParser(log_path)
        self.log_writer = LogWriter(log_path)
        self.generator = SearchEngineGenerator()

    def __call__(self, recover_line):
        with open(self.path, "a") as f:
            save_redo_list = []
            n_line, undo_list = self.log_parser.find_checkpoint()
            for log in self.log_parser.forward(n_line=n_line):
                groups = log.groups()
                command_type = groups[0]
                if 'recover' in command_type:
                    break
                else:
                    if len(groups) == 2:
                        if 'start' in groups[1]:
                            undo_list.append(command_type)
                        elif 'commit' in groups[1]:
                            undo_list.remove(command_type)
                            # transaction = self.log_parser.find_transaction(command_type)
                            # for t in transaction:
                            #     self.execute_recovery(command_type, t, 'new', 'redo')
                            save_redo_list.append(command_type)
                        elif 'abort' in groups[1]:
                            undo_list.remove(command_type)
                            # transaction = self.log_parser.find_transaction(command_type)
                            # for t in transaction:
                            #     self.execute_recovery(command_type, t, 'old', 'redo')
                            save_redo_list.append(command_type)
                    elif command_type.startswith('<T'):
                        print(groups)
                        self.execute_recovery(command_type, groups[1:], 'new', 'redo')


            save_undo_list = undo_list.copy()
            for log in self.log_parser.backward():
                groups = log.groups()
                command_type = groups[0]
                if command_type in undo_list and 'start' in groups[1]:
                    undo_list.remove(command_type)
                    transaction = self.log_parser.find_transaction(command_type)
                    for t in reversed(transaction):
                        self.execute_recovery(command_type, t, 'old', 'undo')
                    self.log_writer.free_write(f"{command_type} abort")

            self.log_writer.recover(recover_line)
            self.log_writer.checkpoint()
            f.write(f"recover {recover_line}\n")
            f.write(f"redo {', '.join(save_redo_list)}\n")
            f.write(f"undo {', '.join(save_undo_list)}\n")

    def execute_recovery(self, t_id, t, value_type, do_type):
        '''
        t (tuple): 트랜잭션 튜플
        value_type (enum): old value인지 new value인지
            ['old', 'new']
        do_Type (enum): undo or redo
            ['undo', 'redo']
        '''
        assert value_type in ['old', 'new'], "Value 타입이 잘못되었습니다."
        assert do_type in ['undo', 'redo'], "Do 타입이 잘못되었습니다."

        is_undo = True if do_type == 'undo' else False

        # Update
        if len(t) == 6:
            table, key_field, key, target_field, old_value, new_value = t
            # value = new_value if value_type in 'new' else old_value
            value = new_value if do_type == 'redo' else old_value

            if is_undo:
                self.log_writer.free_write(f"{t_id}, <{table}>.<id:{key}>.<{target_field}>, <{new_value}>, <{old_value}>")
            else:
                self.log_writer.free_write(f"#redo {t_id}_{t}")
            self.db.update_table(table, key_field, key, target_field, value)
        # Delete
        elif len(t) == 5:
            table, key_field, key, old_tuple, _ = t
            if isinstance(old_tuple, str):
                if old_tuple != 'None':
                    old_tuple = old_tuple[1:-1]
                    if old_tuple.endswith(","):
                        old_tuple = old_tuple[:-1]

            if value_type == 'new':
                if is_undo:
                    self.log_writer.free_write(f"{t_id}, <{table}>.<{key_field}:{key}>, <{old_tuple}>, <None>")
                else:
                    self.log_writer.free_write(f"#redo {t_id}_{t}")
                self.db.delete_table(table, key_field, key)
            else:
                if old_tuple != 'None':
                    if is_undo:
                        self.log_writer.free_write(f"{t_id}, <{table}>.<{key_field}:{key}>, <None>, <{old_tuple}>")
                    else:
                        self.log_writer.free_write(f"#redo {t_id}_{t}")
                    self.db.insert_table(table, old_tuple)
