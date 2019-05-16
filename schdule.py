import re


class ScheduleParser(object):
    TRANSACTION_REGEX = r'^(<T\d+>)\s+(.+;?)'
    SEARCH_REGEX = r'^(search)\s+([\w ]+)'
    CHECKPOINT_REGEX = r'^(checkpoint)$'
    RECOVER_REGEX = r'^system failure - (recover)$'

    def __init__(self, path):
        self.path = path
        self.t_pattern = re.compile(self.TRANSACTION_REGEX)
        self.s_pattern = re.compile(self.SEARCH_REGEX)
        self.c_pattern = re.compile(self.CHECKPOINT_REGEX)
        self.r_pattern = re.compile(self.RECOVER_REGEX)

    def __call__(self):
        with open(self.path, "r") as f:
            for line in f.readlines():
                for p in [self.t_pattern, self.s_pattern, self.c_pattern, self.r_pattern]:
                    m = p.search(line if line else "")
                    if m:
                        yield m
                        break
                else:
                    raise NotImplemented
