import pymysql


class Datasource(object):
    _instance = None

    @classmethod
    def _getInstance(cls):
        return cls._instance

    @classmethod
    def instance(cls, *args, **kargs):
        cls._instance = cls(*args, **kargs)
        cls.instance = cls._getInstance
        return cls._instance

    def __init__(self, host='s.snu.ac.kr', port=3306, user='ADB2018-26161', passwd='ADB2018-26161'):
        print("Connect DB...", end='')
        self.db = pymysql.connect(
            host='localhost', port=3306,
            user='root',
            passwd='test',
            db='pagerank',
            charset='utf8'
        )
        self.cursor = self.db.cursor()
        print("Done!")

    def get_num_doc(self):
        sql = """
            SELECT count(*)
            FROM wiki
        """
        number_of_rows = self.cursor.execute(sql)

        return self.cursor.fetchone()

    def get_all_text(self):
        sql = """
            SELECT id, text
            FROM wiki
        """
        number_of_rows = self.cursor.execute(sql)

        return self.cursor

    def get_all_link_docs(self):
        sql = """
            SELECT id_from AS doc_id
            FROM link
            UNION DISTINCT
            SELECT id_to AS doc_id
            FROM link
        """
        number_of_rows = self.cursor.execute(sql)

        return self.cursor

    def get_back_link(self):
        sql = """
            SELECT id_to, GROUP_CONCAT(id_from)
            FROM link
            GROUP BY id_to
        """
        number_of_rows = self.cursor.execute(sql)

        return self.cursor

    def get_forward_link(self):
        sql = """
            SELECT id_from, GROUP_CONCAT(id_to)
            FROM link
            GROUP BY id_from
        """
        number_of_rows = self.cursor.execute(sql)

        return self.cursor

    def bulk_update_inverted_index(self, data):
        truncate_sql = """
            TRUNCATE TABLE inverted_index;
        """
        sql = """
            INSERT INTO inverted_index(term, id, freq)
            values ( %s, %s, %s )
        """
        self.cursor.execute(truncate_sql)
        tuples = self._dict_to_tuple(data)
        self.cursor.executemany(sql, tuples)
        self.db.commit()

    # TODO: 아래부터는 수정해야함
    def get_document_containing_terms(self, terms):
        sql = """
            SELECT a.term, a.freq, b.id, b.text
            FROM inverted_index AS a INNER JOIN wiki AS b
            ON a.id = b.id
            WHERE term = %s
        """
        for _ in range(len(terms) - 1):
            sql += ' OR term = %s'
        sql += ' ORDER BY term ASC'

        number_of_rows = self.cursor.execute(sql, terms)
        return self.cursor

    def get_document_relate_link(self, doc_ids):
        sql = """
            SELECT id_from, id_to
            FROM link
            WHERE
        """
        # Generate id_from sql
        from_sql = " (id_from = %s"
        for _ in range(len(doc_ids) - 1):
            from_sql += ' OR id_from = %s'
        from_sql += ")"
        # Generate id_to sql
        to_sql = from_sql.replace("id_from", "id_to")

        # Merge two sql
        sql = sql + from_sql + ' AND ' + to_sql

        number_of_rows = self.cursor.execute(sql, doc_ids * 2)
        return self.cursor.fetchall()

    def _dict_to_tuple(self, data):
        result = []
        for term, docs_info in data.items():
            doc_ids, freq = zip(*docs_info)
            result += zip([term, ] * len(doc_ids), doc_ids, freq)
        return result
