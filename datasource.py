import pymysql

from glob import glob
from singleton import SingletonInstance


class Datasource(SingletonInstance):

    def __init__(self, host='s.snu.ac.kr', port=3306, student_id='ADB2018_26161'):
        print("Connect DB...", end='')
        self.db = pymysql.connect(
            host=host, port=3306,
            user=student_id,
            passwd=student_id,
            db=student_id,
            charset='utf8'
        )
        self.cursor = self.db.cursor()
        # self._prepare_schema()
        print("Done!")

    def __del__(self):
        print("Close database connection..")
        self.db.close()
        self.cursor.close()

    def _prepare_schema(self):
        sql = """
            ALTER TABLE link
            ADD CONSTRAINT link_pk
            PRIMARY KEY (id_from, id_to);
        """
        self.cursor.execute(sql)

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
        tuples = self._dict_to_inverted_tuple(data)
        self.cursor.executemany(sql, tuples)
        self.db.commit()

    def bulk_update_doc_info(self, data):
        truncate_sql = """
            TRUNCATE TABLE doc_info;
        """
        sql = """
            INSERT INTO doc_info(id, doc_terms, page_rank)
            values ( %s, %s, %s )
        """
        self.cursor.execute(truncate_sql)
        tuples = self._dict_to_info_tuple(data)
        self.cursor.executemany(sql, tuples)
        self.db.commit()

    def get_document_containing_terms(self, terms):
        target_sql = """
            SELECT *
            FROM inverted_index
            WHERE UPPER(term) = UPPER(%s)
        """
        for _ in range(len(terms) - 1):
            target_sql += ' OR UPPER(term) = UPPER(%s)'

        sql = f"""
            SELECT wiki.id, wiki.title, search.term, search.freq, doc_info.doc_terms, doc_info.page_rank
            FROM ({target_sql}) AS search
        """
        sql += """
            LEFT OUTER JOIN wiki
            ON search.id = wiki.id
            LEFT OUTER JOIN doc_info
            ON search.id = doc_info.id
            ORDER BY search.term
        """

        number_of_rows = self.cursor.execute(sql, terms)
        return self.cursor

    def get_old_value_for_log(self, table, column, key):
        sql = f"""
            SELECT {column} FROM {table} WHERE id = {key}
        """
        number_of_rows = self.cursor.execute(sql)
        result = self.cursor.fetchone()
        return result[0] if result else None

    def get_all_tuple_for_log(self, table, target_column, key):
        sql = f"""
            SELECT * FROM {table} WHERE {target_column} = {key}
        """

        number_of_rows = self.cursor.execute(sql)
        result = self.cursor.fetchall()
        return result if result else None

    def update_table(self, table, key_field, key, target_field, value):
        sql = f"""
            UPDATE {table} SET {target_field} = %s WHERE {key_field} = {key}
        """

        self.cursor.execute(sql, value)
        self.db.commit()

    def insert_table(self, table, value):
        sql = f"""
            INSERT IGNORE INTO {table} VALUES {value}
        """

        self.cursor.execute(sql)
        self.db.commit()

    def delete_table(self, table, key_field, key):
        sql = f"""
            DELETE FROM {table} WHERE {key_field} = {key}
        """

        self.cursor.execute(sql)
        self.db.commit()

    def free_sql(self, sql):
        self.cursor.execute(sql)
        self.db.commit()

    def _dict_to_inverted_tuple(self, data):
        result = []
        for term, info in data.items():
            doc_ids, freq = zip(*info)
            result += zip([term, ] * len(doc_ids), doc_ids, freq)
        return result

    def _dict_to_info_tuple(self, data):
        return [(doc_id, info['nd'] if 'nd' in info.keys() else None, info['rank']) for doc_id, info in data.items()]
