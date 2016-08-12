import os
import time
import sqlite3

class SqliteDB(object):
    def __init__(self, db_path):
        self.db_path = db_path

        self.conn = None

        # you need to check this before connect the database file
        # otherwise you will have disk IO error
        if os.path.exists(self.db_path) is True:
            os.remove(self.db_path)

    def open(self):
        self.conn = sqlite3.connect(self.db_path)

    def initialize(self):

        schema_text = """
            create table benchtable (
                name        text primary key,
                description text
            );
            """
        self.conn.executescript(schema_text)

    def close(self):
        self.conn.close()
        self.conf = None

    def commit(self):
        self.conn.commit()

    def insert(self, key, value):
        self.conn.execute("""
        insert into benchtable (name, description)
        values ('{k}', '{v}')
        """.format(k=key, v=value))

    def update(self, key, value):
        self.conn.execute("""
        update benchtable set description = {v} where name = {k}
        """.format(k=key, v=value))

    def select_all(self):
        cursor = self.conn.cursor()

        cursor.execute("select * from benchtable")

        table = [row for row in cursor.fetchall()]
        return table


