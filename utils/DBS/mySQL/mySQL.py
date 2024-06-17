import sqlite3
import json

class TableManager(object):
    def __init__(self, db_name):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()

    def create_table(self, table_name, keywords):
        columns = ", ".join([f"{keyword} TEXT" for keyword in keywords])
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
        self.cursor.execute(query)
        self.conn.commit()

    def insert_data(self, table_name, data):
        json_data = json.dumps(data)
        query = f"INSERT INTO {table_name} VALUES (?)"
        self.cursor.execute(query, (json_data,))
        self.conn.commit()

    def delete_data(self, table_name, keyword, value):
        query = f"DELETE FROM {table_name} WHERE {keyword} = ?"
        self.cursor.execute(query, (value,))
        self.conn.commit()

    def update_data(self, table_name, keyword, value, new_data):
        set_clause = ", ".join([f"{k} = ?" for k in new_data.keys()])
        json_data = json.dumps(new_data)
        query = f"UPDATE {table_name} SET {set_clause} WHERE {keyword} = ?"
        values = [json_data, value]
        self.cursor.execute(query, values)
        self.conn.commit()

    def select_data(self, table_name, keyword, value):
        query = f"SELECT * FROM {table_name} WHERE {keyword} = ?"
        self.cursor.execute(query, (value,))
        rows = self.cursor.fetchall()
        result = []
        for row in rows:
            result.append(json.loads(row[0]))
        return result

    def close(self):
        self.conn.close()
