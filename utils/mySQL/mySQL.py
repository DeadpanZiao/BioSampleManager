import mysql.connector

from utils.mySQL import MYSQL_HOST, MYSQL_USERNAME, MYSQL_PASSWORD, MYSQL_PORT


class TableManager:
    def __init__(self, db_name):
        self.conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USERNAME,
            password=MYSQL_PASSWORD,
            database=db_name,
            port=MYSQL_PORT
        )
        self.cursor = self.conn.cursor()

    def create_table(self, table_name, keywords):
        columns = ", ".join([f"{keyword} VARCHAR(255)" for keyword in keywords])
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
        self.cursor.execute(query)

    def insert_data(self, table_name, data):
        placeholders = ", ".join(["%s" for _ in data])
        query = f"INSERT INTO {table_name} VALUES ({placeholders})"
        self.cursor.execute(query, data)
        self.conn.commit()

    def delete_data(self, table_name, keyword, value):
        query = f"DELETE FROM {table_name} WHERE {keyword} = %s"
        self.cursor.execute(query, (value,))
        self.conn.commit()

    def update_data(self, table_name, keyword, value, new_data):
        set_clause = ", ".join([f"{k} = %s" for k in new_data.keys()])
        query = f"UPDATE {table_name} SET {set_clause} WHERE {keyword} = %s"
        self.cursor.execute(query, (*new_data.values(), value))
        self.conn.commit()

    def select_data(self, table_name, keyword, value):
        query = f"SELECT * FROM {table_name} WHERE {keyword} = %s"
        self.cursor.execute(query, (value,))
        return self.cursor.fetchall()

    def close(self):
        self.conn.close()


# 使用例子
table_manager = TableManager('testdb')
table_manager.create_table('test_table1', ['keyword1', 'keyword2', 'keyword3'])
table_manager.insert_data('test_table1', ('value1', 'value2', 'value3'))
print(table_manager.select_data('test_table1', 'keyword1', 'value1'))
table_manager.update_data('test_table1', 'keyword1', 'value1', {'keyword2': 'new_value2'})
print(table_manager.select_data('test_table1', 'keyword1', 'value1'))
table_manager.delete_data('test_table1', 'keyword1', 'value1')
print(table_manager.select_data('test_table1', 'keyword1', 'value1'))
table_manager.close()
