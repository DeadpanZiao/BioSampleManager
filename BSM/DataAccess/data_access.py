import sqlite3
import logging

class DataAccess:
    def __init__(self, db_name):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self.table_name = 'data'
        self.setup_logging()
        self.create_table()

    def setup_logging(self):
        logging.basicConfig(filename='data_access.log', level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

    def create_table(self):
        fields = [
            'Collection_name TEXT',
            'Collection_ID TEXT',
            'Collection_summary TEXT',
            'INSDC_project TEXT',
            'INSDC_study TEXT',
            'GEO TEXT',
            'Publication_title TEXT',
            'Publication_doi TEXT CHECK(Publication_doi NOT LIKE \'https://%\')',
            'Publication_PMCID TEXT',
            'Organism TEXT',
            'Disease TEXT',
            'Tissue TEXT',
            'Tech TEXT',
            'RNA_source TEXT',
            'Public BOOLEAN'
        ]

        create_table_sql = f"CREATE TABLE IF NOT EXISTS {self.table_name} (" + ", ".join(fields) + ");"
        try:
            self.cursor.execute(create_table_sql)
            self.conn.commit()
            self.logger.info('Table created successfully or already exists.')
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f'Error creating table: {str(e)}')
            print(f'Error creating table: {str(e)}')

    def insert(self, data):
        try:
            keys = ', '.join(data.keys())
            placeholders = ', '.join(['?'] * len(data))
            sql = f'INSERT INTO {self.table_name} ({keys}) VALUES ({placeholders})'
            self.cursor.execute(sql, tuple(data.values()))
            self.conn.commit()
            result = {'status': 'success', 'message': 'Record inserted successfully'}
        except Exception as e:
            self.conn.rollback()
            result = {'status': 'error', 'message': str(e)}

        self.logger.info(f'Insert operation: {result}')
        return result

    def delete(self, condition):
        try:
            sql = f'DELETE FROM {self.table_name} WHERE {condition}'
            self.cursor.execute(sql)
            self.conn.commit()
            result = {'status': 'success', 'message': 'Record deleted successfully'}
        except Exception as e:
            self.conn.rollback()
            result = {'status': 'error', 'message': str(e)}

        self.logger.info(f'Delete operation: {result}')
        return result

    def update(self, data, condition):
        try:
            set_clause = ', '.join([f'{k} = ?' for k in data])
            sql = f'UPDATE {self.table_name} SET {set_clause} WHERE {condition}'
            self.cursor.execute(sql, tuple(data.values()))
            self.conn.commit()
            result = {'status': 'success', 'message': 'Record updated successfully'}
        except Exception as e:
            self.conn.rollback()
            result = {'status': 'error', 'message': str(e)}

        self.logger.info(f'Update operation: {result}')
        return result

    def query(self, columns='*', condition=None):
        try:
            if condition:
                sql = f'SELECT {columns} FROM {self.table_name} WHERE {condition}'
            else:
                sql = f'SELECT {columns} FROM {self.table_name}'
            self.cursor.execute(sql)
            rows = self.cursor.fetchall()
            col_names = [description[0] for description in self.cursor.description]
            result = {'status': 'success', 'data': [dict(zip(col_names, row)) for row in rows]}
        except Exception as e:
            result = {'status': 'error', 'message': str(e)}

        self.logger.info(f'Query operation: {result}')
        return result

    def close(self):
        self.conn.close()
        self.logger.info('Database connection closed')

# db_name = '../../DBS/test.db'
# db = DataAccess(db_name)
# # 准备要插入的数据
# data_to_insert = {
#     'collection_name': 'Sample Atlas',
#     'collection_id': 'abc123',
#     'publication_title': 'A study on...',
#     'publication_doi': '10.1001/jama.2021.12345',
#     'organism': 'Homo sapiens',
#     'tissue': 'Brain',
#     'tech': 'RNA-seq',
#     'rna_source': 'Nuclei',
#     'public': True
# }
# db.insert(data_to_insert)
# res = db.query()
