import sqlite3
import logging


class SampleAccess:
    def __init__(self, db_name):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self.table_name = 'Sample'
        self._columns = [
            "internal_id", "dataset", "pmid", "pmcid", "doi",  "other_ids", "title", "topic", "resolution",
            "project_description", "disease", "species", "organ","technology_name",
            "library_strategy",  "nuclei_extraction", "dataset_source", "raw_json"
        ]
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
        # Add an auto-incrementing integer column as the primary key
        columns = ', '.join([f"{col} TEXT" if col != 'internal_id' else f"{col} INTEGER PRIMARY KEY AUTOINCREMENT" for col in self._columns])
        query = f"CREATE TABLE IF NOT EXISTS {self.table_name} ({columns})"
        try:
            self.cursor.execute(query)
            self.conn.commit()
            self.logger.info("Table created successfully.")
            return {"status": "success", "data": None}
        except Exception as e:
            self.logger.error(f"Failed to create table: {e}")
            return {"status": "error", "data": str(e)}

    def insert_sample(self, data):
        # Add the internal_id to the dictionary with a value of None so it gets auto-generated
        data['internal_id'] = None
        placeholders = ', '.join(['?'] * len(data))
        columns = ', '.join(data.keys())
        values = tuple(data.values())
        query = f"INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders})"
        try:
            self.cursor.execute(query, values)
            self.conn.commit()

            # Get the last inserted id
            internal_id = self.cursor.lastrowid
            data['internal_id'] = internal_id
            self.logger.info("Record inserted successfully.")
            return {"status": "success", "data": data}
        except Exception as e:
            self.logger.error(f"Failed to insert record: {e}")
            return {"status": "error", "data": str(e)}

    def delete_sample(self, doi):
        query = f"DELETE FROM {self.table_name} WHERE doi=?"
        try:
            self.cursor.execute(query, (doi,))
            self.conn.commit()
            self.logger.info(f"Record with DOI {doi} deleted successfully.")
            return {"status": "success", "data": None}
        except Exception as e:
            self.logger.error(f"Failed to delete record: {e}")
            return {"status": "error", "data": str(e)}

    def update_sample(self, doi, data):
        set_clause = ', '.join([f"{k} = ?" for k in data.keys()])
        values = tuple(data.values()) + (doi,)
        query = f"UPDATE {self.table_name} SET {set_clause} WHERE doi=?"
        try:
            self.cursor.execute(query, values)
            self.conn.commit()
            self.logger.info(f"Record with DOI {doi} updated successfully.")
            return {"status": "success", "data": None}
        except Exception as e:
            self.logger.error(f"Failed to update record: {e}")
            return {"status": "error", "data": str(e)}

    def get_sample_by_doi(self, doi):
        query = f"SELECT * FROM {self.table_name} WHERE doi=?"
        try:
            self.cursor.execute(query, (doi,))
            row = self.cursor.fetchone()
            if row is not None:
                result = dict(zip(self._columns, row))
                self.logger.info(f"Retrieved record with DOI {doi}.")
                return {"status": "success", "data": result}
            else:
                self.logger.warning(f"No record found with DOI {doi}.")
                return {"status": "not_found", "data": None}
        except Exception as e:
            self.logger.error(f"Failed to retrieve record: {e}")
            return {"status": "error", "data": str(e)}

    def close(self):
        self.conn.close()
