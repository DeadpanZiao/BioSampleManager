import json
import sqlite3
import logging


class SampleAccess:
    def __init__(self, db_name):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self.table_name = 'Sample'
        self._columns = [
            "internal_id", "geo_id", "pmid", "pmcid", "doi",  "other_ids", "title", "topic", "resolution",
            "project_description", "disease", "species", "organ","technology_name",
            "library_strategy",  "nuclei_extraction", "dataset_source", "raw_json"
        ]
        self.list_columns = ["geo_id","pmid", "pmcid", "doi", "other_ids", "organ", "technology_name", "disease",
                             "species", "topic", "technology_name", "library_strategy"]
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
        # Define which columns should be able to store JSON lists
        list_columns = self.list_columns

        # Construct the columns definition
        columns = []
        for col in self._columns:
            if col in list_columns:
                columns.append(f"{col} TEXT")  # Store as TEXT because we'll serialize the list to JSON
            elif col == 'internal_id':
                columns.append(f"{col} INTEGER PRIMARY KEY AUTOINCREMENT")
            else:
                columns.append(f"{col} TEXT")

        columns_def = ', '.join(columns)

        query = f"CREATE TABLE IF NOT EXISTS {self.table_name} ({columns_def})"

        try:
            self.cursor.execute(query)
            self.conn.commit()
            self.logger.debug("Table created successfully.")
            return {"status": "success", "data": None}
        except Exception as e:
            self.logger.error(f"Failed to create table: {e}")
            return {"status": "error", "data": str(e)}

    def insert_sample(self, data):
        # Add the internal_id to the dictionary with a value of None so it gets auto-generated
        data['internal_id'] = None

        # Serialize list fields to JSON
        list_fields = self.list_columns
        for field in list_fields:
            if field in data and isinstance(data[field], list):
                data[field] = json.dumps(data[field])  # Serialize list to JSON string
        if 'raw_json' in data and isinstance(data['raw_json'], dict):
            data['raw_json'] = json.dumps(data['raw_json'])  # Serialize dict to JSON string
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

    def check_if_exists(self, geo_id, pmid, pmcid, doi, other_ids):
        # Initialize the results dictionary
        results = {
            'geo_id': False,
            'pmid': False,
            'pmcid': False,
            'doi': False,
            'other_ids': False
        }

        # Check if the record exists for each parameter
        if geo_id is not None:
            results['geo_id'] = self._check_list_existence('geo_id', geo_id)

        if pmid is not None:
            results['pmid'] = self._check_list_existence('pmid', pmid)

        if pmcid is not None:
            results['pmcid'] = self._check_list_existence('pmcid', pmcid)

        if doi is not None:
            results['doi'] = self._check_list_existence('doi', doi)

        if other_ids is not None:
            results['other_ids'] = self._check_list_existence('other_ids', other_ids)

        return results

    def _check_list_existence(self, column, values):
        # Serialize the list to a JSON string for comparison
        serialized_values = json.dumps(values)

        # Query the database to find a match
        query = f"SELECT COUNT(*) FROM {self.table_name} WHERE {column} = ?"
        self.cursor.execute(query, (serialized_values,))
        count = self.cursor.fetchone()[0]

        # Return True if a match is found, False otherwise
        return count > 0

    def close(self):
        self.conn.close()

