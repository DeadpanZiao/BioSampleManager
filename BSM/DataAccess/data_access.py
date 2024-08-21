import json
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
        self.list_columns = ["dataset","pmid", "pmcid", "doi", "other_ids", "organ", "technology_name", "disease",
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

    def check_if_exists(self, dataset, pmid, pmcid, doi, other_ids):
        # Prepare the conditions for the SQL query
        conditions = []
        params = []

        # Convert lists to JSON strings for comparison
        if isinstance(dataset, list):
            dataset_str = json.dumps(dataset)
            conditions.append("dataset LIKE ? OR dataset LIKE ?")
            params.extend([f'%{dataset_str}%', f'%{json.dumps(json.loads(dataset_str) + ["%"])}%'])
        elif dataset is not None:
            conditions.append("dataset = ?")
            params.append(dataset)

        if isinstance(pmid, list):
            pmid_str = json.dumps(pmid)
            conditions.append("pmid LIKE ? OR pmid LIKE ?")
            params.extend([f'%{pmid_str}%', f'%{json.dumps(json.loads(pmid_str) + ["%"])}%'])
        elif pmid is not None:
            conditions.append("pmid = ?")
            params.append(pmid)

        if isinstance(pmcid, list):
            pmcid_str = json.dumps(pmcid)
            conditions.append("pmcid LIKE ? OR pmcid LIKE ?")
            params.extend([f'%{pmcid_str}%', f'%{json.dumps(json.loads(pmcid_str) + ["%"])}%'])
        elif pmcid is not None:
            conditions.append("pmcid = ?")
            params.append(pmcid)

        if isinstance(doi, list):
            doi_str = json.dumps(doi)
            conditions.append("doi LIKE ? OR doi LIKE ?")
            params.extend([f'%{doi_str}%', f'%{json.dumps(json.loads(doi_str) + ["%"])}%'])
        elif doi is not None:
            conditions.append("doi = ?")
            params.append(doi)

        # Add conditions for other_ids
        if isinstance(other_ids, list):
            other_ids_str = json.dumps(other_ids)
            conditions.append("other_ids LIKE ? OR other_ids LIKE ?")
            params.extend([f'%{other_ids_str}%', f'%{json.dumps(json.loads(other_ids_str) + ["%"])}%'])
        elif other_ids is not None:
            conditions.append("other_ids = ?")
            params.append(other_ids)

        # Check if all parameters are None or empty
        if not (dataset or pmid or pmcid or doi or other_ids):
            return False

        # Construct the SQL query
        check_query = """
        SELECT COUNT(*), dataset, pmid, pmcid, doi, other_ids FROM {table_name}
        WHERE {conditions}
        """.format(table_name=self.table_name, conditions=" AND ".join(conditions))

        # Execute the query
        self.cursor.execute(check_query, params)
        result = self.cursor.fetchone()

        if result and result[0] > 0:
            # Record exists
            existing_dataset, existing_pmid, existing_pmcid, existing_doi, existing_other_ids = result[1:]
            self.logger.info(f"Record already exists with the following values: "
                             f"dataset={existing_dataset}, pmid={existing_pmid}, pmcid={existing_pmcid}, "
                             f"doi={existing_doi}, other_ids={existing_other_ids}")
            return True
        else:
            return False

    def close(self):
        self.conn.close()

