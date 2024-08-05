import sqlite3
import logging


# class DataAccess:
#     def __init__(self, db_name):
#         self.conn = sqlite3.connect(db_name)
#         self.cursor = self.conn.cursor()
#         self.table_name = 'data'
#         self.setup_logging()
#         self.create_table()
#
#     def setup_logging(self):
#         logging.basicConfig(filename='data_access.log', level=logging.INFO,
#                             format='%(asctime)s - %(levelname)s - %(message)s')
#         self.logger = logging.getLogger(__name__)
#         console_handler = logging.StreamHandler()
#         console_handler.setLevel(logging.INFO)
#         formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
#         console_handler.setFormatter(formatter)
#         self.logger.addHandler(console_handler)
#
#     def create_table(self):
#         fields = [
#             'Collection_name TEXT',
#             'Collection_ID TEXT',
#             'Collection_summary TEXT',
#             'INSDC_project TEXT',
#             'INSDC_study TEXT',
#             'GEO TEXT',
#             'Publication_title TEXT',
#             'Publication_doi TEXT CHECK(Publication_doi NOT LIKE \'https://%\')',
#             'Publication_PMCID TEXT',
#             'Organism TEXT',
#             'Disease TEXT',
#             'Tissue TEXT',
#             'Tech TEXT',
#             'RNA_source TEXT',
#             'Public BOOLEAN'
#         ]
#
#         create_table_sql = f"CREATE TABLE IF NOT EXISTS {self.table_name} (" + ", ".join(fields) + ");"
#         try:
#             self.cursor.execute(create_table_sql)
#             self.conn.commit()
#             self.logger.info('Table created successfully or already exists.')
#         except Exception as e:
#             self.conn.rollback()
#             self.logger.error(f'Error creating table: {str(e)}')
#             print(f'Error creating table: {str(e)}')
#
#     def insert(self, data):
#         try:
#             keys = ', '.join(data.keys())
#             placeholders = ', '.join(['?'] * len(data))
#             sql = f'INSERT INTO {self.table_name} ({keys}) VALUES ({placeholders})'
#             self.cursor.execute(sql, tuple(data.values()))
#             self.conn.commit()
#             result = {'status': 'success', 'message': 'Record inserted successfully'}
#         except Exception as e:
#             self.conn.rollback()
#             result = {'status': 'error', 'message': str(e)}
#
#         self.logger.info(f'Insert operation: {result}')
#         return result
#
#     def delete(self, condition):
#         try:
#             sql = f'DELETE FROM {self.table_name} WHERE {condition}'
#             self.cursor.execute(sql)
#             self.conn.commit()
#             result = {'status': 'success', 'message': 'Record deleted successfully'}
#         except Exception as e:
#             self.conn.rollback()
#             result = {'status': 'error', 'message': str(e)}
#
#         self.logger.info(f'Delete operation: {result}')
#         return result
#
#     def update(self, data, condition):
#         try:
#             set_clause = ', '.join([f'{k} = ?' for k in data])
#             sql = f'UPDATE {self.table_name} SET {set_clause} WHERE {condition}'
#             self.cursor.execute(sql, tuple(data.values()))
#             self.conn.commit()
#             result = {'status': 'success', 'message': 'Record updated successfully'}
#         except Exception as e:
#             self.conn.rollback()
#             result = {'status': 'error', 'message': str(e)}
#
#         self.logger.info(f'Update operation: {result}')
#         return result
#
#     def query(self, columns='*', condition=None):
#         try:
#             if condition:
#                 sql = f'SELECT {columns} FROM {self.table_name} WHERE {condition}'
#             else:
#                 sql = f'SELECT {columns} FROM {self.table_name}'
#             self.cursor.execute(sql)
#             rows = self.cursor.fetchall()
#             col_names = [description[0] for description in self.cursor.description]
#             result = {'status': 'success', 'data': [dict(zip(col_names, row)) for row in rows]}
#         except Exception as e:
#             result = {'status': 'error', 'message': str(e)}
#
#         self.logger.info(f'Query operation: {result["status"]}')
#         return result
#
#     def close(self):
#         self.conn.close()
#         self.logger.info('Database connection closed')


# class ProjectAccess:
#     def __init__(self, db_name):
#         self.conn = sqlite3.connect(db_name)
#         self.cursor = self.conn.cursor()
#         self.table_name = 'publications'
#         self.setup_logging()
#         self.create_table()
#         self.publication_columns = [
#             "doi", "pmid", "pmcid", "dataset", "title", "authors", "issn", "eissn",
#             "epub_date", "journal_full_name", "journal", "pub_date", "pub_type",
#             "abstract", "keywords", "pub_status", "mesh_term", "fulltext_link", "topic"
#         ]
#
#     def setup_logging(self):
#         logging.basicConfig(filename='data_access.log', level=logging.INFO,
#                             format='%(asctime)s - %(levelname)s - %(message)s')
#         self.logger = logging.getLogger(__name__)
#         console_handler = logging.StreamHandler()
#         console_handler.setLevel(logging.INFO)
#         formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
#         console_handler.setFormatter(formatter)
#         self.logger.addHandler(console_handler)
#
#     def create_table(self):
#         """
#         创建一个名为 'publications' 的表，并设置 'doi' 为主键。
#         """
#         self.cursor.execute("""
#             CREATE TABLE IF NOT EXISTS publications (
#                 doi TEXT PRIMARY KEY,
#                 pmid INTEGER,
#                 pmcid TEXT,
#                 dataset TEXT,
#                 title TEXT,
#                 authors TEXT,
#                 issn INTEGER,
#                 eissn INTEGER,
#                 epub_date TEXT,
#                 journal_full_name TEXT,
#                 journal TEXT,
#                 pub_date TEXT,
#                 pub_type TEXT,
#                 abstract TEXT,
#                 keywords TEXT,
#                 pub_status TEXT,
#                 mesh_term TEXT,
#                 fulltext_link TEXT,
#                 topic TEXT
#             );
#         """)
#         self.conn.commit()
#         self.logger.info("Table created successfully")
#
#     def execute_sql(self, sql, data=None):
#         """
#         执行 SQL 语句，并返回操作的状态。
#         """
#         result = {'status': 'error', 'message': ''}
#
#         try:
#             if data:
#                 self.cursor.execute(sql, data)
#             else:
#                 self.cursor.execute(sql)
#             self.conn.commit()
#             result['status'] = 'success'
#             result['message'] = 'Operation completed successfully'
#         except Exception as e:
#             self.conn.rollback()
#             result['message'] = str(e)
#
#         self.logger.info(f'Execute SQL: {result}')
#         return result
#
#     def insert_publication(self, publication):
#         """
#         插入一条出版物记录到 'publications' 表中。
#         """
#         keys = ', '.join(self.publication_columns)
#         placeholders = ', '.join(['?'] * len(self.publication_columns))
#         sql = f'INSERT INTO {self.table_name} ({keys}) VALUES ({placeholders})'
#         return self.execute_sql(sql, publication)
#
#     def get_publication_by_doi(self, doi):
#         """
#         根据 DOI 查询出版物。
#         """
#         sql = "SELECT * FROM publications WHERE doi=?"
#         self.cursor.execute(sql, (doi,))
#         row = self.cursor.fetchone()
#         if row:
#             self.logger.info(f"Found publication with DOI {doi}")
#             return {'status': 'success', 'message': 'Publication found', 'data': row}
#         else:
#             self.logger.info(f"No publication found with DOI {doi}")
#             return {'status': 'error', 'message': 'Publication not found'}
#
#     def update_publication(self, doi, new_data):
#         """
#         更新给定 DOI 的出版物信息。
#         """
#         set_clause = ', '.join([f"{k}=?" for k in self.publication_columns[:-1]])  # Exclude 'doi' from the set clause
#         sql = f"UPDATE {self.table_name} SET {set_clause} WHERE doi=?"
#         return self.execute_sql(sql, (*new_data, doi))
#
#     def delete_publication(self, doi):
#         """
#         删除给定 DOI 的出版物。
#         """
#         sql = "DELETE FROM publications WHERE doi=?"
#         return self.execute_sql(sql, (doi,))
#
#     def close(self):
#         self.conn.close()
#         self.logger.info('Database connection closed')

class SampleAccess:
    def __init__(self, db_name):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self.table_name = 'Sample'
        self._columns = [
            "doi", "dataset", "pmid", "pmcid", "title", "topic",
            "project_title", "project_description", "is_cancer", "species", "organ",
            "sample_location", "library_strategy", "library_layout", "single_cell", "nuclei_extraction",
            "technology_name", "instrument", "extraction_protocol", "data_processing"
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
        columns = ', '.join([f"{col} TEXT" for col in self._columns])
        query = f"CREATE TABLE IF NOT EXISTS {self.table_name} ({columns}, PRIMARY KEY(doi))"
        try:
            self.cursor.execute(query)
            self.conn.commit()
            self.logger.info("Table created successfully.")
            return {"status": "success", "data": None}
        except Exception as e:
            self.logger.error(f"Failed to create table: {e}")
            return {"status": "error", "data": str(e)}

    def insert_sample(self, data):
        placeholders = ', '.join(['?'] * len(data))
        columns = ', '.join(data.keys())
        values = tuple(data.values())
        query = f"INSERT INTO {self.table_name} ({columns}) VALUES ({placeholders})"
        try:
            self.cursor.execute(query, values)
            self.conn.commit()
            self.logger.info("Record inserted successfully.")
            return {"status": "success", "data": None}
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

    def get_publication_by_doi(self, doi):
        query = f"SELECT * FROM {self.table_name} WHERE doi=?"
        try:
            self.cursor.execute(query, (doi,))
            row = self.cursor.fetchone()
            if row is not None:
                result = dict(zip(self.publication_columns, row))
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




# class SampleAccess:
#     def __init__(self, db_name):
#         self.conn = sqlite3.connect(db_name)
#         self.cursor = self.conn.cursor()
#         self.table_name = 'samples'
#         self.setup_logging()
#         self.create_table()
#
#     def setup_logging(self):
#         logging.basicConfig(filename='data_access.log', level=logging.INFO,
#                             format='%(asctime)s - %(levelname)s - %(message)s')
#         self.logger = logging.getLogger(__name__)
#         console_handler = logging.StreamHandler()
#         console_handler.setLevel(logging.INFO)
#         formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
#         console_handler.setFormatter(formatter)
#         self.logger.addHandler(console_handler)
#
#     def create_table(self):
#         """
#         创建一个名为 'samples' 的表，并设置 'source_sample_id' 为主键。
#         """
#         self.cursor.execute("""
#             CREATE TABLE IF NOT EXISTS samples (
#                 source_sample_id TEXT PRIMARY KEY,
#                 project_id TEXT,
#                 datasetID TEXT,
#                 experiment_id TEXT,
#                 experiment_description TEXT,
#                 protocol_description TEXT,
#                 species TEXT,
#                 species_id INTEGER,
#                 sample_id TEXT,
#                 sample_name TEXT,
#                 sample_description TEXT,
#                 organ TEXT,
#                 organ_ontology_id TEXT,
#                 organ_tax2 TEXT,
#                 organ_tax2_ontology TEXT,
#                 organ_note TEXT,
#                 individual_id TEXT,
#                 individual_name TEXT,
#                 gender TEXT,
#                 age INTEGER,
#                 age_unit TEXT,
#                 development_stage TEXT,
#                 individual_status TEXT,
#                 ethnic_group TEXT,
#                 current_diagnostic TEXT,
#                 phenotype TEXT,
#                 treatment TEXT,
#                 treatment_duration TEXT,
#                 disease_note TEXT,
#                 disease TEXT,
#                 disease_id TEXT,
#                 library_strategy TEXT,
#                 library_layout TEXT,
#                 library_selection TEXT,
#                 technology_name TEXT,
#                 tech_company TEXT,
#                 release_time TEXT,
#                 sequencer_name TEXT,
#                 sequencer_company TEXT,
#                 extraction_protocol TEXT,
#                 data_processing TEXT,
#                 source_name TEXT,
#                 is_cancer BOOLEAN,
#                 sample_location TEXT
#             );
#         """)
#         self.conn.commit()
#         self.logger.info("Table created successfully")
#
#     def insert_sample(self, sample):
#         """
#         插入一条样本记录到 'samples' 表中。
#         """
#         try:
#             self.cursor.execute("""
#                 INSERT INTO samples (source_sample_id, project_id, datasetID, experiment_id, experiment_description,
#                 protocol_description, species, species_id, sample_id, sample_name, sample_description, organ,
#                 organ_ontology_id, organ_tax2, organ_tax2_ontology, organ_note, individual_id, individual_name, gender,
#                 age, age_unit, development_stage, individual_status, ethnic_group, current_diagnostic, phenotype,
#                 treatment, treatment_duration, disease_note, disease, disease_id, library_strategy, library_layout,
#                 library_selection, technology_name, tech_company, release_time, sequencer_name, sequencer_company,
#                 extraction_protocol, data_processing, source_name, is_cancer, sample_location)
#                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
#                         ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
#                         ?, ?, ?, ?);
#             """, sample)
#             self.conn.commit()
#             self.logger.info(f"Sample {sample[0]} inserted successfully")
#             return {'status': 'success', 'message': f'Sample {sample[0]} inserted successfully'}
#         except sqlite3.IntegrityError as e:
#             self.logger.error(f"Failed to insert sample {sample[0]}: {e}")
#             return {'status': 'error', 'message': f'Failed to insert sample {sample[0]}: {e}'}
#
#     def get_sample_by_id(self, source_sample_id):
#         """
#         根据 source_sample_id 查询样本。
#         """
#         self.cursor.execute(f"SELECT * FROM {self.table_name} WHERE source_sample_id=?", (source_sample_id,))
#         row = self.cursor.fetchone()
#         if row:
#             self.logger.info(f"Found sample {source_sample_id}")
#             return {'status': 'success', 'sample': row}
#         else:
#             self.logger.info(f"No sample found with: {source_sample_id}")
#             return {'status': 'error', 'message': f"No sample found with: {source_sample_id}"}
#
#     def update_sample(self, source_sample_id, new_data):
#         """
#         更新给定 source_sample_id 的样本信息。
#         """
#         try:
#             self.cursor.execute(f"""
#                 UPDATE {self.table_name} SET
#                     source_sample_id=?, project_id=?, datasetID=?, experiment_id=?, experiment_description=?,
#                     protocol_description=?, species=?, species_id=?, sample_id=?, sample_name=?, sample_description=?,
#                     organ=?, organ_ontology_id=?, organ_tax2=?, organ_tax2_ontology=?, organ_note=?, individual_id=?,
#                     individual_name=?, gender=?, age=?, age_unit=?, development_stage=?, individual_status=?,
#                     ethnic_group=?, current_diagnostic=?, phenotype=?, treatment=?, treatment_duration=?,
#                     disease_note=?, disease=?, disease_id=?, library_strategy=?, library_layout=?, library_selection=?,
#                     technology_name=?, tech_company=?, release_time=?, sequencer_name=?, sequencer_company=?,
#                     extraction_protocol=?, data_processing=?, source_name=?, is_cancer=?, sample_location=?
#                 WHERE source_sample_id=?
#             """, (*new_data, source_sample_id))
#             self.conn.commit()
#             self.logger.info(f"Sample {source_sample_id} updated successfully")
#             return {'status': 'success', 'message': f'Sample {source_sample_id} updated successfully'}
#         except sqlite3.Error as e:
#             self.logger.error(f"Failed to update sample {source_sample_id}: {e}")
#             return {'status': 'error', 'message': f'Failed to update sample {source_sample_id}: {e}'}
#
#     def delete_sample(self, source_sample_id):
#         """
#         删除给定 source_sample_id 的样本。
#         """
#         try:
#             self.cursor.execute(f"DELETE FROM {self.table_name} WHERE source_sample_id=?", (source_sample_id,))
#             self.conn.commit()
#             self.logger.info(f"Sample {source_sample_id} deleted successfully")
#             return {'status': 'success', 'message': f'Sample {source_sample_id} deleted successfully'}
#         except sqlite3.Error as e:
#             self.logger.error(f"Failed to delete sample {source_sample_id}: {e}")
#             return {'status': 'error', 'message': f'Failed to delete sample {source_sample_id}: {e}'}
#
#     def close(self):
#         self.conn.close()
#         self.logger.info('Database connection closed')



# if __name__ == "__main__":
#     # data_access = ProjectAccess('publications.db')
#     #
#     # # 插入示例数据
#     # publication = (
#     # '10.1038/s41590-022-01165-7', 34925200, None, 'dataset1', 'Title of the article', 'Author Name', 12345, 123456,
#     # '2022-07-27', 'Full Journal Name', 'Journal Abbreviation', '2022-07-27', 'Article', 'Abstract of the article',
#     # 'keyword1, keyword2', 'pub_status', 'mesh_term', 'fulltext_link', 'topic')
#     # data_access.insert_publication(publication)
#     #
#     # # 查询数据
#     # publication = data_access.get_publication_by_doi('10.1038/s41590-022-01165-7')
#     # print(publication)
#     #
#     # # 更新数据
#     # new_data = (34925201, None, 'dataset2', 'Updated Title', 'Updated Author', 12346, 123457, '2022-07-28',
#     #             'Updated Full Journal Name', 'Updated Journal Abbreviation', '2022-07-28', 'Review Article',
#     #             'Updated Abstract', 'updated_keyword1, updated_keyword2', 'updated_pub_status', 'updated_mesh_term',
#     #             'updated_fulltext_link', 'updated_topic')
#     # data_access.update_publication('10.1038/s41590-022-01165-7', new_data)
#     #
#     # # 删除数据
#     # # data_access.delete_publication('10.1038/s41590-022-01165-7')
#     #
#     # # 关闭连接
#     # data_access.close()
#
#     test_data = {
#         "source_sample_id": "SS123456",
#         "project_id": "PRJ1234",
#         "datasetID": "DS001",
#         "experiment_id": "EXP12345",
#         "experiment_description": "RNA sequencing of human liver tissue",
#         "protocol_description": "Illumina TruSeq RNA Library Prep",
#         "species": "Homo sapiens",
#         "species_id": 9606,
#         "sample_id": "SMP123456",
#         "sample_name": "Liver Tissue Sample 1",
#         "sample_description": "Fresh frozen liver biopsy from a patient with hepatocellular carcinoma",
#         "organ": "Liver",
#         "organ_ontology_id": "UBERON:0002107",
#         "organ_tax2": "liver",
#         "organ_tax2_ontology": "UBERON:0002107",
#         "organ_note": "Sample taken from the right lobe",
#         "individual_id": "INDV12345",
#         "individual_name": "Patient 1",
#         "gender": "Male",
#         "age": 45,
#         "age_unit": "years",
#         "development_stage": "Adult",
#         "individual_status": "Cancer patient",
#         "ethnic_group": "Caucasian",
#         "current_diagnostic": "Hepatocellular carcinoma",
#         "phenotype": "Hepatomegaly",
#         "treatment": "Sorafenib",
#         "treatment_duration": "3 months",
#         "disease_note": "Stage IIIA",
#         "disease": "Hepatocellular carcinoma",
#         "disease_id": "DOID:9352",
#         "library_strategy": "RNA-Seq",
#         "library_layout": "PAIRED",
#         "library_selection": "polyA",
#         "technology_name": "NextSeq 500",
#         "tech_company": "Illumina",
#         "release_time": "2023-08-01",
#         "sequencer_name": "NextSeq 500",
#         "sequencer_company": "Illumina",
#         "extraction_protocol": "QIAzol Lysis Reagent",
#         "data_processing": "Trimming and alignment using HISAT2",
#         "source_name": "Hospital A",
#         "is_cancer": True,
#         "sample_location": "Right lobe of liver"
#     }
#     test_values = tuple(test_data.values())
#     data_access = SampleAccess('sample.db')
#     data_access.insert_sample(test_values)
#     data = data_access.get_sample_by_id('SS123456')
#     print(data)
#     data_access.update_sample('SS123456', test_values)
