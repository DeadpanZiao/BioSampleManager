from BSM.DataAccess import DataAccess
from BSM.DataAccess.data_access import ProjectAccess, SampleAccess


class DataController:
    def __init__(self, db_name='../../DBS/test.db'):
        self.data_access = DataAccess(db_name)

    def read_all_data(self):
        try:
            result = self.data_access.query()
            if result['status'] == 'success':
                return result['data']
            else:
                return None
        except Exception as e:
            return None

    def close(self):
        self.data_access.close()


class ProjectController:
    def __init__(self, db_name='../../DBS/test.db'):
        self.data_access = ProjectAccess(db_name)

    def insert_project(self, publication):
        try:
            if isinstance(publication, dict):
                publication = tuple(publication.values())
            result = self.data_access.insert_publication(publication)
            if result['status'] == 'success':
                result['data'] = publication
                return result
            else:
                return result
        except Exception as e:
            return 'insert project failed'

    def close(self):
        self.data_access.close()


class SampleController:
    def __init__(self, db_name='../../DBS/test.db'):
        self.data_access = SampleAccess(db_name)

    def insert_sample(self, sample):
        try:
            if isinstance(sample, dict):
                sample = tuple(sample.values())
            result = self.data_access.insert_sample(sample)
            if result['status'] == 'success':
                result['data'] = sample
                return result
            else:
                return result
        except Exception as e:
            return 'insert project failed'

    def close(self):
        self.data_access.close()

# Example usage
if __name__ == "__main__":
    db_name = '../../DBS/projects.db'
    # insert sample
    test_sample= {
        "source_sample_id": "SS1234567",
        "project_id": "PRJ1234",
        "datasetID": "DS001",
        "experiment_id": "EXP12345",
        "experiment_description": "RNA sequencing of human liver tissue",
        "protocol_description": "Illumina TruSeq RNA Library Prep",
        "species": "Homo sapiens",
        "species_id": 9606,
        "sample_id": "SMP123456",
        "sample_name": "Liver Tissue Sample 1",
        "sample_description": "Fresh frozen liver biopsy from a patient with hepatocellular carcinoma",
        "organ": "Liver",
        "organ_ontology_id": "UBERON:0002107",
        "organ_tax2": "liver",
        "organ_tax2_ontology": "UBERON:0002107",
        "organ_note": "Sample taken from the right lobe",
        "individual_id": "INDV12345",
        "individual_name": "Patient 1",
        "gender": "Male",
        "age": 45,
        "age_unit": "years",
        "development_stage": "Adult",
        "individual_status": "Cancer patient",
        "ethnic_group": "Caucasian",
        "current_diagnostic": "Hepatocellular carcinoma",
        "phenotype": "Hepatomegaly",
        "treatment": "Sorafenib",
        "treatment_duration": "3 months",
        "disease_note": "Stage IIIA",
        "disease": "Hepatocellular carcinoma",
        "disease_id": "DOID:9352",
        "library_strategy": "RNA-Seq",
        "library_layout": "PAIRED",
        "library_selection": "polyA",
        "technology_name": "NextSeq 500",
        "tech_company": "Illumina",
        "release_time": "2023-08-01",
        "sequencer_name": "NextSeq 500",
        "sequencer_company": "Illumina",
        "extraction_protocol": "QIAzol Lysis Reagent",
        "data_processing": "Trimming and alignment using HISAT2",
        "source_name": "Hospital A",
        "is_cancer": True,
        "sample_location": "Right lobe of liver"
    }
    test_values = tuple(test_sample.values())
    controller = SampleController(db_name)
    res = controller.insert_sample(test_values)
    print(res)

    # insert project
    test_publication = (
    '10.1038/s41590-022-01165-333', 34925200, None, 'dataset1', 'Title of the article', 'Author Name', 12345, 123456,
    '2022-07-27', 'Full Journal Name', 'Journal Abbreviation', '2022-07-27', 'Article', 'Abstract of the article',
    'keyword1, keyword2', 'pub_status', 'mesh_term', 'fulltext_link', 'topic')
    controller = ProjectController(db_name)
    res = controller.insert_project(test_publication)
    print(res)
