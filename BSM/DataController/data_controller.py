import os

from BSM.DataAccess import SampleAccess
OVERLAP_CHECK_LEVEL = 3

class SampleController:
    def __init__(self, db_name='../../DBS/test.db'):
        self.data_access = SampleAccess(db_name)

    def insert_sample(self, sample):
        # Check if all keys in the sample dictionary are valid column names
        invalid_keys = set(sample.keys()) - set(self.data_access._columns)
        if invalid_keys:
            error_message = f"The following keys are not valid column names: {', '.join(invalid_keys)}"
            return {"status": "error", "data": error_message}

        # Check if the record already exists based on the specified criteria
        exists_criteria = self.data_access.check_if_exists(
            sample.get('geo_id'),
            sample.get('pmid'),
            sample.get('pmcid'),
            sample.get('doi'),
            sample.get('other_ids')
        )

        # Determine if the record exists based on the number of matching criteria
        if sum(exists_criteria.values()) >= OVERLAP_CHECK_LEVEL:
            return {"status": "exists", "data": sample}

        try:
            result = self.data_access.insert_sample(sample)
            return result
        except Exception as e:
            return {"status": "error", "data": str(e)}

    def close(self):
        self.data_access.close()


# Example usage
if __name__ == "__main__":
    db_name = '../../DBS/test-projects2.db'
    if not os.path.exists('../../DBS'):
        os.mkdir('../../DBS')
    # insert sample
    data_to_insert = {
        "geo_id": ["GSE1234"],
        "pmid": ["1234567891"],
        "doi": [
        "10.1016/j.xgen.2022.100108"
    ],
        "pmcid": ["PMC12345678"],
        "title": "Example Title",
        "other_ids":  ["48259aa8-f168-4bf5-b797-af8e88da6637",
                       "67e75752-53dd-4aec-92be-a99fb73707af"],

        "raw_json": {
            'key': 'val'
        }
        # ... add more fields as needed
    }
    controller = SampleController(db_name)
    res = controller.insert_sample(data_to_insert)
    print(res)


