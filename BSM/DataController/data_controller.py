import os

from BSM.DataAccess.data_access import SampleAccess


class SampleController:
    def __init__(self, db_name='../../DBS/test.db'):
        self.data_access = SampleAccess(db_name)


    def insert_sample(self, sample):
        # Check if all keys in the sample dictionary are valid column names
        invalid_keys = set(sample.keys()) - set(self.data_access._columns)
        if invalid_keys:
            error_message = f"The following keys are not valid column names: {', '.join(invalid_keys)}"
            return {"status": "error", "data": error_message}

        try:
            result = self.data_access.insert_sample(sample)
            return result
        except Exception as e:
            return {"status": "error", "data": str(e)}

    def close(self):
        self.data_access.close()


# Example usage
if __name__ == "__main__":
    db_name = '../../DBS/projects.db'
    if not os.path.exists('../../DBS'):
        os.mkdir('../../DBS')
    # insert sample
    data_to_insert = {
        "dataset": "Dataset1",
        "pmid": "123456789",
        "doi": "10.1234/abcdef",
        "pmcid": "PMC12345678",
        "title": "Example Title",
        # ... add more fields as needed
    }
    controller = SampleController(db_name)
    res = controller.insert_sample(data_to_insert)
    print(res)


