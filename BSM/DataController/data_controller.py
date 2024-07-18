from BSM.DataAccess import DataAccess


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


# Example usage
if __name__ == "__main__":
    db_name = '../../DBS/test.db'
    controller = DataController(db_name)
    data = controller.read_all_data()
    if data is not None:
        print("Data retrieved successfully:")
        for row in data:
            print(row)
    else:
        print("Failed to retrieve data.")
    controller.close()
