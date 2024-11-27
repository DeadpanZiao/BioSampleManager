import logging

logging.basicConfig(level=logging.INFO)


class BaseDownloader(object):
    def __init__(self, data_source):
        self.logger = logging.getLogger(__name__)
        self.data_source = data_source

    def execute(self, output_folder):
        raise NotImplementedError("Subclasses must implement the fetch method.")

    def execute_once(self, data_, output_folder):
        raise NotImplementedError("Subclasses must implement the fetch method.")

    def read_data_source(self):
        return


class HCADownloader(BaseDownloader):

    def execute(self, output_folder):
        data = self.read_data_source()
        for data_ in data:
            self.execute_once(data_, output_folder)
        self.logger.info(f'download complete. File saved to {output_folder}')

    def execute_once(self, data_, output_folder):
        pass
