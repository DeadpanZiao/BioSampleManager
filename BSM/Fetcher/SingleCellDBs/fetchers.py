import logging
logging.basicConfig(level=logging.INFO)


class SingleCellDBFetcher(object):
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def fetch(self, db_name):
        raise NotImplementedError("Subclasses must implement the fetch method.")