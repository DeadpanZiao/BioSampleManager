import requests
from BSM.Fetcher.SingleCellDBs.fetchers import SingleCellDBFetcher
from BSM.Fetcher.utils import JsonManager


class CellxgeneFetcher(SingleCellDBFetcher):
    def __init__(self, domain_name="cellxgene.cziscience.com/curation/v1"):
        super().__init__()
        self.domain_name = domain_name
        self.api_url_base = f"https://api.{domain_name}"
        self.datasets_url = f"{self.api_url_base}/datasets"
        self.collections_url = f"{self.api_url_base}/collections"
        self.headers = {"Content-Type": "application/json"}

    def fetch(self, db_name):
        res = requests.get(url=self.datasets_url, headers=self.headers)
        res.raise_for_status()
        data = res.json()

        manager = JsonManager(db_name)
        manager.save(data)
        self.logger.info("Data saved successfully to JSON file.")

    def fetch_collections(self, db_name):
        res = requests.get(url=self.datasets_url, headers=self.headers)
        res.raise_for_status()
        data = res.json()

        manager = JsonManager(db_name)
        manager.save(data)
        self.logger.info("Data saved successfully to JSON file.")
    