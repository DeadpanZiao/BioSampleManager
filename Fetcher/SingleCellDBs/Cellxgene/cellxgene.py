import requests

from Fetcher.SingleCellDBs import SingleCellDBFetcher
from utils.DBS.json_file import JsonManager


class Cellxgene(SingleCellDBFetcher):
    def __init__(self, domain_name="cellxgene.cziscience.com", datasets_path="/curation/v1/datasets"):
        super().__init__()
        self.domain_name = domain_name
        self.api_url_base = f"https://api.{domain_name}"
        self.datasets_url = f"{self.api_url_base}{datasets_path}"
        self.headers = {"Content-Type": "application/json"}

    def fetch(self, db_name):
        res = requests.get(url=self.datasets_url, headers=self.headers)
        res.raise_for_status()
        data = res.json()

        manager = JsonManager(db_name)
        manager.save(data)
        self.logger.info("Data saved successfully to JSON file.")
