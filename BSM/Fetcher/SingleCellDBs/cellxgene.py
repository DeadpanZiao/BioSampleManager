import json
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

    def fetch_dataset(self):
        res = requests.get(url=self.datasets_url, headers=self.headers)
        res.raise_for_status()
        data = res.json()
        return data

    def fetch_collections(self):
        res = requests.get(url=self.collections_url, headers=self.headers)
        res.raise_for_status()
        data = res.json()
        return data

    def fetch(self, db_name):
        collections = self.fetch_collections()
        dataset = self.fetch_dataset()

        data1 = collections
        data2 = dataset

        merged_data = []
        for obj in data1:
            datasets = obj.get('datasets', [])
            for dataset in datasets:
                dataset_id1 = dataset.get('dataset_id')
                for i in data2:
                    dataset_id2 = i.get('dataset_id')
                    if dataset_id1 == dataset_id2:
                        dataset.update(i)
                        merged_data.append(obj)

        manager = JsonManager(db_name)
        manager.save(merged_data)
        self.logger.info("Data saved successfully to JSON file.")

