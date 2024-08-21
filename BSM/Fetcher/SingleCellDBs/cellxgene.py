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
        datasets = self.fetch_dataset()

        merged_datasets = []
        for collection in collections:
            collection_datasets = collection.get('datasets', [])
            for dataset in collection_datasets:
                dataset_id = dataset.get('dataset_id')
                matching_dataset = next((d for d in datasets if d.get('dataset_id') == dataset_id), None)
                if matching_dataset:
                    dataset.update(matching_dataset)
            merged_datasets.append(collection)

        json_manager = JsonManager(db_name)
        json_manager.save(merged_datasets)
        self.logger.info("Data saved successfully to JSON file.")

