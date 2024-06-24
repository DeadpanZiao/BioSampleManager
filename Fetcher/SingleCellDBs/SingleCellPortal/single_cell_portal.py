import requests

from Fetcher.SingleCellDBs import SingleCellDBFetcher
from utils.DBS.json_file import JsonManager



class SingleCellPortalFetcher(SingleCellDBFetcher):
    def __init__(self, domain_name="singlecell.broadinstitute.org", datasets_path="/site/studies"):
        super().__init__()
        self.domain_name = domain_name
        self.api_url_base = f"https://{domain_name}/single_cell/api/v1"
        self.datasets_url = f"{self.api_url_base}{datasets_path}"
        self.headers = {"Content-Type": "application/json"}

    def fetch(self, db_name):
        response = requests.get(self.datasets_url, headers=self.headers, verify=False)
        if response.status_code == 200:
            studies = response.json()
            self.logger.info("Data saved successfully to JSON file.")
            merged_data = []
            for study in studies:
                accessions = study.get('accession', 'N/A')
                study_url = f"{self.datasets_url}/{accessions}"
                response = requests.get(study_url, headers=self.headers, verify=False,timeout=120)
                if response.status_code == 200:
                    study_data = response.json()
                    merged_data.append(study_data)
                    self.logger.info(f"Data saved successfully to {accessions}.json file.")
                else:
                    self.logger.error(f"Failed to retrieve study {accessions}. Status code: {response.status_code}")
            manager = JsonManager(db_name)
            manager.save(merged_data)
        else:
            self.logger.error(f"Failed to retrieve studies. Status code: {response.status_code}")

fecter = SingleCellPortalFetcher()
fecter.fetch('studies')