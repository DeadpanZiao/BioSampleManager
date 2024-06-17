import requests
from Fetcher.SingleCellDBs import SingleCellDBFetcher
from utils.DBS.json_file import JsonManager


class SingleCellPortal(SingleCellDBFetcher):
    def __init__(self, domain_name="singlecell.broadinstitute.org", datasets_path="/site/studies"):
        super().__init__()
        self.domain_name = domain_name
        self.api_url_base = f"https://{domain_name}/single_cell/api/v1"
        self.datasets_url = f"{self.api_url_base}{datasets_path}"
        self.headers = {"Content-Type": "application/json"}

    def fetch(self):
        response = requests.get(self.datasets_url, headers=self.headers, verify=False)
        if response.status_code == 200:
            studies = response.json()
            manager = JsonManager('studies')
            manager.save(studies)
            self.logger.info("Data saved successfully to JSON file.")

            for study in studies:
                accessions = study.get('accession', 'N/A')
                study_url = f"{self.datasets_url}/{accessions}"
                response = requests.get(study_url, headers=self.headers, verify=False)
                if response.status_code == 200:
                    study_data = response.json()
                    manager = JsonManager(f'{accessions}')
                    manager.save(study_data)
                    self.logger.info(f"Data saved successfully to {accessions}.json file.")
                else:
                    self.logger.error(f"Failed to retrieve study {accessions}. Status code: {response.status_code}")
        else:
            self.logger.error(f"Failed to retrieve studies. Status code: {response.status_code}")