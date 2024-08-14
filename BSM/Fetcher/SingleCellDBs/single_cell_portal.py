import requests
from BSM.Fetcher.SingleCellDBs.fetchers import SingleCellDBFetcher
from BSM.Fetcher.utils import JsonManager

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
            manager = JsonManager('data')#修改
            manager.save(studies)
            self.logger.info("Data saved successfully to JSON file.")
            # merged_data = {}#删除
            final_data = []#增加
            for study in studies:
                accessions = study.get('accession', 'N/A')
                study_url = f"{self.datasets_url}/{accessions}"
                response = requests.get(study_url, headers=self.headers, verify=False)
                if response.status_code == 200:
                    study_data = response.json()
                    # merged_data.update(study_data)#删除
                    final_data.append(study_data)#增加
                    self.logger.info(f"Data saved successfully to {accessions}.json file.")
                else:
                    self.logger.error(f"Failed to retrieve study {accessions}. Status code: {response.status_code}")
            manager = JsonManager(db_name)#修改
            manager.save(final_data)
        else:
            self.logger.error(f"Failed to retrieve studies. Status code: {response.status_code}")

# f = SingleCellPortalFetcher()
# f.fetch(r'C:\Users\wxj01\Desktop\cell\scp.json')
