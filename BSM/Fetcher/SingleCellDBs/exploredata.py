import requests
from tqdm import tqdm
import json
import urllib.parse
from collections import defaultdict
import os

from BSM.Fetcher.SingleCellDBs.fetchers import SingleCellDBFetcher
from BSM.Fetcher.utils import JsonManager


class ExploreDataFetcher(SingleCellDBFetcher):
    def __init__(self, project_url=r'https://service.azul.data.humancellatlas.org/index/projects?size=100&catalog=dcp40&order=asc&sort=projectTitle&filters=%7B%7D',
                 files_url=r'https://service.azul.data.humancellatlas.org/index/files'):
        super().__init__()
        self.project_meta_data = []
        self.project_meta_data_with_url = []
        self.project_url = project_url
        self.files_url = files_url
        self.headers = {'Accept': 'application/json, text/plain, */*'}

    def fetch(self, file_name):

        self.fetch_project()
        manager = JsonManager(file_name)

        if os.path.exists(file_name):
            self.project_meta_data_with_url = manager.load_by_line()
            exist_entryId = {item['entryId'] for item in self.project_meta_data_with_url}
            fetched_entryId = {item['entryId'] for item in self.project_meta_data}
            missing_entryId = fetched_entryId - exist_entryId
            if not missing_entryId:
                self.logger.info("No New Projects!")
                return
            projects = [item for item in self.project_meta_data if item['entryId'] in missing_entryId]
            self.logger.info(f"find {len(projects)} New Projects! Fetching ...")
            # print(projects)
        else:
            projects = self.project_meta_data
        self.fetch_url(projects)
        manager.save_by_lines(self.project_meta_data_with_url)
        # with open(file_name, 'w', encoding='utf-8') as f:
        #     for item in tqdm(self.project_meta_data_with_url, desc='Saving JSON'):
        #         json.dump(item, f, ensure_ascii=False)
        #         f.write('\n')
        self.logger.info("Data saved successfully to JSON file.")

    def fetch_project(self):
        url = self.project_url

        response = requests.get(url)
        response.raise_for_status()  # 如果请求失败，抛出异常
        # 解析JSON数据
        data = response.json()
        total = data['pagination']['total']
        with tqdm(total=total, desc='Fetching Project Data', initial=data['pagination']['count']) as pbar:
            while url:
                # 提取“hits”字段
                hits = data.get('hits', [])
                self.project_meta_data.extend(hits)
                # 获取下一页的URL
                pagination = data.get('pagination', {})
                url = pagination.get('next', None)
                if url:
                    response = requests.get(url)
                    response.raise_for_status()  # 如果请求失败，抛出异常
                    data = response.json()
                    pbar.update(data['pagination']['count'])

    def fetch_url(self, projects):
        for project in tqdm(projects, desc='Processing Project'):
            entry_id = project.get('entryId')
            params = {
                "catalog": "dcp40",
                "filters": json.dumps({"projectId": {"is": [entry_id]}}),
                "size": 1000
            }
            url = f"{self.files_url}?{urllib.parse.urlencode(params)}"
            while True:
                try:
                    response = requests.get(url)
                    if response.status_code == 200:
                        break
                except Exception as e:
                    tqdm.write(str(e))
            data = response.json()
            total = data['pagination']['total']
            aggregated_data = defaultdict(list)
            with tqdm(total=total, desc='Fetching Project URLs', initial=data['pagination']['count']) as pbar:
                while url:

                    # 提取“hits”字段
                    hits = data.get('hits', [])
                    # 处理每个hit的files字段
                    for hit in hits:
                        files = hit.get('files', [])
                        for file in files:
                            file_format = file.get('format')
                            file_url = file.get('url')
                            if file_format and file_url:
                                file.pop('format')
                                aggregated_data[file_format].append(file)

                    # 获取下一页的URL
                    pagination = data.get('pagination', {})
                    url = pagination.get('next', None)
                    if url:
                        while True:
                            try:
                                response = requests.get(url)
                                if response.status_code == 200:
                                    break
                            except Exception as e:
                                tqdm.write(str(e))
                        data = response.json()
                        pbar.update(data['pagination']['count'])
            project['files'] = aggregated_data
            self.project_meta_data_with_url.append(project)

f = ExploreDataFetcher()
f.fetch('hca.json')