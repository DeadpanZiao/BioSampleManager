from GEOparse import GEOparse
import time
import GEOparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import glob
import requests
from bs4 import BeautifulSoup
import os
from pysradb.sraweb import SRAweb

from BSM.Fetcher.SingleCellDBs.fetchers import SingleCellDBFetcher
from BSM.Fetcher.utils import JsonManager


class DownloadSoft(SingleCellDBFetcher):
    def __init__(self, soft_filepath, max_workers=1):
        super().__init__()
        self.soft_filepath = soft_filepath
        self.max_workers = max_workers

    def get_soup(self, current_url):
        response = requests.get(current_url)
        url_content = response.text
        soup = BeautifulSoup(url_content, 'html.parser')
        return response, soup, url_content

    def extract_remaining_gse_ids(self, gse_ids_saved, gse_ids_succeed):
        set_saved = set(gse_ids_saved)
        set_succeed = set(gse_ids_succeed)
        remaining_gse_ids = list(set_saved - set_succeed)

        return remaining_gse_ids

    def download_and_save_gse(self, gse_id):
        try:
            self.logger.info(f'downloading gse id: {gse_id}')
            gse = GEOparse.get_GEO(geo=gse_id, destdir=self.soft_filepath)
            print(f"Successfully downloaded {gse_id}")
            time.sleep(2)

        except Exception as e:
            print(f"Failed to download {gse_id}: {e}")

    def get_gse_id_saved(self):
        gse_ids_saved = []
        gse_id_file = os.path.join(self.soft_filepath, 'gse_ids.txt')
        if not os.path.exists(gse_id_file):
            return []
        with open(gse_id_file, 'r') as filehandle:
            for line in filehandle:
                current_item = line.strip()
                gse_ids_saved.append(current_item)
        return gse_ids_saved

    def get_ids_failed_dl(self):
        gse_ids_saved = self.get_gse_id_saved()

        gse_ids_succeed = []
        gse_ids_failed = []
        pattern_soft = f'{self.soft_filepath}*_family.soft.gz'
        files_soft = glob.glob(pattern_soft)

        for file in files_soft:
            file = os.path.basename(file)
            parts = file.split('_')
            if len(parts) >= 2 and parts[0].startswith('GSE'):
                gse_ids_succeed.append(parts[0])

        gse_ids_failed = self.extract_remaining_gse_ids(gse_ids_saved, gse_ids_succeed)
        return gse_ids_succeed, gse_ids_failed

    def get_gse_id_new(self):
        gse_ids_new = []
        url = 'https://www.ncbi.nlm.nih.gov/'
        DatabaseName = 'geo' + '/browse/?view='
        view = 'series' + '&display='
        display = 500
        str_med = '&page='
        page_initial = 1

        url_initial = url + DatabaseName + view + str(display) + str_med + str(page_initial)
        url_result = self.get_soup(url_initial)
        response_initial = url_result[0]
        soup_initial = url_result[1]
        content_initial = url_result[2]

        for n in range(50):
            if response_initial.status_code == 200:
                break
            elif n == 49:
                print(f"Initial page request failed, status code: {response_initial.status_code}")
            else:
                response_initial = requests.get(url_initial)

        page_cnt = soup_initial.find(id='page_cnt')
        page_cnt = int(page_cnt.text)

        for i in range(page_cnt):
            page = i + 1
            url_current = url + DatabaseName + view + str(display) + str_med + str(page)
            url_current_result = self.get_soup(url_current)
            response_current = url_current_result[0]

            for n in range(50):
                if response_current.status_code == 200:
                    break
                elif n == 49:
                    print(f"Current page request failed, status code: {response_current.status_code}")
                else:
                    url_current_result = self.get_soup(url_current)
                    response_current = url_current_result[0]

            soup_current = url_current_result[1]
            content_current = url_current_result[2]

            geo_table = soup_current.find(id='geo_data')
            geo_table_tbody = geo_table.find('tbody')
            geo_table_tbody_tr = geo_table_tbody.find_all('tr')
            len_tr = len(geo_table_tbody_tr)
            for i2 in range(len_tr):
                geo_table_tbody_tr_td = geo_table_tbody_tr[i2].find('td')
                geo_id_text = geo_table_tbody_tr_td.text
                geo_id_text = geo_id_text.strip('\n')
                gse_ids_saved = self.get_gse_id_saved()
                if len(gse_ids_saved)>0 and geo_id_text == gse_ids_saved[0]:
                    found = True
                    break
                else:
                    gse_ids_new.append(geo_id_text)
                    found = False
            if found:
                break
        return gse_ids_new

    def download_soft_failed_dl(self):
        gse_ids_failed_dl = self.get_ids_failed_dl()[1]
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self.download_and_save_gse, gse_id) for gse_id in gse_ids_failed_dl}

            for future in tqdm(as_completed(futures), total=len(gse_ids_failed_dl)):
                pass

        print("All soft files that failed to download before downloads completed or attempted.")

    def download_soft_new(self):
        gse_ids_new = self.get_gse_id_new()
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self.download_and_save_gse, gse_id) for gse_id in gse_ids_new}

            for future in tqdm(as_completed(futures), total=len(gse_ids_new)):
                pass

        print("All new ids downloads completed or attempted.")

    def save_as_gse_ids_file_new(self):
        gse_ids_saved = self.get_gse_id_saved()
        gse_ids_new = self.get_gse_id_new()

        new_gse_ids_all = gse_ids_new + gse_ids_saved
        filename = f"gse_ids.txt"
        with open(filename, 'w') as file:
            for data in new_gse_ids_all:
                file.write(f"{data}\n")

            print(f"Data saved to new txt file: {filename}")


class GeoFetcher(SingleCellDBFetcher):
    def __init__(self):

        super().__init__()
        # self.soft_filepath = soft_filepath

    def fetch_all_soft_file(self, soft_filepath):
        downloader = DownloadSoft(soft_filepath)
        downloader.download_soft_new()

    def get_gse_metadata(self, file):
        gse = GEOparse.get_GEO(filepath=file)
        gse_metadata = gse.metadata
        gse_id = gse_metadata['geo_accession'][0]
        db = SRAweb()
        try:
            df = db.gse_to_srp(gse_id, detailed=True)
            # print(df)
            srp_id = df.iloc[0, 1]
            dr = db.srp_to_srr(srp_id)
            srr_ids = dr.iloc[:, 1]
            srr_ids_value = srr_ids.values
            fastq_links = []
            current_fastq_links = [f"https://trace.ncbi.nlm.nih.gov/Traces/sra-reads-be/fastq?acc={srr_id}" for srr_id
                                   in
                                   srr_ids_value]
            # current_fastq_links = self.generate_fastq_links(srr_ids)
            fastq_links.extend(current_fastq_links)
        except Exception as e:
            # 处理其他所有异常
            print(gse_id, f"An unexpected error occurred: {e}")
            fastq_links = []

        # a=srr_ids_value[0]
        # srr_ids_list = srr_ids.tolist

        # new meta-sample
        samples = {}
        # 从gse_metadata中提取sample_id
        sample_ids = gse_metadata.get('sample_id', [])
        # 移除gse_metadata中的'sample_id'键
        if 'sample_id' in gse_metadata:
            gse_metadata.pop('sample_id')
        # 获取gse中的samples的metadata
        gse_gsm = gse.gsms
        # save_as_json(gse_gsm, 'GSE_gsms.json')
        gse_gsm_len = len(gse_gsm)
        gsm_meta = []
        for gsm in gse_gsm.items():
            gsm_id = gsm[0]
            gsm_info = gsm[1]
            current_gsm_meta = gsm_info.metadata
            gsm_meta.append(current_gsm_meta)
        # 将additional_data直接赋值给sample_metadata
        samples['sample_id'] = sample_ids
        samples['sample_metadata'] = gsm_meta

        # 将samples合并到series中
        gse_metadata['samples'] = samples
        # save_as_json(gse_metadata, 'GSE_meta.json')

        gse_metadata["fastq_links"] = fastq_links
        return gse_metadata

    def get_all_soft_file(self, soft_filepath):
        pattern = soft_filepath + '*_family.soft.gz'
        matching_files = glob.glob(pattern)

        if not matching_files:
            return "No soft files found."

        return matching_files

    def check_json_file(self, json_name):
        matching_files = glob.glob(json_name)
        if not matching_files:
            print(f"{json_name} not found.")
            return True
        else:
            print(f"{json_name} found.")
            return False

    def fetch_gse_meta(self, db_folder):
        soft_file = self.get_all_soft_file(db_folder)
        loop_time = []
        for file in soft_file:
            json_name = file.split('/')[-1].split('\\')[-1].split('_')[0] + '_metadata.json'
            json_name = f'{db_folder}{json_name}'
            if self.check_json_file(json_name):
                print(f"{json_name} is downloading.")
                start_time = time.time()  # 记录开始时间
                current_gse = self.get_gse_metadata(file)
                manager = JsonManager(json_name)
                manager.save(current_gse)
                self.logger.info(f"{file} metadata saved successfully to JSON file.")
                # current_gse_id = current_gse['geo_accession'][0]
                # all_gse[current_gse_id] = current_gse
                end_time = time.time()  # 记录结束时间
                single_loop_time = end_time - start_time  # 计算本次循环耗时
                loop_time.append(single_loop_time)

    def fetch(self, db_name):
        self.logger.info('Direct GEO fetch is challenging and is highly likely to fail due to too huge data.'
                         'Use fetch_all_soft_file() and fetch_gse_meta() instead')


