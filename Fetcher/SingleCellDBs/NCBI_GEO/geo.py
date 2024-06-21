from GEOparse import GEOparse
import glob
import time

from Fetcher.SingleCellDBs import SingleCellDBFetcher
from utils.DBS.json_file import JsonManager


class Geo(SingleCellDBFetcher):
    def __init__(self, soft_filepath):  # soft_filepath='D:/3.3-zjprogram/GEO/':

        super().__init__()
        self.soft_filepath = soft_filepath

    def get_gse_metadata(self, file):
        # gse = GEOparse.get_GEO(geo="GSE178333", destdir="C:/GEO/") # 在线下载
        # GEOparse.GEOparse.parse_GSE("./GSE267215_family.soft.gz")
        gse = GEOparse.get_GEO(filepath=file)
        gse_metadata = gse.metadata

        # 新建一个字典-sample
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
        '''
        gsms（samples）的columns与table没有获取
        '''
        return gse_metadata

    def get_all_soft_file(self):
        pattern = self.soft_filepath + '*_family.soft.gz'
        matching_files = glob.glob(pattern)

        if not matching_files:
            return "No soft files found."

        return matching_files

    def merge_geo_cojson(self):
        soft_file = self.get_all_soft_file()
        all_gse = {}
        loop_time = []
        for file in soft_file:
            start_time = time.time()  # 记录开始时间
            current_gse = self.get_gse_metadata(file)
            current_gse_id = current_gse['geo_accession'][0]
            all_gse[current_gse_id] = current_gse
            end_time = time.time()  # 记录结束时间
            single_loop_time = end_time - start_time  # 计算本次循环耗时
            loop_time.append(single_loop_time)
        return all_gse, loop_time

    def fetch(self, db_name, time_db):
        data, fetch_time = self.merge_geo_cojson()
        # data = data_and_time[0]
        manager = JsonManager(db_name)
        manager.save(data)
        self.logger.info("Data saved successfully to JSON file.")
        # fetch_time = data_and_time[1]
        manager_time = JsonManager(time_db)
        manager_time.save(fetch_time)
        # filename = './GEO_metadata_all.json'
        # save_as_json(all_gse, filename)
        # filename_time = './saveas_json_time.json'
        # save_as_json(loop_time, filename_time)
