from GEOparse import GEOparse
import glob
import time

from Fetcher.SingleCellDBs import SingleCellDBFetcher
from utils.DBS.json_file import JsonManager
from pysradb.sraweb import SRAweb

'''
思路：
    1、方法
    元数据获取通过库“GEOparse”
    调用：给class GeoFetcher 传递“soft_filepath”（soft文件路径）参数，
         然后调用fetch函数，先读取当前目录中所有soft文件，然后依次获取soft的metadata，
         首先判断当前目录是否已有该soft文件的json文件，若无，则的元数据信息存为单个json文件
    2、运行/测试
    运行tests/test-fetcher/test_geo.py
'''


class GeoFetcher(SingleCellDBFetcher):
    def __init__(self, soft_filepath):  # soft_filepath='D:/3.3-zjprogram/GEO/':

        super().__init__()
        self.soft_filepath = soft_filepath

    def get_gse_metadata(self, file):
        # gse = GEOparse.get_GEO(geo="GSE178333", destdir="C:/GEO/")
        # GEOparse.GEOparse.parse_GSE("./GSE267215_family.soft.gz")
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

    def get_all_soft_file(self):
        pattern = self.soft_filepath + '*_family.soft.gz'
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

    def fetch(self, json_path):
        soft_file = self.get_all_soft_file()
        loop_time = []
        for file in soft_file:
            json_name = file.split('/')[-1].split('\\')[-1].split('_')[0] + '_metadata.json'
            json_name = f'{json_path}{json_name}'
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

