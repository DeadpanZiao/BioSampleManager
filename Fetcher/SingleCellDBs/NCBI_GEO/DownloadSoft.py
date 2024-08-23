import time
import GEOparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm  # 进度条库，可选，用于可视化下载进度
import glob
import requests
from bs4 import BeautifulSoup
import os

from Fetcher.SingleCellDBs import SingleCellDBFetcher

'''
思路：
    1、爬最新gse的ids
    之前爬的gse ids已存为gse_ids.txt文件
    函数：save_as_gse_ids_file_new
    2、下载soft文件
    分两个函数：
    “download_soft_failed_dl”（重新下载之前为下载成功的文件）
    “download_soft_new”（下载最新爬的ids）
    3、运行/测试
    可以随时运行tests/test-fetcher/test_download_softfile.py爬最新的数据覆盖之前的gse_ids.txt文件
'''


class DownloadSoft(SingleCellDBFetcher):
    def __init__(self, soft_filepath, max_workers=8):  # soft_filepath='D:/3.3-zjprogram/GEO/':

        super().__init__()
        self.soft_filepath = soft_filepath
        self.max_workers = max_workers

    def get_soup(self, current_url):
        response = requests.get(current_url)  # , proxies=proxies
        url_content = response.text  # 网页内容
        # 使用 BeautifulSoup 解析网页内容
        soup = BeautifulSoup(url_content, 'html.parser')
        return response, soup, url_content

    def extract_remaining_gse_ids(self, gse_ids_saved, gse_ids_succeed):
        # 将两个列表转换为集合
        set_saved = set(gse_ids_saved)
        set_succeed = set(gse_ids_succeed)

        # 计算差集
        remaining_gse_ids = list(set_saved - set_succeed)

        return remaining_gse_ids

    def download_and_save_gse(self, gse_id):
        try:
            gse = GEOparse.get_GEO(geo=gse_id, destdir=self.soft_filepath)
            # geoID_SuccssDL.append(gse_id)
            print(f"Successfully downloaded {gse_id}")
            # 防止请求太频繁
            time.sleep(2)
            # save_geo_data(geoID_SuccssDL, 'GeoID_SuccessLD')

        except Exception as e:
            # geoID_FailedDL.append(gse_id)
            print(f"Failed to download {gse_id}: {e}")
            # save_geo_data(geoID_FailedDL, 'GeoID_FailedLD')

    def get_gse_id_saved(self):
        gse_ids_saved = []
        # 先读取上一次保存的series的ID
        gse_id_file = f'{self.soft_filepath}/gse_ids.txt'
        with open(gse_id_file, 'r') as filehandle:
            for line in filehandle:
                # 去除每行末尾的换行符
                current_item = line.strip()
                gse_ids_saved.append(current_item)
        return gse_ids_saved

    def get_ids_failed_dl(self):
        # 获取已保存的gse_ids
        gse_ids_saved = self.get_gse_id_saved()

        gse_ids_succeed = []
        gse_ids_failed = []
        # 设定查找模式，查找所有*.soft.gz文件
        pattern_soft = f'{self.soft_filepath}*_family.soft.gz'
        files_soft = glob.glob(pattern_soft)

        # # 使用join()方法将列表元素合并成一个字符串，这里使用空字符串作为分隔符
        # big_soft_string = ' '.join(files_soft)
        for file in files_soft:
            file = os.path.basename(file)
            # 分割文件名
            parts = file.split('_')
            if len(parts) >= 2 and parts[0].startswith('GSE'):
                gse_ids_succeed.append(parts[0])

        gse_ids_failed = self.extract_remaining_gse_ids(gse_ids_saved, gse_ids_succeed)
        return gse_ids_succeed, gse_ids_failed

    def get_gse_id_new(self):
        gse_ids_new = []
        url = 'https://www.ncbi.nlm.nih.gov/'
        DatabaseName = 'geo' + '/browse/?view='
        view = 'series' + '&display='  # view=series\samples\datset\
        display = 500
        str_med = '&page='
        page_initial = 1

        # 利用初始链接获取总页数
        url_initial = url + DatabaseName + view + str(display) + str_med + str(page_initial)
        url_result = self.get_soup(url_initial)
        response_initial = url_result[0]
        soup_initial = url_result[1]
        content_initial = url_result[2]

        # 检查请求是否成功（HTTP状态码为200）
        for n in range(50):
            # sleep_seconds(edge_low, edge_high)
            if response_initial.status_code == 200:
                break
            elif n == 49:
                print(f"拟立项标准总页数url链接50次请求失败，状态码：{response_initial.status_code}")
            else:
                response_initial = requests.get(url_initial)

        page_cnt = soup_initial.find(id='page_cnt')
        page_cnt = int(page_cnt.text)
        # 获取表格Series数据
        for i in range(page_cnt):
            page = i + 1
            url_current = url + DatabaseName + view + str(display) + str_med + str(page)
            url_current_result = self.get_soup(url_current)
            response_current = url_current_result[0]

            # 检查请求是否成功（HTTP状态码为200）
            for n in range(50):
                # sleep_seconds(edge_low, edge_high)
                if response_current.status_code == 200:
                    break
                elif n == 49:
                    print(f"拟立项标准总页数url链接50次请求失败，状态码：{response_current.status_code}")
                else:
                    url_current_result = self.get_soup(url_current)
                    response_current = url_current_result[0]

            soup_current = url_current_result[1]
            content_current = url_current_result[2]

            geo_table = soup_current.find(id='geo_data')
            # geo_table_thead = geo_table.find(id_='thead')
            geo_table_tbody = geo_table.find('tbody')
            geo_table_tbody_tr = geo_table_tbody.find_all('tr')
            len_tr = len(geo_table_tbody_tr)
            for i2 in range(len_tr):
                geo_table_tbody_tr_td = geo_table_tbody_tr[i2].find('td')
                geo_id_text = geo_table_tbody_tr_td.text
                geo_id_text = geo_id_text.strip('\n')
                # 获取已保存的gse_ids
                gse_ids_saved = self.get_gse_id_saved()
                if geo_id_text == gse_ids_saved[0]:
                    found = True
                    break
                else:
                    gse_ids_new.append(geo_id_text)
                    found = False
            if found:
                break
        return gse_ids_new

    def download_soft_failed_dl(self):
        # 获取已保存的gse_ids
        gse_ids_failed_dl = self.get_ids_failed_dl()[1]
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:  # 根据实际情况调整最大工作线程数
            futures = {executor.submit(self.download_and_save_gse, gse_id) for gse_id in gse_ids_failed_dl}

            # 可选：使用tqdm显示下载进度
            for future in tqdm(as_completed(futures), total=len(gse_ids_failed_dl)):
                # 这里可以直接yield from as_completed(futures)，但为了演示加入进度条，使用循环
                pass  # 实际上不需要在这里做处理，as_completed已经异步完成了任务

        print("All soft files that failed to download before downloads completed or attempted.")

    def download_soft_new(self):
        # 获取新爬的gse_ids
        gse_ids_new = self.get_gse_id_new()
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:  # 根据实际情况调整最大工作线程数
            futures = {executor.submit(self.download_and_save_gse, gse_id) for gse_id in gse_ids_new}

            # 可选：使用tqdm显示下载进度
            for future in tqdm(as_completed(futures), total=len(gse_ids_new)):
                # 这里可以直接yield from as_completed(futures)，但为了演示加入进度条，使用循环
                pass  # 实际上不需要在这里做处理，as_completed已经异步完成了任务

        print("All new ids downloads completed or attempted.")

    def save_as_gse_ids_file_new(self):

        # 获取已保存的gse_ids
        gse_ids_saved = self.get_gse_id_saved()

        # 获取新爬的gse_ids
        gse_ids_new = self.get_gse_id_new()

        new_gse_ids_all = gse_ids_new + gse_ids_saved
        filename = f"gse_ids.txt"
        # 将列表内容写入文件
        with open(filename, 'w') as file:
            for data in new_gse_ids_all:
                file.write(f"{data}\n")  # 假设每条数据占一行，以换行符分隔

            print(f"数据已保存至新txt文件：{filename}")

