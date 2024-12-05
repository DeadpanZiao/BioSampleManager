import logging
import os
import json
import sqlite3

import requests
from pathlib import Path
from tqdm import tqdm
from urllib.parse import urlparse
from ftplib import FTP

logging.basicConfig(level=logging.INFO)


class BaseDownloader:
    """基础下载器类，包含公共下载功能"""

    def __init__(self, download_dir):
        """初始化下载目录"""
        self.download_dir = download_dir

    def _save_path(self, url):
        """根据URL获取保存文件的路径"""
        local_filename = os.path.basename(urlparse(url).path)
        save_path = os.path.join(self.download_dir, local_filename)
        return save_path

    def _create_directory(self, path):
        """创建目录（如果不存在）"""
        if not os.path.exists(path):
            os.makedirs(path)

    def _download_with_progress(self, url, file_path):
        """下载文件并显示进度条"""
        try:
            if url.startswith('https'):
                with requests.get(url, stream=True) as r:
                    r.raise_for_status()
                    total_size_in_bytes = int(r.headers.get('content-length', 0))
                    block_size = 1024  # 1KB

                    with open(file_path, 'wb') as f:
                        with tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True,
                                  desc=os.path.basename(file_path)) as pbar:
                            for data in r.iter_content(block_size):
                                f.write(data)
                                pbar.update(len(data))
            elif url.startswith('ftp'):
                def callback(block_num, block_size, total_size):
                    progress_bytes = block_num * block_size
                    tqdm.write(f"\r下载进度: {progress_bytes / total_size * 100:.2f}%", end='')

                with FTP(urlparse(url).hostname) as ftp:
                    ftp.login()  # 默认使用匿名登录，如有需要可添加用户名和密码
                    file_parts = urlparse(url).path.split('/')
                    remote_filename = file_parts[-1]
                    total_size = ftp.size(remote_filename)

                    with open(file_path, 'wb') as f:
                        ftp.retrbinary(f'RETR {remote_filename}', f.write,
                                       callback=lambda block_num, block_size: callback(block_num, block_size,
                                                                                       total_size))

                    tqdm.write('\n')  # 换行
            else:
                raise ValueError("不支持的协议: " + url)
        except Exception as e:
            print(f"下载文件 {url} 时出错: {e}")

# HCADownloader类继承BaseDownloader
class HCADownloader(BaseDownloader):
    """特定于HCA项目的下载器类，继承自BaseDownloader"""

    def __init__(self, db_path, download_dir):
        """初始化数据库路径和下载目录"""
        super().__init__(download_dir)
        self.db_path = db_path

    def fetch_download_links(self):
        """从数据库获取下载链接"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT internal_id, download_links FROM Sample")
        rows = cursor.fetchall()
        download_tasks = []

        for row in rows:
            download_links_str = row[1]
            internal_id = str(row[0])
            if download_links_str is not None:
                download_links_list = json.loads(download_links_str)
                for link in download_links_list:
                    if "service." in link or "ftp." in link:
                        link = link.strip()
                        download_tasks.append((internal_id, link))

        cursor.close()
        conn.close()
        return download_tasks

    def download_files(self):
        """下载文件"""
        tuples_list = self.fetch_download_links()
        for id, link in tuples_list:
            save_dir = os.path.join(self.download_dir, id)
            self._create_directory(save_dir)
            file_path = self._save_path(link)
            if not os.path.exists(save_dir):
                self._download_with_progress(link, file_path)
            else:
                print(f"文件 {file_path} 已存在，跳过下载。")