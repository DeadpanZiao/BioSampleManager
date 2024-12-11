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
    """Base class for downloader, containing common download functionalities"""

    def __init__(self, download_dir):
        """Initialize the download directory"""
        self.download_dir = download_dir

    def _save_path(self, url, save_dir=None):
        """Get the path to save the file based on URL"""
        local_filename = os.path.basename(urlparse(url).path)
        if save_dir is not None:
            return os.path.join(save_dir, local_filename)
        else:
            return os.path.join(self.download_dir, local_filename)

    def _create_directory(self, path):
        """Create directory if it does not exist"""
        if not os.path.exists(path):
            os.makedirs(path)

    def _download_with_progress(self, url, file_path):
        """Download the file with a progress bar"""
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
                    tqdm.write(f"\rDownload progress: {progress_bytes / total_size * 100:.2f}%", end='')

                with FTP(urlparse(url).hostname) as ftp:
                    ftp.login()  # Anonymous login by default, can add username and password if necessary
                    file_parts = urlparse(url).path.split('/')
                    remote_filename = file_parts[-1]
                    total_size = ftp.size(remote_filename)

                    with open(file_path, 'wb') as f:
                        ftp.retrbinary(f'RETR {remote_filename}', f.write,
                                       callback=lambda block_num, block_size: callback(block_num, block_size,
                                                                                       total_size))

                    tqdm.write('\n')  # New line
            else:
                raise ValueError("Unsupported protocol: " + url)
        except Exception as e:
            logging.info(f"Error downloading file {url}: {e}")


class HCADownloader(BaseDownloader):
    """Downloader class specific to the HCA project, inheriting from BaseDownloader"""

    def __init__(self, db_path, download_dir):
        """Initialize database path and download directory"""
        super().__init__(download_dir)
        self.db_path = db_path

    def fetch_download_links(self):
        """Fetch download links from the database"""
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
        tuples_list = self.fetch_download_links()
        for id, link in tuples_list:
            save_dir = os.path.join(self.download_dir, id)
            self._create_directory(save_dir)
            file_path = self._save_path(link, save_dir)  # 使用新的 save_dir 参数
            if not os.path.exists(file_path):  # Check if the specific file path exists
                self._download_with_progress(link, file_path)
            else:
                logging.info(f"File {file_path} already exists, skipping download.")