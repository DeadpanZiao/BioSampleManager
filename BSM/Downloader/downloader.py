import asyncio
import logging
import os
import json
import sqlite3

import aiofiles
import aiohttp
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
            file_path = self._save_path(link, save_dir)
            if not os.path.exists(file_path):
                self._download_with_progress(link, file_path)
            else:
                logging.info(f"File {file_path} already exists, skipping download.")

    async def _get_response_headers(self, session, url):
        async with session.head(url, allow_redirects=True) as response:
            return response.headers

    async def _check_file_exists(self, file_path):
        return Path(file_path).exists()

    async def _async_download_file(self, session, url, save_dir, semaphore, overall_progress=None):
        async with semaphore:  # 控制并发量
            try:
                if url.startswith('ftp://'):
                    pass  # FTP链接的处理需要额外的库或方法，这里暂时不做处理
                else:
                    # 获取重定向后的URL
                    headers = await self._get_response_headers(session, url)
                    url = headers.get('Location', url)

                async with session.get(url) as response:
                    if response.status == 200:
                        content_disposition = response.headers.get('Content-Disposition', '')
                        file_name = content_disposition.split('filename=')[-1].strip('"') or \
                                    url.split('/')[-1].split('?')[0]
                        file_path = os.path.join(save_dir, file_name)

                        total_size = int(response.headers.get('Content-Length', 0))

                        # 检查本地文件是否存在且大小是否正确
                        if Path(file_path).exists():
                            local_file_size = Path(file_path).stat().st_size
                            if local_file_size == total_size:
                                logging.info(f"File {file_name} already exists and is correct size, skipping.")
                                if overall_progress:
                                    overall_progress.update(1)
                                return
                            else:
                                logging.warning(f"File {file_name} exists but size does not match, re-downloading.")
                                os.remove(file_path)  # 移除不完整的文件

                        wrote = 0

                        async with aiofiles.open(file_path, 'wb') as f:
                            with tqdm(total=total_size, unit='B', unit_scale=True, desc=file_name, leave=False) as pbar:
                                async for chunk in response.content.iter_chunked(1024 * 1024):  # 每次写入1MB 避免写入缓存
                                    await f.write(chunk)
                                    wrote += len(chunk)
                                    pbar.update(len(chunk))

                        if total_size != 0 and wrote != total_size:
                            logging.error(f"ERROR, something went wrong downloading {file_name}")
                            if Path(file_path).exists():
                                os.remove(file_path)  # 下载失败移除部分下载的文件
                    else:
                        logging.error(f"Failed to download {url}. Status code: {response.status}")
                    if overall_progress:
                        overall_progress.update(1)
            except Exception as e:
                logging.error(f"Error downloading {url}: {e}")
                if overall_progress:
                    overall_progress.update(1)

    async def async_download_files(self, workers=5):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT internal_id, download_links FROM Sample WHERE download_links IS NOT NULL")
        links = cursor.fetchall()
        conn.close()

        semaphore = asyncio.Semaphore(workers)
        tasks = []
        total_files = 0

        for item in links:
            id, link_json = item
            try:
                link_data = json.loads(link_json)
                if isinstance(link_data, dict):
                    total_files += len(link_data)
                elif isinstance(link_data, list):
                    total_files += len(link_data)
            except json.JSONDecodeError as e:
                logging.error(f"Error decoding JSON for ID {id}: {e}")
        with tqdm(total=total_files, desc="Overall Progress") as overall_progress:
            async with aiohttp.ClientSession() as session:
                for item in links:
                    id, link_json = item
                    try:
                        link_data = json.loads(link_json)

                        if isinstance(link_data, dict):
                            for key, link in link_data.items():
                                if isinstance(link, str) and link.startswith(('https://service', 'ftp://')):
                                    save_dir = os.path.join(self.download_dir, str(id))
                                    os.makedirs(save_dir, exist_ok=True)
                                    task = self._async_download_file(session, link, save_dir, semaphore,
                                                                     overall_progress=overall_progress)
                                    tasks.append(task)
                        elif isinstance(link_data, list):
                            for link in link_data:
                                if isinstance(link, str) and link.startswith(
                                        ('https://service', 'ftp://', 'https://storage')):
                                    save_dir = os.path.join(self.download_dir, str(id))
                                    os.makedirs(save_dir, exist_ok=True)
                                    task = self._async_download_file(session, link, save_dir, semaphore,
                                                                     overall_progress=overall_progress)
                                    tasks.append(task)
                        else:
                            logging.error(
                                f"Unsupported data type for ID {id}: Expected dict or list, got {type(link_data)}")

                    except json.JSONDecodeError as e:
                        logging.error(f"Error decoding JSON for ID {id}: {e}")

                await asyncio.gather(*tasks)