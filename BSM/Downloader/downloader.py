import os
import aiohttp
from tqdm.asyncio import tqdm
import asyncio
import sqlite3
import json
import aiofiles
import requests

class BaseDownloader:
    def __init__(self, database_path, table_name, save_root):
        self.database_path = database_path
        self.table_name = table_name
        self.save_root = save_root

    def create_connection(self):
        return sqlite3.connect(self.database_path)

    async def check_file_exists(self, file_path):
        return os.path.exists(file_path)

    def get_response_headers(self, url):
        try:
            headers = {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"
            }
            response = requests.get(url, headers=headers, allow_redirects=False)
            return response.headers
        except Exception as e:
            print(f"An error occurred: {e}")
            return {}

    async def download_file(self, session, url, save_dir, semaphore, progress=None, overall_progress=None):
        raise NotImplementedError("This method should be overridden by subclasses")


class HCADownloader(BaseDownloader):
    def __init__(self, database_path, table_name, save_root):
        super().__init__(database_path, table_name, save_root)

    async def download_file(self, session, url, save_dir, semaphore, progress=None, overall_progress=None):
        async with semaphore:  # 控制并发量
            try:
                if url.startswith('ftp://'):
                    pass  # FTP链接的处理需要额外的库或方法，这里暂时不做处理
                else:
                    # 获取重定向后的URL
                    headers = self.get_response_headers(url)
                    url = headers.get('Location', url)

                async with session.get(url) as response:
                    if response.status == 200:
                        content_disposition = response.headers.get('Content-Disposition', '')
                        file_name = content_disposition.split('filename=')[-1].strip('"') or url.split('/')[-1].split('?')[0]
                        file_path = os.path.join(save_dir, file_name)

                        if await self.check_file_exists(file_path):
                            print(f"File {file_name} already exists, skipping.")
                            if overall_progress:
                                overall_progress.update(1)
                            return

                        total_size = int(response.headers.get('Content-Length', 0))
                        wrote = 0

                        async with aiofiles.open(file_path, 'wb') as f:
                            with tqdm(total=total_size, unit='B', unit_scale=True, desc=file_name, leave=False) as pbar:
                                async for chunk in response.content.iter_chunked(1024 * 1024):  # 每次写入1MB 避免写入缓存
                                    await f.write(chunk)
                                    wrote += len(chunk)
                                    pbar.update(len(chunk))

                        if total_size != 0 and wrote != total_size:
                            print(f"ERROR, something went wrong downloading {file_name}")
                    else:
                        print(f"Failed to download {url}. Status code: {response.status}")
                    if overall_progress:
                        overall_progress.update(1)
            except Exception as e:
                print(f"Error downloading {url}: {e}")
                if overall_progress:
                    overall_progress.update(1)

    async def main(self):
        conn = self.create_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT internal_id as id, download_links as link FROM {self.table_name} WHERE download_links IS NOT NULL limit 2")
        links = cursor.fetchall()
        conn.close()

        semaphore = asyncio.Semaphore(5)  # 控制并发数为5
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
                print(f"Error decoding JSON for ID {id}: {e}")

        with tqdm(total=total_files, desc="Overall Progress") as overall_progress:
            async with aiohttp.ClientSession() as session:
                for item in links:
                    id, link_json = item
                    try:
                        link_data = json.loads(link_json)

                        if isinstance(link_data, dict):
                            for key, link in link_data.items():
                                if isinstance(link, str) and link.startswith(('https://service', 'ftp://')):
                                    save_dir = os.path.join(self.save_root, str(id))
                                    os.makedirs(save_dir, exist_ok=True)
                                    real_link = self.get_response_headers(link).get('Location', link)

                                    task = self.download_file(session, real_link, save_dir, semaphore, overall_progress=overall_progress)
                                    tasks.append(task)
                        elif isinstance(link_data, list):
                            for link in link_data:
                                if isinstance(link, str) and link.startswith(('https://service', 'ftp://', 'https://storage')):
                                    save_dir = os.path.join(self.save_root, str(id))
                                    os.makedirs(save_dir, exist_ok=True)

                                    task = self.download_file(session, link, save_dir, semaphore, overall_progress=overall_progress)
                                    tasks.append(task)
                        else:
                            print(f"Unsupported data type for ID {id}: Expected dict or list, got {type(link_data)}")

                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON for ID {id}: {e}")

                await asyncio.gather(*tasks)
