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

class Downloader(BaseDownloader):
    def __init__(self, database_path, table_name, save_root, num_workers=1, downloader_type='hca', timeout=7200, **kwargs):
        super().__init__(database_path, table_name, save_root)
        self.num_workers = num_workers
        self.downloader_type = downloader_type
        self.timeout = timeout
        if downloader_type == 'hca':
            self.dcp = kwargs.get('dcp')
            self.cookie = None
        elif downloader_type == 'scp':
            self.cookie = self._process_cookie(kwargs.get('cookie'))
            self.dcp = None
        elif downloader_type == 'cxg':
            self.cookie = None
            self.dcp = None
        else:
            raise ValueError(f"unsupported database type: {downloader_type}")
    @staticmethod
    def _process_cookie(cookie):
        """
        Process the cookie parameter. It can be a dictionary or a path to a JSON file.
        If it's a path, load and return the dictionary.
        Otherwise, raise a ValueError.
        """
        if isinstance(cookie, dict):
            return cookie
        elif isinstance(cookie, str) and os.path.isfile(cookie):
            try:
                with open(cookie, 'r') as f:
                    return json.load(f)
            except Exception as e:
                raise ValueError(f"Failed to load cookie from JSON file at {cookie}: {e}")
        else:
            raise ValueError(f"Invalid cookie format. Expected a dictionary or a valid JSON file path, got {type(cookie)}")
    async def download_file(self, session, url, save_dir, semaphore, progress=None, overall_progress=None):
        async with semaphore:
            try:
                headers = {
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"
                }

                if self.downloader_type == 'hca':
                    if self.dcp is not None:
                        url = url.replace('dcp44', self.dcp)
                    final_url = self.get_response_headers(url).get('Location', url)
                    file_path = os.path.join(save_dir, final_url.split('/')[-1].split('?')[0])
                elif self.downloader_type == 'scp':  # scp
                    url = url.replace('/api/v1/site/studies', '/data/public')
                    final_url = url
                    file_path = os.path.join(save_dir, final_url.split('/')[-1].split('?')[-1].replace('filename=',''))
                elif self.downloader_type == 'cxg':
                    final_url = url
                    file_path = os.path.join(save_dir, final_url.split('/')[-1].split('?')[-1])
                else:
                    final_url = url
                    file_path = os.path.join(save_dir, final_url.split('/')[-1].split('?')[-1])
                if await self.check_file_exists(file_path):
                    wrote = os.path.getsize(file_path)
                    headers['Range'] = f'bytes={wrote}-'
                    print(f"Resuming download of {file_path} from byte {wrote}")
                else:
                    wrote = 0

                request_kwargs = {'headers': headers, 'timeout': self.timeout}
                if self.downloader_type == 'scp' and self.cookie:
                    request_kwargs['cookies'] = self.cookie

                async with session.get(final_url, **request_kwargs) as response:
                    if response.status == 416:
                        print(f"The local copy of {file_path} is complete.")
                        if overall_progress:
                            overall_progress.update(1)
                    else:
                        total_size = None
                        if response.status == 206:  # Partial Content
                            content_range = response.headers.get('Content-Range')
                            if content_range:
                                total_size = int(content_range.partition('/')[-1].strip())
                        elif response.status == 200:  # OK - whole file
                            total_size = int(response.headers.get('Content-Length', 0))

                        if total_size is None:
                            print("Warning: Unable to determine total size. Downloading without progress tracking.")
                            pbar = tqdm(unit='B', unit_scale=True, desc=f"Downloading {file_path}", leave=False)
                        else:
                            pbar = tqdm(total=total_size, unit='B', unit_scale=True, desc=f"Downloading {file_path}",
                                        initial=wrote, leave=False)

                        async with aiofiles.open(file_path, 'ab') as f:
                            async for chunk in response.content.iter_chunked(1024 * 1024):
                                await f.write(chunk)
                                wrote += len(chunk)
                                pbar.update(len(chunk))

                        pbar.close()

                        if total_size is not None and wrote != total_size:
                            print(
                                f"ERROR: Incomplete download detected for {file_path}. Expected {total_size} bytes, got {wrote}.")
                        else:
                            print(f"Successfully downloaded {file_path}")

            except Exception as e:
                print(f"Error occurred while downloading {url}: {e}")

            finally:
                if overall_progress:
                    overall_progress.update(1)

    async def main(self):
        conn = self.create_connection()
        cursor = conn.cursor()
        cursor.execute(f"SELECT internal_id as id, download_links as link FROM {self.table_name} WHERE download_links IS NOT NULL")
        links = cursor.fetchall()
        conn.close()

        semaphore = asyncio.Semaphore(self.num_workers)
        tasks = []
        total_files = 0

        # 计算总文件数
        for item in links:
            id, link_json = item
            try:
                link_data = json.loads(link_json)
                if isinstance(link_data, (dict, list)):
                    total_files += len(link_data)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON for ID {id}: {e}")

        with tqdm(total=total_files, desc="Overall Progress") as overall_progress:
            async with aiohttp.ClientSession() as session:
                selected_links = links[:] if self.downloader_type == 'scp' else links
                
                for item in selected_links:
                    id, link_json = item
                    try:
                        link_data = json.loads(link_json)
                        save_dir = os.path.join(self.save_root, str(id))
                        os.makedirs(save_dir, exist_ok=True)

                        if isinstance(link_data, dict):
                            for key, link in link_data.items():
                                if isinstance(link, str) and link.startswith(('https://', 'ftp://')):
                                    task = self.download_file(session, link, save_dir, semaphore, overall_progress=overall_progress)
                                    tasks.append(task)
                        elif isinstance(link_data, list):
                            for link in link_data:
                                if isinstance(link, str) and link.startswith(('https://', 'ftp://', 'https://storage')):
                                    task = self.download_file(session, link, save_dir, semaphore, overall_progress=overall_progress)
                                    tasks.append(task)
                        else:
                            print(f"Unsupported data type for ID {id}: Expected dict or list, got {type(link_data)}")

                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON for ID {id}: {e}")

                await asyncio.gather(*tasks)

if __name__ == "__main__":
    # HCA下载器示例
    hca_downloader = Downloader(
        database_path="path/to/your/database.db",
        table_name="your_table_name",
        save_root="path/to/save/directory",
        downloader_type="hca",
        num_workers=4,  # 并发数
        dcp="your_dcp_value",  # HCA特定参数
        timeout=7200  # 可选的超时设置
    )
    
    # SCP下载器示例
    scp_downloader = Downloader(
        database_path="path/to/your/database.db",
        table_name="your_table_name",
        save_root="path/to/save/directory",
        downloader_type="scp",
        num_workers=4,  # 并发数
        cookie={"your": "cookie"},  # SCP特定参数
        timeout=7200  # 可选的超时设置
    )

    # 运行下载器
    asyncio.run(hca_downloader.main())  # 或者
    asyncio.run(scp_downloader.main())