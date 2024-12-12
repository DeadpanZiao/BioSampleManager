import asyncio
from BSM.Downloader.downloader import HCADownloader

def start_downloading(database_path, table_name):
    downloader = HCADownloader(database_path, table_name)
    async def run_downloader():
        await downloader.async_download_files()
    asyncio.run(run_downloader())


if __name__ == '__main__':
    database_path = r'../../DBS/projects-hca-qwen2-72b-instruct1128.db'
    save_root = r'D:\backup\hca_download'

    start_downloading(database_path, save_root)