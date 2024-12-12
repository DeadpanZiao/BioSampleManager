import asyncio
from BSM.Downloader.downloader import HCADownloader

def start_downloading(database_path,  save_root):
    downloader = HCADownloader(database_path, save_root)
    asyncio.run(downloader.main())


if __name__ == '__main__':
    database_path = r'../../DBS/projects-hca-qwen2-72b-instruct1128.db'
    save_root = r'D:/zjlab/data/'

    start_downloading(database_path,  save_root)