import asyncio
from BSM.Downloader.downloader import HCADownloader


def start_downloading(database_path, table_name, save_root):
    downloader = HCADownloader(database_path, table_name, save_root)
    asyncio.run(downloader.main())


if __name__ == '__main__':
    # 示例调用，实际路径应根据需要替换

    database_path = r'E:\projects-hca-qwen2-72b-instruct1128.db'
    save_root = r'E:\backup\hca_download'
    table_name = r'Sample'



    start_downloading(database_path, table_name, save_root)