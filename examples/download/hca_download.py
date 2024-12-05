from BSM.Downloader.downloader import HCADownloader

if __name__ == "__main__":
    db_path = "../../DBS/projects-hca-qwen2-72b-instruct1128.db"
    download_dir = r'D:/zjlab/data/'

    downloader = HCADownloader(db_path, download_dir)
    downloader.download_files()