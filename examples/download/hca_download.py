from BSM.Downloader.downloader import HCADownloader

if __name__ == "__main__":
    db_path = "/home/lza/BSM/DBS/projects-hca-moonshot-v1-128k1128.db"
    download_dir = r'/zjbs-data/hca/'

    downloader = HCADownloader(db_path, download_dir)
    downloader.download_files()