from Fetcher.SingleCellDBs.NCBI_GEO.DownloadSoft import DownloadSoft

downloader = DownloadSoft(soft_filepath='D:/3.3-zjprogram/BioDatabase/GEO/GEO_soft/')
'''下面三个函数可以单独调用'''

# 下载之前下载失败的ids
# downloader.download_soft_failed_dl()

# 下载最新爬的id
downloader.download_soft_new()

# 将新爬的ids添加倒gse_ids.txt中
downloader.save_as_gse_ids_file_new()
