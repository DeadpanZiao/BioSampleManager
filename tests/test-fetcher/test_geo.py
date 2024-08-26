from Fetcher.SingleCellDBs.NCBI_GEO.geo import GeoFetcher
from Fetcher.SingleCellDBs.NCBI_GEO.DownloadSoft import DownloadSoft

'''保存metadata的json文件'''
# soft_filepath为soft文件要存的目录
soft_filepath = 'D:/3.3-zjprogram/BioDatabase/GEO/GEO_soft/'
geo_gse = GeoFetcher(soft_filepath)
# db_name为元数据保存的文件名与保存所执行时间的文件名
json_path = 'D:/3.3-zjprogram/BioDatabase/jsons/'
geo_gse.fetch(json_path)

'''下载soft文件'''
downloader = DownloadSoft(soft_filepath)
# 下面三个函数可以单独调用，互不影响
# 下载之前下载失败的ids
# downloader.download_soft_failed_dl()
# 下载最新爬的id
# downloader.download_soft_new()
# 将新爬的ids添加倒gse_ids.txt中
downloader.save_as_gse_ids_file_new()
