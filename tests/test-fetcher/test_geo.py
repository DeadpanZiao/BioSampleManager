from Fetcher.SingleCellDBs.NCBI_GEO.geo import Geo
from Fetcher.SingleCellDBs import SingleCellDBFetcher
from utils.DBS.json_file import JsonManager
import glob
import time

# soft_filepath为soft文件要存的目录
geo_gse = Geo(soft_filepath='D:/3.3-zjprogram/BioDatabase/GEO/GEO_soft/')

# db_name为元数据保存的文件名与保存所执行时间的文件名
db_name = ['D:/3.3-zjprogram/BioDatabase/geo_metadata.json',
           'D:/3.3-zjprogram/BioDatabase/geo_metadata_saved_time.json']
geo_gse.merge_geo_json(db_name)
