from Fetcher.SingleCellDBs.NCBI_GEO.geo import Geo
from Fetcher.SingleCellDBs import SingleCellDBFetcher
from utils.DBS.json_file import JsonManager
import glob
import time


geo_gse = Geo(soft_filepath='D:/3.3-zjprogram/BioDatabase/GEO/GEO_soft/')
geo_gse.merge_geo_json('D:/3.3-zjprogram/BioDatabase/')
