from Fetcher.SingleCellDBs.NCBI_GEO.geo import Geo

geo_gse = Geo(soft_filepath='D:/3.3-zjprogram/BioDatabase/GEO/')
geo_gse.fetch('D:/3.3-zjprogram/BioDatabase/GEO_metadata_all.json',
              'D:/3.3-zjprogram/BioDatabase/GEO_looptime_all.json')
