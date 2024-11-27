import os

from BSM.Fetcher.SingleCellDBs import ExploreDataFetcher


def main():
    output_dir = 'D:\projects\BSM\jsons'
    fetcher = ExploreDataFetcher()
    fetcher.fetch(os.path.join(output_dir, 'hca1127_rawjson.json'))

if __name__ == '__main__':
    main()