import unittest
from unittest.mock import patch, MagicMock
from BSM.Fetcher.SingleCellDBs import CellxgeneFetcher

class TestCellxgeneFetcher(unittest.TestCase):

    @patch('Fetcher.SingleCellDBs.Cellxgene.cellxgene.requests.get')
    @patch('Fetcher.SingleCellDBs.Cellxgene.cellxgene.JsonManager')
    def test_fetch(self, mock_json_manager, mock_get):
        # 设置模拟对象
        mock_response = MagicMock()
        mock_response.json.return_value = {'data': 'test'}
        mock_get.return_value = mock_response
        mock_manager_instance = MagicMock()
        mock_json_manager.return_value = mock_manager_instance

        # 创建 CellxgeneFetcher 实例并调用 fetch 方法
        fetcher = CellxgeneFetcher()
        fetcher.fetch('db_name')

        # 检查是否正确调用了 requests.get
        mock_get.assert_called_once_with(url=fetcher.datasets_url, headers=fetcher.headers)

        # 检查是否正确调用了 JsonManager 的 save 方法
        mock_manager_instance.save.assert_called_once_with({'data': 'test'})

if __name__ == '__main__':
    unittest.main()
