import unittest
from unittest.mock import patch, MagicMock

from Fetcher.SingleCellDBs import SingleCellPortalFetcher


class TestSingleCellPortalFetcher(unittest.TestCase):

    @patch('Fetcher.SingleCellDBs.SingleCellPortal.single_cell_portal.requests.get')
    @patch('Fetcher.SingleCellDBs.SingleCellPortal.single_cell_portal.JsonManager')
    def test_fetch(self, mock_json_manager, mock_get):
        # 设置模拟对象
        mock_response = MagicMock()
        mock_response.json.return_value = [{'accession': 'test1'}, {'accession': 'test2'}]
        mock_response.status_code = 200
        mock_get.return_value = mock_response
        mock_manager_instance = MagicMock()
        mock_json_manager.return_value = mock_manager_instance

        # 创建 SingleCellPortalFetcher 实例并调用 fetch 方法
        fetcher = SingleCellPortalFetcher()
        fetcher.fetch('db_name.json')

        # 检查是否正确调用了 requests.get
        self.assertEqual(mock_get.call_count, 3)  # 一次获取所有 studies，然后对每个 study 发起一次请求

        # 检查是否正确调用了 JsonManager 的 save 方法
        self.assertEqual(mock_manager_instance.save.call_count, 2)  # 一次保存所有 studies，一次保存合并的数据

if __name__ == '__main__':
    unittest.main()
