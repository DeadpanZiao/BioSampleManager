import unittest
from unittest.mock import patch, Mock

from Fetcher.SingleCellDBs.SingleCellPortal.single_cell_portal import SingleCellPortal

class TestSingleCellPortal(unittest.TestCase):

    @patch('requests.get')
    @patch('utils.DBS.json_file.JsonManager.save')
    def test_fetch(self, mock_json_manager, mock_requests_get):
        # 创建一个Cellxgene实例
        singlecellportal = SingleCellPortal()

        # 模拟requests.get方法返回的数据
        mock_response = Mock()
        mock_response.json.return_value = {"key": "value"}
        mock_response.raise_for_status = Mock()
        mock_requests_get.return_value = mock_response

        # 模拟JsonManager实例和save方法
        mock_manager_instance = Mock()
        mock_json_manager.return_value = mock_manager_instance

        # 调用fetch方法
        singlecellportal.fetch()
        # 检查requests.get方法是否被正确调用
        mock_requests_get.assert_any_call(singlecellportal.datasets_url, headers=singlecellportal.headers, verify=False)
        # # 检查JsonManager构造函数是否被正确调用
        # mock_json_manager.assert_called_once_with()
        #
        # # 检查JsonManager.save方法是否被正确调用
        # mock_manager_instance.save.assert_called_once_with({"key": "value"})


if __name__ == '__main__':
    unittest.main()
