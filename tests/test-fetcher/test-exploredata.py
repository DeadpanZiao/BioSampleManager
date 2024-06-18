# import unittest
from unittest.mock import patch, Mock

from Fetcher.SingleCellDBs.ExploreData.exploredata import ExploreData


# class TestExploredata(unittest.TestCase):
#
#     # @patch('requests.get')
#     # @patch('utils.DBS.json_file.JsonManager.save')
#     def test_fetch(self):
#         # 创建一个Cellxgene实例
#         exploredata = ExploreData()
#
#         # # 模拟requests.get方法返回的数据
#         # mock_response = Mock()
#         # mock_response.json.return_value = {"key": "value"}
#         # mock_response.raise_for_status = Mock()
#         # mock_requests_get.return_value = mock_response
#         #
#         # # 模拟JsonManager实例和save方法
#         # mock_manager_instance = Mock()
#         # mock_json_manager.return_value = mock_manager_instance
#
#         # 调用fetch方法
#         project_data = exploredata.fetch_project()
#         print(project_data)
#         # 检查requests.get方法是否被正确调用
#         # mock_requests_get.assert_called_once_with(url=cellxgene.datasets_url, headers=cellxgene.headers)
#
#         # # 检查JsonManager构造函数是否被正确调用
#         # mock_json_manager.assert_called_once_with()
#         #
#         # # 检查JsonManager.save方法是否被正确调用
#         # mock_manager_instance.save.assert_called_once_with({"key": "value"})


if __name__ == '__main__':
    # unittest.main()
    exploredata = ExploreData()
    exploredata.fetch('exploredata.json')
    # print(len(project_data))
