import random

from BSM.DataController import DataController


class BaseWorkflowProcessor:
    def __init__(self, positive_next_module=None, negative_next_module=None):
        self.positive_next_module = positive_next_module
        self.negative_next_module = negative_next_module
        self.dc = DataController()
    def process(self, data):
        # 这是数据处理模块的核心方法，每个具体的数据处理模块需要实现这个方法
        # 这个方法需要返回处理过的数据和一个标识符
        raise NotImplementedError("Subclasses must implement process method")

    def match_string_to_list_of_dicts(self, search_strings, list_of_dicts, keys_to_search):
        for dictionary in list_of_dicts:
            if any(dictionary.get(key) in search_strings for key in keys_to_search):
                return True
        return False


class TestBaseWorkflowProcessor(BaseWorkflowProcessor):
    def process(self, data):
        # 返回输入的数据和一个随机的布尔值
        dc = DataController()
        data_to_compare = dc.read_all_data()
        print(data_to_compare)
        return data, random.choice([True, False])
