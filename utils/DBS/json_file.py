import json
import os
from tqdm import tqdm
import ijson

class JsonManager(object):
    def __init__(self,  filename):
        self.filename = filename

    def save(self, data):
        with open(self.filename, 'w') as file:
            json.dump(data, file, indent=4)

    # def save_by_lines(self, data):
    #     with open(self.filename, 'w', encoding='utf-8') as f:
    #         for item in tqdm(data, desc='Saving JSON'):
    #             json.dump(item, f, ensure_ascii=False)
    #             f.write('\n')
    #
    # def load_by_line(self):
    #     json_list = []
    #     # 逐行读取JSON文件并解析
    #     with open(self.filename, 'r', encoding='utf-8') as f:
    #         for line in f:
    #             # 解析JSON字符串为字典
    #             item = json.loads(line)
    #             json_list.append(item)
    #
    #     return json_list

    def write_large_json(self, data):
        # 将data保存成一个大json，data中每一个item为一个字典，可直接dump成json
        with open(self.filename, 'w', encoding='utf-8') as f:
            f.write('[\n')
            first = True
            for item in data:
                if not first:
                    f.write(',\n')
                json.dump(item, f, ensure_ascii=False)
                first = False
            f.write('\n]')

    def read_large_json(self):
        items = []
        with open(self.filename, 'r', encoding='utf-8') as f:
            # 使用 ijson 解析器逐条读取 JSON 对象
            for item in ijson.items(f, 'item'):
                items.append(item)
        return items
