import json
import os
from tqdm import tqdm

class JsonManager(object):
    def __init__(self,  filename):
        self.filename = filename

    def save(self, data):
        with open(self.filename, 'w') as file:
            json.dump(data, file, indent=4)

    def save_by_lines(self, data):
        with open(self.filename, 'w', encoding='utf-8') as f:
            for item in tqdm(data, desc='Saving JSON'):
                json.dump(item, f, ensure_ascii=False)
                f.write('\n')

    def load_by_line(self):
        json_list = []
        # 逐行读取JSON文件并解析
        with open(self.filename, 'r', encoding='utf-8') as f:
            for line in f:
                # 解析JSON字符串为字典
                item = json.loads(line)
                json_list.append(item)

        return json_list

