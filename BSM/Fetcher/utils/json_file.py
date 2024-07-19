import json
import ijson

class JsonManager(object):
    def __init__(self,  filename):
        self.filename = filename

    def save(self, data):
        with open(self.filename, 'w') as file:
            json.dump(data, file, indent=4)


    def write_large_json(self, data):
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
            for item in ijson.items(f, 'item'):
                items.append(item)
        return items