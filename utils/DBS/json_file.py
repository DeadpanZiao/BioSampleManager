import json
import os


class JsonManager(object):
    def __init__(self, filename):
        self.filename = filename

    def save(self, data):
        with open(self.filename, 'w', encoding='utf-8') as file:
            json.dump(data, file, indent=4)
