import re

from BSM.DataController import DataController
from BSM.Processors.BaseWorkflowProcessor import BaseWorkflowProcessor


class SCPAccessionProcessor(BaseWorkflowProcessor):
    def process(self, data):
        accession = d['accession']
        data_to_compare = self.dc.read_all_data() # list of dict
        flag = self.match_string_to_list_of_dicts(search_strings=[accession], list_of_dicts=data_to_compare,
                                                  keys_to_search=['INSDC_project', 'INSDC_study', 'GEO'])
        return data, flag

class CXGAccessionProcessor(BaseWorkflowProcessor):
    def process(self, data):
        links = d['links']
        gse_pattern = re.compile(r'GSE\d+')
        gse_values = set()
        for link in links:
            # 搜索link_name中的GSE值
            match_name = gse_pattern.search(link.get('link_name', ''))
            if match_name:
                gse_values.add(match_name.group())

            # 搜索link_url中的GSE值
            match_url = gse_pattern.search(link.get('link_url', ''))
            if match_url:
                gse_values.add(match_url.group())

        data_to_compare = self.dc.read_all_data()
        flag = self.match_string_to_list_of_dicts(search_strings=gse_values, list_of_dicts=data_to_compare,
                                                  keys_to_search=['INSDC_project', 'INSDC_study', 'GEO'])
        return data, flag
class HCAAccessionProcessor(BaseWorkflowProcessor):
    def process(self, data):
        acc = data['projects'][0]['accessions']
        gse_pattern = re.compile(r'GSE\d+')
        insdc_project_pattern = re.compile(r'SRP\d+|ERP\d+|DRP\d+')
        insdc_study_pattern = re.compile(r'PRJNA\d+|PRJEB\d+|PRJDA\d+')
        accessions = set()

        for entry in acc:
            namespace = entry.get('namespace')
            accession = entry.get('accession')

            if accession:
                if (namespace == 'geo_series' and gse_pattern.match(accession)) or \
                        (namespace == 'insdc_project' and insdc_project_pattern.match(accession)) or \
                        (namespace == 'insdc_study' and insdc_study_pattern.match(accession)):
                    accessions.add(accession)
        data_to_compare = self.dc.read_all_data()
        flag = self.match_string_to_list_of_dicts(search_strings=list(accessions), list_of_dicts=data_to_compare,
                                                  keys_to_search=['INSDC_project', 'INSDC_study', 'GEO'])
        return data, flag

import json

proc = HCAAccessionProcessor()

file_path = r'D:\projects\BSM\exploredata_json.json'
with open(file_path, 'r', encoding='utf-8') as file:
    data = json.load(file)
for d in data[:]:
    proc.process(d)

