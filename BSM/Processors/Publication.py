from BSM.Processors.BaseWorkflowProcessor import TestBaseWorkflowProcessor,BaseWorkflowProcessor
from BSM.DataController import DataController
import json
import re
class PublicaationProcessor(BaseWorkflowProcessor):

    def singlecell(self,input_data):
        input_title = input_data[0].get('publications', [])[0].get('title', None)
        input_pmcid = input_data[0].get('publications', [])[0].get('pmcid', None)
        url = input_data[0].get('publications', [])[0].get('url', None)
        pattern = r"10.\d{4,9}/[-._;()/:A-Z0-9]+"
        # 搜索匹配的模式
        match = re.search(pattern, url, re.IGNORECASE)
        input_doi = match.group()
        return input_doi,input_title,input_pmcid

    def cellxgene(self,input_data):
        input_doi = input_data[0]['datasets'][0]['collection_doi']
        input_title = input_data[0]['datasets'][0]['collection_name']
        return input_doi,input_title

    def exploredata(self,input_data):
        input_title = input_data[0]['projects'][0]['publications'][0]['publicationTitle']
        input_doi = input_data[0]['projects'][0]['publications'][0]['doi']
        return input_doi, input_title

    def process(self, data):
        # 实例化 DataController 对象
        dc = DataController()

        # 读取所有数据
        data_from_db = dc.read_all_data()
        print(f"Data from DB: {data_from_db}")

        # 将数据库中的数据解析为JSON
        db_json_list = json.loads(data_from_db)
        # db_json_list = [{
        #     "doi": "10.1016/j.cell.2015.11.009",
        #     "title":"Single-Cell Genomics Unveils Critical Regulators of Th17 Cell Pathogenicity"
        # }]
        # 从输入数据解析出DOI和title
        input_json = json.loads(data)

        # 从输入判断使用哪个函数解析输入数据

        # input_doi, input_title, input_pmcid = self.singlecell(data)
        if 'accession' in input_json[0]:
            input_doi, input_title, input_pmcid= self.singlecell(input_json)
        elif 'collection_id' in input_json[0]:
            input_doi, input_title = self.cellxgene(input_json)
        elif 'projects' in input_json[0]:
            input_doi, input_title = self.exploredata(input_json)
        else:
            raise ValueError("Unknown data format")


        # 比较输入数据与数据库中的数据
        for db_json in db_json_list:
            db_doi = db_json.get("doi")
            db_title = db_json.get("title")

            if input_doi == db_doi and input_title == db_title:
                return data, False

        # 如果没有找到匹配项，返回True
        return data, True



