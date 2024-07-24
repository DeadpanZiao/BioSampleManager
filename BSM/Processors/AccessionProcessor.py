from BSM.DataController import DataController
from BSM.Processors.BaseWorkflowProcessor import BaseWorkflowProcessor


class SCPAccessionProcessor(BaseWorkflowProcessor):
    def process(self, data):
        # 返回输入的数据和一个随机的布尔值
        dc = DataController()
        data_to_compare = dc.read_all_data()



data_to_process = {
    'collection_name': 'Sample Atlas',
    'collection_id': 'abc123',
    'publication_title': 'A study on...',
    'publication_doi': '10.1001/jama.2021.12345',
    'organism': 'Homo sapiens',
    'tissue': 'Brain',
    'tech': 'RNA-seq',
    'rna_source': 'Nuclei',
    'public': True
}

proc = SCPAccessionProcessor()
proc.process(data_to_process)