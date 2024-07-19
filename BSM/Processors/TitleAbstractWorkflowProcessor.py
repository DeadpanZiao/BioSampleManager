from BSM.DataController import DataController
from BaseWorkflowProcessor import BaseWorkflowProcessor
from BSM.Fetcher.utils.json_file import JsonManager
from sentence_transformers import SentenceTransformer


class TitleAbstractWorkflowProcessor(BaseWorkflowProcessor):
    def __init__(self):
        super().__init__()
        self.model = SentenceTransformer("all-MiniLM-L6-v2")
        self.threshold = 0.95

    def process(self, data):
        dc = DataController()
        data_to_compare = dc.read_all_data()
        title_abstract_to_compare = []
        for d in data_to_compare:
            if d['Publication_title'] is not None:
                title = d['Publication_title']
            else:
                title = ""

            if d['Collection_summary'] is not None:
                abstract = d['Collection_summary']
            else:
                abstract = ""

            title_abstract_to_compare.append(title + " " + abstract)
        # title_abstract_to_compare = [(d['Publication_title'] + ' ' + d['Collection_summary']) for d in data_to_compare]
        embeddings_data = self.model.encode([data])
        embeddings_title_abstract_to_compare = self.model.encode(title_abstract_to_compare)
        similarities = self.model.similarity(embeddings_data, embeddings_title_abstract_to_compare)
        # print(similarities)
        return any(x > self.threshold for x in similarities.data[0])


if __name__ == '__main__':

    p = TitleAbstractWorkflowProcessor()

    filename1 = '../../DBS/cellxgene.json'
    manager1 = JsonManager(filename1)
    data1 = manager1.read_large_json()

    for d in data1:
        title_abstract = d['name'] + " " + d['description']
        flag = p.process(title_abstract)
        print(flag)

    filename2 = '../../DBS/singlecellportal.json'
    manager2 = JsonManager(filename2)
    data2 = manager2.read_large_json()

    for d in data2:
        title_abstract = d['name'] + " " + d['description']
        flag = p.process(title_abstract)
        print(flag)

    filename3 = '../../DBS/exploredata_json.json'
    manager3 = JsonManager(filename3)
    data3 = manager3.read_large_json()

    for d in data3:
        projects = d['projects'][0]
        # if len(projects['publications']) > 1:
        #     print(projects)
        title_abstract = projects['projectTitle'] + " " + projects['projectDescription']
        flag = p.process(title_abstract)
        print(flag)

