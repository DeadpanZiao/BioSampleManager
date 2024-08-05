import re
import json
from BSM.DataAccess.data_access import ProjectAccess

class CXG_doi():
    def process(self, data, db_name):
        publications_db = ProjectAccess(db_name)
        for col in data:
            doi = col["collection_doi"]
            if publications_db.get_publication_by_doi(doi)['status'] == 'error':
                dataset = col["dataset_id"]
                title = col["title"]
                pub_date = re.match(r"\d+-\d+-\d+",col["published_at"]).group()
                pub_status = col['processing_status']
                fulltext_link_pattern = r'Discover in Collection:\s*(https?://\S+)'
                fulltext_link = re.search(fulltext_link_pattern, col["citation"]).group()
                publications = [
                    doi,None, None, dataset, title, None, None, None, None, None, None, pub_date, None, None, None, pub_status, None, fulltext_link, None
                ]
                publications_db.insert_publication(publications)


if __name__ == '__main__':
    add = CXG_doi()
    file_path = r'D:\pycharm\learn\singlecelldbs\cellxgene.json'
    with open(file_path, 'r', encoding='utf_8') as e:
        data = json.load(e)
        add.process(data, 'publications.db')
