import re
import json
from BSM.DataAccess.data_access import SampleAccess

class process():
    def Cxg_Add(self, data, db_name):
        publications_db = SampleAccess(db_name)
        for row in data:
            doi = row["doi"]
            dataset_pattern = r'GSE\d{6}'
            dataset = []
            for i in row["links"]:
                if re.search(dataset_pattern, i["link_name"]):
                    dataset.append(re.search(dataset_pattern, i["link_name"]).group())
            if dataset == []:
                data = None
            else:
                data = dataset[0]
            title = row["name"]
            project_description = row["description"]
            species = row["datasets"][0]['organism'][0]['label']
            organ = row['datasets'][0]['tissue'][0]['label']
            technology_name = row['datasets'][0]['assay'][0]['label']
            publications = {
                'doi' : doi,
                'dataset' : data,
                'pmid' : None,
                'pmcid' : None,
                'title' : title,
                'topic' : None,
                'project_title' : None,
                'project_description' : project_description,
                'is_cancer' : None,
                'species' : species,
                'organ' : organ,
                'sample_location' : None,
                'library_strategy' : None,
                'library_layout' : None,
                'single_cell' : None,
                'nuclei_extraction' : None,
                'technology_name' : technology_name,
                'instrument' : None,
                'extraction_protocol' : None,
                'data_processing' : None
            }
            if publications_db.get_publication_by_doi(doi)['status'] == 'not_found':
                publications_db.insert_sample(publications)

            else:
                publications_db.update_sample(doi, publications)

    def Epd_add(self, data, db_name):
        publications_db = SampleAccess(db_name)
        for row in data:
            doi = None
            pmcid = None
            if row['projects'][0]['publications']:
                doi = row['projects'][0]['publications'][0]['doi']
                pmcid_pattern = r'PMC\d{7}'
                pmcid_match = re.search(pmcid_pattern, str(row['projects'][0]['publications'][0]['publicationUrl']))
                if pmcid_match:
                    pmcid = pmcid_match.group()

                title = row['projects'][0]['publications'][0]['publicationTitle']

            dataset = []
            dataset_pattern = r'GSE\d{6}'
            for i in row['projects'][0]['accessions']:
                if re.search(dataset_pattern, i['accession']):
                    dataset.append(re.search(dataset_pattern, i['accession']).group())

            if dataset == []:
                dataset = 'null'

            dataset = str(dataset)
            project_title = row['projects'][0]['projectTitle']
            project_description = row['projects'][0]['projectDescription']
            if row['donorOrganisms']:
                temp = row['donorOrganisms'][0]['disease']
                if re.search(r'cancer', str(temp)):
                    is_cancer = "TRUE"

                else:
                    is_cancer = "FALSE"
                species = row['donorOrganisms'][0]['genusSpecies']

            else:
                is_cancer = "NS"
                species = None

            if row["specimens"]:
                organ = row['specimens'][0]['organ']

            else:
                organ = None
            if row['samples']:
                sample_location = row['samples'][0]['sampleEntityType']

            else: sample_location = None

            for i in row['protocols']:
                if 'nucleicAcidSource' in i:
                    if i['nucleicAcidSource'] == ['single cell']:
                        single_cell = "TRUE"

                    else:
                        single_cell = "FALSE"

                    if i['nucleicAcidSource'] == ['single cell']:
                        nuclei_extracion = "TRUE"

                    else:
                        nuclei_extracion = "FALSE"

            for i in row['protocols']:
                if 'libraryConstructionApproach' in i:
                    technology_name = i['libraryConstructionApproach']

            for i in row['protocols']:
                if 'instrumentManufacturerModel' in i:
                    instrument = i['instrumentManufacturerModel']

            data_processing = None
            for i in row['protocols']:
                if 'workflow' in i:
                    data_processing = i['workflow']

            publications = {
                'doi': doi,
                'dataset': dataset,
                'pmid': None,
                'pmcid': pmcid,
                'title': title,
                'topic': None,
                'project_title': project_title,
                'project_description': str(project_description),
                'is_cancer': is_cancer,
                'species': str(species),
                'organ': str(organ),
                'sample_location': str(sample_location),
                'library_strategy': None,
                'library_layout': None,
                'single_cell': single_cell,
                'nuclei_extraction': nuclei_extracion,
                'technology_name': str(technology_name),
                'instrument': str(instrument),
                'extraction_protocol': None,
                'data_processing': str(data_processing)
            }
            if publications_db.get_publication_by_doi(doi)['status'] == 'not_found':
                publications_db.insert_sample(publications)

            else:
                publications_db.update_sample(doi, publications)



    def Scp_add(self, data, db_name):
        publications_db = SampleAccess(db_name)
        doi = None
        for row in data:
            doi_pattern_1 = r'doi.org/([^\s,]+)'
            doi_pattern_2 = r'doi:\s*10\.\d+\/[^\s;]+'
            if row['publications']:
                doi_text_1 = row['publications'][0]['url']
                doi_text_2 = pmid_text =  row['publications'][0]['citation']
                pmcid = row['publications'][0]['pmcid']

            if re.search(doi_pattern_1, doi_text_1):
                doi = re.search(doi_pattern_1, doi_text_1).group()

            elif re.search(doi_pattern_2, doi_text_2):
                search = re.search(doi_pattern_2, doi_text_2).group()
                doi = [search.split(':')][0][1].strip()

            pmid_pattern = r'PMID:\s*(\d+)'
            pmid = None
            if re.search(pmid_pattern, pmid_text):
                pmid = re.search(pmid_pattern, pmid_text).group(1)

            title = row['name']
            project_description = row['description']
            is_cancer = "FALSE"
            if re.search(r'cancer', row['description']):
                is_cancer = "TRUE"

            library_strategy_pattern = r'\w+-seq'
            library_strategy_temp = []
            if re.search(library_strategy_pattern, row['description']):
                library_strategy_temp.append(re.search(library_strategy_pattern, row['description']).group())
            library_strategy = str(library_strategy_temp)
            publications = {
                'doi': doi,
                'dataset': None,
                'pmid': pmid,
                'pmcid': pmcid,
                'title': title,
                'topic': None,
                'project_title': None,
                'project_description': project_description,
                'is_cancer': is_cancer,
                'species': None,
                'organ': None,
                'sample_location': None,
                'library_strategy': library_strategy,
                'library_layout': None,
                'single_cell': None,
                'nuclei_extraction': None,
                'technology_name': None,
                'instrument': None,
                'extraction_protocol': None,
                'data_processing': None
            }
            if publications_db.get_publication_by_doi(doi)['status'] == 'not_found':
                publications_db.insert_sample(publications)

            else:
                publications_db.update_sample(doi, publications)





if __name__ == '__main__':
    add = process()
    cxg_file_path = r'C:\Users\wxj01\Desktop\singlecelldbs\cellxgene.json'
    epd_file_path = r'C:\Users\wxj01\Desktop\singlecelldbs\exploredata_json.json'
    scp_file_path = r'C:\Users\wxj01\Desktop\singlecelldbs\singlecellportal.json'
    with open(scp_file_path, 'r', encoding='utf_8') as s:
        scp_data = json.load(s)
        add.Scp_add(scp_data, 'publications.db')

    with open(cxg_file_path, 'r', encoding='utf_8') as c:
        cxg_data = json.load(c)
        add.Cxg_Add(cxg_data, 'publications.db')

    with open(epd_file_path, 'r', encoding='utf-8') as e:
        epd_data = json.load(e)
        add.Epd_add(epd_data, 'publications.db')