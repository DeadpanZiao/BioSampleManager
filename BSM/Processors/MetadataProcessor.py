from BSM.DataController import DataController
from BSM.Processors.BaseWorkflowProcessor import BaseWorkflowProcessor
from BSM.Fetcher.utils.json_file import JsonManager

import logging
logging.basicConfig(level=logging.CRITICAL + 1)
import random
import pandas as pd
from sentence_transformers import SentenceTransformer, util
model = SentenceTransformer('all-MiniLM-L6-v2')

class MetadataProcessor(BaseWorkflowProcessor):
    def __init__(self,db):
        super().__init__()
        self.db = db
        self.threshold = 0.85
        self.batch_size = 100

    def pre_process(self,data):
        # deal with input
        # todo: na取值需要特殊处理？

        if self.db == "CXG":
            # todo: 多个dataset的数据是否应该拆成多条数据？这里只取了1条
            dataset = data["datasets"][0]
            organism = [item["label"] for item in dataset["organism"]]
            disease = [item["label"] for item in dataset["disease"]]
            tissue = [item["label"] for item in dataset["tissue"]]
            tech = [item["label"] for item in dataset["assay"]]
            rna_source = dataset["suspension_type"]

        elif self.db == "HCA":
            organism1 = list(data["projects"][0].get("contributedAnalyses",{}).get("genusSpecies",{}).keys())
            if organism1 != []:
                organism = organism1
            else:
                if data["donorOrganisms"] != []:
                    organism2 = data["donorOrganisms"][0].get("genusSpecies",[])
                else:
                    organism2 = []
                organism = organism2
            if data["specimens"] != []:
                disease = data["specimens"][0].get("disease",[])
            else:
                disease = []
            tissue1 = list(data["projects"][0]["contributedAnalyses"].get("organ",{}).keys())
            if tissue1 != []:
                tissue = tissue1
            else:
                if data["cellSuspensions"] != []:
                    tissue2 = data["cellSuspensions"][0].get("organ",[])
                else:
                    tissue2 = []
                tissue = tissue2
            tech = data["protocols"][1].get("libraryConstructionApproach",[])
            rna_source = data["protocols"][1].get("nucleicAcidSource",[])

        elif self.db == "SCP":
            pass
            # todo: singlecellportal数据库目前无对应study metadata数据，待补充

        else:
            raise Exception("Unknown database")
        # key_data = {
        #     "organism": organism,
        #     "disease": disease,
        #     "tissue": tissue,
        #     "tech": tech,
        #     "rna_source": rna_source
        # }
        key_data = {
            "organism": ", ".join(filter(None, sorted(organism))),
            "disease": ", ".join(filter(None, sorted(disease))),
            "tissue": ", ".join(filter(None, sorted(tissue))),
            "tech": ", ".join(filter(None, sorted(tech))),
            "rna_source": ", ".join(filter(None, sorted(rna_source)))
        }
        return key_data

    def process(self, data):
        # get data of input
        input_data = self.pre_process(data)
        print(f'input_data:{input_data}')
        # get data of database
        data_to_compare = self.dc.read_all_data()

        # step1: compare ["organism","tech","rna_source"]
        i_str = input_data["organism"].lower() + input_data["tech"].lower() + input_data["rna_source"].lower()
        data_to_embedding = []
        for data in data_to_compare:
            db_str = data["Organism"].lower() + data["Tech"].lower() + data["RNA_source"].lower()
            if i_str == db_str:
                data_to_embedding.append(data)
            else:
                pass

        if len(data_to_embedding) == 0:
            print(f'''Reason: ["organism","tech","rna_source"] not same''')
            return data, False
        else:
            # step2: compare ["disease","tissue"] by SentenceTranformer
            df = pd.DataFrame(data_to_embedding)
            db_embeddings_disease = model.encode(df["Disease"].tolist(),
                                                 batch_size=self.batch_size,
                                                 show_progress_bar=True)
            db_embeddings_tissue = model.encode(df["Tissue"].tolist(),
                                                batch_size=self.batch_size,
                                                show_progress_bar=True)
            # compare
            query_embedding_disease = model.encode(input_data["disease"])
            query_embedding_tissue = model.encode(input_data["tissue"])
            similarities_disease = util.pytorch_cos_sim(query_embedding_disease, db_embeddings_disease)
            similarities_tissue = util.pytorch_cos_sim(query_embedding_tissue, db_embeddings_tissue)
            df['similarity_disease'] = similarities_disease
            df['similarity_tissue'] = similarities_tissue
            # result
            data_similar = df[(df['similarity_disease'] > self.threshold) & (df['similarity_tissue'] > self.threshold)]
            if len(data_similar) > 0:
                print('similar data:\n')
                # print row of similar data
                for index, row in data_similar.iterrows():
                    print(row)
                return data, True
            else:
                print(f'''Reason: ["disease","tissue"] not same''')
                return data, False


if __name__ == "__main__":
    # 方案零（已实现，排除）
    # 1、向量检索metadata的5个字段，给出数据库中最相似的3条记录
    # 2、输入llm判断是否存在重复

    # 方案一（by xinqi）
    # 1、获取全部metadata的取值，利用llm制作正则化list（value很多，有点困难）
    # 2、按照正则化后的value直接匹配，如果全部相等则返回true，否则返回false

    # 方案二（当前采用）
    # 1、第一步匹配organism、tech、rna_resource，匹配方式为处理完大小写之后直接匹配，false则直接返回false，true则进行第二步
    # 2、针对数据库进行向量检索（disease和tissue），设置语义相似度阈值，超过返回true，否则返回false

    # ---------test CXG-----------
    # test1: 随机一条数据
    # json_file1 = "../../DBS/cellxgene.json"
    # jf1 = JsonManager(json_file1)
    # data1 = jf1.read_large_json()
    # num = random.randint(0, len(data1))
    # input_data1 = data1[num]

    # test2: 构造一条模拟数据
    input_data1 = {
        "collection_id": "4828d33d-fb26-42e7-bf36-18293b0eec85",
        "datasets": [
            {
                "assay": [
                    {
                        "label": "10x 3' v3",
                        "ontology_term_id": "EFO:0009922"
                    }
                ],
                "dataset_id": "5dec4249-8459-4df0-8998-37193135754c",
                "dataset_version_id": "dea35fd3-c576-4ca5-9c10-72546e312b2e",
                "disease": [
                    {
                        "label": "normal",
                        "ontology_term_id": "PATO:0000461"
                    }
                ],
                "organism": [
                    {
                        "label": "Mus musculus",
                        "ontology_term_id": "NCBITaxon:10090"
                    }
                ],
                "suspension_type": [
                    "cell"
                ],
                "tissue": [
                    {
                        "label": "barrel cortex field",
                        "ontology_term_id": "UBERON:0010415",
                        "tissue_type": "tissue"
                    }
                ]
            }
        ]
    }

    db_name = "CXG"
    mp = MetadataProcessor(db_name)
    data, flag = mp.process(input_data1)
    print(flag)


    # ---------test HCA-----------
    # test1: 随机一条数据
    # json_file2 = "../../DBS/exploredata_json.json"
    # jf2 = JsonManager(json_file2)
    # data2 = jf2.read_large_json()
    # num = random.randint(0, len(data2))
    # input_data2 = data2[num]

    # test2: 构造一条模拟数据
    input_data2 = {
        "protocols":[
                {"workflow": ["raw_matrix_generation"]},
                {
                    "libraryConstructionApproach": ["10x 3' v3"],
                    "nucleicAcidSource": ["cell"]
                }
        ],
        "projects": [
            {"contributedAnalyses" : {
                "genusSpecies" : {"Homo sapiens": {}},
                "organ" : {"breast": {}}
            }}
        ],
        "specimens": [{
            "organ": ["breast"],
            "disease": ["normal"],
        }],
        "donorOrganisms": [
            {"genusSpecies": ["Homo sapiens"]}
        ],
        "cellSuspensions": [{"organ": ["heart"]}],
    }

    db_name = "HCA"
    mp = MetadataProcessor(db_name)
    data, flag = mp.process(input_data2)
    print(flag)
