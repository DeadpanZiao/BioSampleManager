from BSM.DataController import data_controller
import re
import json
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from langchain_openai import ChatOpenAI
source_info = [
        {"type": "cxg", "file_name": "cellxgene", "dataset_source": "CellxGene"},
        {"type": "hca", "file_name": "exploredata", "dataset_source": "Human Cell Atlas"},
        {"type": "scp", "file_name": "singlecellportal", "dataset_source": "Single Cell Portal"}
    ]

class ProjectMetadataExtractor():
    def __init__(self,data_source:str,api_url:str,api_key:str,model_name:str,json_schema:list):
        self.data_source = data_source
        self.api_url = api_url
        self.api_key = api_key
        self.model_name = model_name
        self.json_schema = json_schema

    def chain_llm_api(self,prompt):
        llm = ChatOpenAI(
            openai_api_base=self.api_url,
            openai_api_key=self.api_key,
            model_name=self.model_name,
            temperature=0.1,
        )
        return llm.invoke(prompt)

    def generate_prompt(self, input_metadata):
        prompt = f"You are an expert at biomedical and genomic information, you are given a task to parse the sample METADATA from a public database of a study to a given format, based on your domain knowledge and hints given. \n"
        prompt += special_prompt()[self.data_source] + "\n"  # special prompt for different data source
        prompt += f"Please align the input data to the provided json schema and return the alignment result strictly in json format. \n\n"
        # input metadata
        prompt += f"Input: \n\n{input_metadata}\n\n"
        # output json schema
        prompt += f"Output json schema: \n\n"
        for item in self.json_schema:
            prompt += f"- **{item['Field']}**: {item['Type']}, {item['Description']}\n"
        prompt += (
            f"\nRemember to respond with a markdown code snippet of a json blob, and NOTHING else, NO EXPLANATION\n"
            f"Note that in cases where information is not available, please respond with `null`.\n"
            f"Note that if a list of choices is given, select the closest description.")

        return prompt

    def _parse_json_from_response(self,response):
        """
        1.json load directly
        2.extract json str by regex from response
        """
        try:
            return json.loads(response)
        except json.JSONDecodeError:
            pattern = r'```json(.+?)```'
            match = re.search(pattern, response, re.DOTALL)
            if match:
                json_str = match.group(1).strip()
                return json.loads(json_str)
            else:
                return None

    def _check_single_output(self,input_metadata,json_output):
        """
        1. Check if the fields of LLM output is consistent with the schema
        2. Check if the content of some specific fields is derived from the original input
        """
        result = {}
        field_list = []
        for item in self.json_schema:
            field_list.append(item["Field"])
        if json_output is not None:
            for key, value in json_output.items():
                if key in field_list:
                    result[key] = value
                else:
                    pass
        for field in field_list:
            if result != {}:
                if field not in result.keys():
                    result[field] = None
            else:
                result[field] = None

        for key, value in result.items():
            if key in ['dataset', 'pmid', 'pmcid', 'doi', 'other_ids', 'technology_name'] and value is not None:
                input_str = json.dumps(input_metadata, ensure_ascii=False)
                new_value = []
                for item in value:
                    if str(item) in input_str:
                        new_value.append(item)
                    else:
                        pass
                if len(new_value) >= 1:
                    result[key] = new_value
                else:
                    result[key] = None

        return result

    def pre_process_data(self,input_metadata):
        # if hca, remove the "files" element
        if self.data_source == "hca":
            result = {key:value for key,value in input_metadata.items() if key != "files"}
            return result
        else:
            return input_metadata


    def extract_single(self,input_metadata:dict):
        input_metadata_new = self.pre_process_data(input_metadata)
        """extract metadata from a single input"""
        prompt = self.generate_prompt(input_metadata_new)
        response = self.chain_llm_api(prompt)
        token_usage = response.usage_metadata
        json_output = self._parse_json_from_response(response.content)
        output = self._check_single_output(input_metadata,json_output)

        return output,token_usage,input_metadata


    def extract_batch(self,input_metadata_list:list,max_workers=10):
        """Using thread pool to batch extract metadata from a batch of inputs"""

        results = []
        failed_tasks = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self.extract_single, item): index for index, item in enumerate(input_metadata_list)}
            # create tqdm progress bar
            progress_bar = tqdm(total=len(futures), desc="Inference Tasks", unit="task")
            for future in as_completed(futures):
                task_id = futures[future]
                try:
                    result = future.result()
                    results.append((task_id,result))
                except Exception as e:
                    print(f"处理第{task_id}个数据项时发生错误：{e}")
                    failed_tasks.append(task_id)
                finally:
                    progress_bar.update(1)
            progress_bar.close()
        # return success outputs, the indexes of failed tasks
        return results,failed_tasks

    def post_process_data(self,extract_single_result):
        result = extract_single_result[0]
        input_metadata = extract_single_result[2]

        for item in source_info:
            if item["type"] == self.data_source:
                result["dataset_source"] = item["dataset_source"]
                break
        result["raw_json"] = input_metadata

        return result


def special_prompt():
    # todo: 可以继续调整优化各数据来源的special_prompt
    desc_normal = f""
    desc_cxg = f"Let's start with the basic information about the input data, which contains metadata about 1 project, corresponding to 1 specified doi, with 1 or more datasets in the project. The 'dataset' information can be found from the 'link_name' of the 'links' in the 'datasets', note that only the id of the geo is needed for 'dataset' field, other ids can be put into the 'other_ids' field."
    desc_hca = f"Let's start with the basic information about the input data, which contains metadata about 1 project, corresponding to 1 or more doi."
    desc_scp = f"Let's start with the basic information about the input data, which contains metadata about 1 study. IDs like SCP... can be put into 'other_ids' fields. If the 'name' field contains content, it should be treated as the title of the project."
    return {"normal":desc_normal, "cxg": desc_cxg, "hca": desc_hca, "scp": desc_scp}


def read_json_file(file_path):
    with open(file_path, 'r', encoding="utf-8") as file:
        data = json.load(file)
    return data


def read_excel_file(file_path):
    df = pd.read_excel(file_path,header=0)
    data = df.to_dict(orient='records')
    return data

def save_json_file(data, file_path):
    with open(file_path, 'w', encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=4)


def convert_data_for_insert(data_dict):
    # 将数据字典中所有取值为list的转换为纯字符串
    result = {}
    for key,value in data_dict.items():
        if value is None:
            pass
        elif type(value).__name__ == 'list':
            string_value = json.dumps(value, ensure_ascii=False)
            result[key] = string_value
        else:
            result[key] = value

    return result

def generate_json_name(data_source,current_number):
     json_name = f"{data_source}_{current_number:06d}"
     return json_name


if __name__ == "__main__":
    API_URL = "https://api.moonshot.cn/v1/"
    API_KEY = ""    ######### Commit记得删除！！！
    MODEL = "moonshot-v1-128k"  ########## 可用8k、32k、128k

    # 数据文件
    data_source = 'scp'
    for item in source_info:
        if item["type"] == data_source:
            file_name = item["file_name"]
            break
    input_metadata_list = read_json_file(f"../../DBS/{file_name}.json") # 原始json数据路径
    print(len(input_metadata_list))
    json_schema = read_excel_file("../../DBS/json_schema.xlsx") # json_schema路径
    extractor = ProjectMetadataExtractor(data_source, API_URL, API_KEY, MODEL, json_schema)


    # # 测试单条抽取保存为json
    # number = 1    # 第几条数据
    # start_time = time.time()
    # result = extractor.extract_single(input_metadata_list[number-1])
    # print(f"output:\n{result[0]}")
    # print(f"token_usage:\n{result[1]}")
    # result_data = extractor.post_process_data(result)
    # print(result_data)
    # result_json_path = f"../../../Bio Data/kimi_output_test/{generate_json_name(data_source,number)}.json"
    # save_json_file(result_data,result_json_path)
    # end_time = time.time()
    # elapsed_time = end_time - start_time  # 计算运行时间
    # print(f"运行时间：{elapsed_time}秒")


    # 批量抽取保存为json
    results,failed_tasks = extractor.extract_batch(input_metadata_list,max_workers=20) # 批量抽取，可以调整max_workers
    print(failed_tasks)
    for result in results:
        task_id = result[0]
        content = result[1] # 每条抽取结果
        # print(content)
        result_data = extractor.post_process_data(content)  # 处理成保存json需要的内容
        # print(result_data)
        ######### 修改json保存路径
        result_json_path = f"../../../Bio Data/kimi_output/{generate_json_name(data_source, task_id+1)}.json"
        save_json_file(result_data, result_json_path)