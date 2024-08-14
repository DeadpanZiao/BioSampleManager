from BSM.DataController import data_controller
import re
import json
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from langchain_openai import ChatOpenAI


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
            if key in ['dataset', 'pmid', 'pmcid', 'doi'] and value is not None:
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


    def extract_single(self,input_metadata:dict):
        """extract metadata from a single input"""
        prompt = self.generate_prompt(input_metadata)
        response = self.chain_llm_api(prompt)
        token_usage = response.usage_metadata
        json_output = self._parse_json_from_response(response.content)
        output = self._check_single_output(input_metadata,json_output)

        return output,token_usage,input_metadata


    def extract_batch(self,input_metadata_list:list,max_workers=10):
        """Using thread pool to batch extract metadata from a batch of inputs"""

        results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self.extract_single, item) for item in input_metadata_list}
            # create tqdm progress bar
            progress_bar = tqdm(total=len(futures), desc="Inference Tasks", unit="task")
            i = 0
            for future in as_completed(futures):
                i += 1
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    print(f"处理第{i}个数据项时发生错误：{e}")
                finally:
                    progress_bar.update(1)
            progress_bar.close()

        return results


def special_prompt():
    # todo: 待调整优化各数据来源的special_prompt
    desc_normal = f""
    desc_cxg = f"Let's start with the basic information about the input data, which contains metadata about 1 project, corresponding to 1 specified doi, with 1 or more datasets in the project."
    desc_hca = f"Let's start with the basic information about the input data, which contains metadata about 1 project, corresponding to 1 or more doi."
    desc_scp = f"Let's start with the basic information about the input data, which contains metadata about 1 study."
    return {"normal":desc_normal, "cxg": desc_cxg, "hca": desc_hca, "scp": desc_scp}


def read_json_file(file_path):
    with open(file_path, 'r', encoding="utf-8") as file:
        data = json.load(file)
    return data


def read_excel_file(file_path):
    df = pd.read_excel(file_path,header=0)
    data = df.to_dict(orient='records')
    return data


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


if __name__ == "__main__":
    API_URL = "https://api.moonshot.cn/v1/"
    API_KEY = "your-key"
    MODEL = "moonshot-v1-32k"

    # 用cxg作为测试数据
    data_source = 'cxg'
    input_metadata_list = read_json_file(f"../../DBS/{data_source}.json")
    json_schema = read_excel_file("../../DBS/json_schema.xlsx")
    extractor = ProjectMetadataExtractor(data_source, API_URL, API_KEY, MODEL, json_schema)
    db_name = '../../DBS/projects.db'
    controller = data_controller.SampleController(db_name)

    # 测试单条抽取入库
    result = extractor.extract_single(input_metadata_list[0])
    print(f"output:\n{result[0]}")
    print(f"token_usage:\n{result[1]}")
    insert_data = convert_data_for_insert(result[0])
    print(insert_data)
    res = controller.insert_sample(insert_data)
    print(res)

    # # 测试批量抽取入库
    # results = extractor.extract_batch(input_metadata_list[3:6]) # 测试第4、5、6条数据
    # for result in results:
    #     print(f"output:\n{result[0]}")
    #     print(f"token_usage:\n{result[1]}")
    #     insert_data = convert_data_for_insert(result[0])
    #     res = controller.insert_sample(insert_data)
    #     print(res)

