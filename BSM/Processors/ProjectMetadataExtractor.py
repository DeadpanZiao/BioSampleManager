import logging
import math

from BSM.DataController import data_controller
import re
import json
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from langchain_openai import ChatOpenAI, OpenAI
from langchain.text_splitter import RecursiveJsonSplitter

from BSM.DataController.data_controller import SampleController

source_info = [
    {"type": "cxg", "file_name": "cellxgene", "dataset_source": "CellxGene"},
    {"type": "hca", "file_name": "exploredata", "dataset_source": "Human Cell Atlas"},
    {"type": "scp", "file_name": "singlecellportal", "dataset_source": "Single Cell Portal"}
]


class ProjectMetadataExtractor():
    def __init__(self, data_source: str, api_url: str, api_key: str, model_name: str, json_schema: list):
        self.data_source = data_source
        self.api_url = api_url
        self.api_key = api_key
        self.model_name = model_name
        self.json_schema = json_schema

    def chain_llm_api(self, prompt):
        llm = ChatOpenAI(
            openai_api_base=self.api_url,
            openai_api_key=self.api_key,
            model_name=self.model_name,
            temperature=0.0,
        )
        return llm.invoke(prompt)

    def generate_prompt(self, input_metadata):
        prompt = f"You are an expert at biomedical and genomic information, you are given a task to parse the sample METADATA from a public database of a study to a given format, based on your domain knowledge and hints given. \n"
        prompt += special_prompt()[self.data_source] + "\n"  # special prompt for different data source
        prompt += f"Please align the input data to the provided json schema and return the alignment result strictly in json format. Do not include comments in the returned JSON. \n\n"
        prompt += "There are chances that the geo_id, pmid, pmcid, doi, are not provided in the input, use null in this case. Be strict and careful with keys above\n" \
                  "The ID shown in the context that starts with 'GSE' is highly to be a geo_id. ALL other possible ids shown in the context must be put into other_ids. Possible id value longer than 30 characters does not belong to geo_id. Put them into other_ids instead"
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

    def _parse_json_from_response(self, response):
        """
        1.json load directly
        2.extract json str by regex from response
        """
        try:
            print(json.loads(response))
            return json.loads(response)
        except json.JSONDecodeError:
            pattern = r'```json(.+?)```'
            match = re.search(pattern, response, re.DOTALL)
            if match:
                json_str = match.group(1).strip()
                fixed_json = json_str.replace(r'\xa0', ' ')
                return json.loads(fixed_json, strict=False)
            else:
                return None

    def _check_single_output(self, input_metadata, json_output):
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

    def pre_process_data(self, input_metadata):
        #  if hca, remove the "files" element.
        if self.data_source == "hca":
            result = {key: value for key, value in input_metadata.items() if key != "files"}
            return result
        #  if cxg, remove the "development_stage", "donor_id", "self_reported_ethnicity", "sex" elements of "datasets".
        elif self.data_source == "cxg":
            result = {}
            for key, value in input_metadata.items():
                if key == "datasets":
                    new_datasets = []
                    for item in value:
                        if len(item) >= 1 and type(item).__name__ == 'dict':
                            new_item = {key: value for key, value in item.items() if
                                        key not in ["development_stage", "donor_id", "self_reported_ethnicity", "sex"]}
                            new_datasets.append(new_item)
                    result[key] = new_datasets
                else:
                    result[key] = value
            return result
        #  if scp, remove the "directory_listings", "full_description" and "upload_file_size", "media_url" elements of "study_files" elements.
        elif self.data_source == "scp":
            result = {}
            for key, value in input_metadata.items():
                if key in ["directory_listings", "full_description"]:
                    pass
                elif key == "study_files":
                    new_study_files = []
                    if len(value) >= 1 and type(value).__name__ == 'list':
                        for item in value:
                            new_item = {key: value for key, value in item.items() if
                                        key not in ["upload_file_size", "media_url"]}
                            new_study_files.append(new_item)
                        result[key] = new_study_files
                else:
                    result[key] = value
            return result
        else:
            return input_metadata

    def extract_single(self, input_metadata: dict):
        """extract metadata from a single input"""
        input_metadata_new = self.pre_process_data(input_metadata)
        prompt = self.generate_prompt(input_metadata_new)
        response = self.chain_llm_api(prompt)
        token_usage = response.usage_metadata
        json_output = self._parse_json_from_response(response.content)
        output = self._check_single_output(input_metadata_new, json_output)

        return output, token_usage, input_metadata

    def extract_batch(self, input_metadata_list: list, max_workers=10):
        """Using thread pool to batch extract metadata from a batch of inputs"""

        results = []
        failed_tasks = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self.extract_single, item): index for index, item in
                       enumerate(input_metadata_list)}
            # create tqdm progress bar
            progress_bar = tqdm(total=len(futures), desc="Inference Tasks", unit="task")
            for future in as_completed(futures):
                task_id = futures[future]
                try:
                    result = future.result()
                    results.append((task_id, result))
                except Exception as e:
                    print(f"error processing task {task_id}: {e}")
                    failed_tasks.append(task_id)
                finally:
                    progress_bar.update(1)
            progress_bar.close()
        # return success outputs, the indexes of failed tasks
        return results, failed_tasks

    def post_process_data(self, extract_single_result):
        result = extract_single_result[0]
        input_metadata = extract_single_result[2]

        for item in source_info:
            if item["type"] == self.data_source:
                result["dataset_source"] = item["dataset_source"]
                break
        result["raw_json"] = input_metadata

        return result

    def check_and_split_long_input(self, input_metadata: dict, length_limit=180000):
        """
        check if the length of input exceeds limit;
        if exceed limit, split the str by length.
        """
        json_splitter = RecursiveJsonSplitter(
            max_chunk_size=length_limit
        )

        json_str = str(input_metadata)
        if len(json_str) >= length_limit:
            json_parts = json_splitter.split_json(input_metadata)
            return False, json_parts
        else:
            return True, input_metadata

    def generate_merge_prompt(self, split_outputs):
        prompt = f"You are an expert at biomedical and genomic information. You need to complete a task to merge multiple results containing several fields into one result."
        prompt += (
            f"It is known that these results are extracted from different parts of the metadata of a research project."
            f"Please follow the schema of the input json data, aggregate them into one project-level data, and return the result strictly in json format."
        )
        # Input list
        prompt += f"\n\nInput\n"
        for item in split_outputs:
            prompt += f"```\n{item}\n```"
        # output json schema
        prompt += f"Output json schema: \n\n"
        for item in self.json_schema:
            prompt += f"- **{item['Field']}**: {item['Type']}, {item['Description']}\n"
        prompt += (
            f"\nRemember to respond with a markdown code snippet of a json blob, and NOTHING else, NO EXPLANATION\n"
            f"Note that in cases where information is not available, please respond with `null`.\n"
            # f"Note that 'dataset' field is ID of type GSE, for others please put it in 'other_ids' field.\n"
            f"Note that if value is a list, take the concatenated set."
        )

        return prompt

    def extract_single_longtext(self, input_metadata: dict):
        """extract metadata from a single input"""
        input_metadata_new = self.pre_process_data(input_metadata)
        within_limit, split_json_list = self.check_and_split_long_input(input_metadata_new)

        if within_limit:
            return self.extract_single(input_metadata)

        else:
            print(f"Since the data is long, split it into {len(split_json_list)} sections")
            split_outputs = []
            split_token_usage = []
            for input_json in split_json_list:
                output, token_usage, input_metadata_split = self.extract_single(input_json)
                split_outputs.append(output)
                split_token_usage.append(token_usage)

            # merge output
            input_tokens = 0
            output_tokens = 0
            total_tokens = 0
            for item in split_token_usage:
                input_tokens += item["input_tokens"]
                output_tokens += item["output_tokens"]
                total_tokens += item["total_tokens"]

            merge_prompt = self.generate_merge_prompt(split_outputs)
            merge_response = self.chain_llm_api(merge_prompt)
            merge_json_output = self._parse_json_from_response(merge_response.content)
            merge_output = self._check_single_output(input_metadata_new, merge_json_output)

            usage_metadata = merge_response.usage_metadata
            merge_input_tokens = input_tokens + usage_metadata["input_tokens"]
            merge_output_tokens = output_tokens + usage_metadata["output_tokens"]
            merge_total_tokens = total_tokens + usage_metadata["total_tokens"]
            merge_token_usage = {
                "input_tokens": merge_input_tokens,
                "output_tokens": merge_output_tokens,
                "total_tokens": merge_total_tokens,
            }

            return merge_output, merge_token_usage, input_metadata


def special_prompt():
    # todo: 可以继续调整优化各数据来源的special_prompt
    desc_normal = f""
    desc_cxg = f"Let's start with the basic information about the input data, which contains metadata about 1 project, corresponding to 1 specified doi, with 1 or more datasets in the project. The 'geo_id' information can be found from the 'link_name' of the 'links' in the 'datasets', note that only the id of the geo is needed for 'geo_id' field, the data of dataset_id should be put into 'other_ids' fields.."
    desc_hca = f"Let's start with the basic information about the input data, which contains metadata about 1 project, corresponding to 1 or more doi."
    desc_scp = f"Let's start with the basic information about the input data, which contains metadata about 1 study. IDs like SCP... can be put into 'other_ids' fields. If the 'name' field contains content, it should be treated as the title of the project."
    return {"normal": desc_normal, "cxg": desc_cxg, "hca": desc_hca, "scp": desc_scp}
