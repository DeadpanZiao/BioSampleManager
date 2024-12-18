import json
import logging
import math

from tqdm import tqdm

from BSM.DataController.data_controller import SampleController
from BSM.Processors.ProjectMetadataExtractor import ProjectMetadataExtractor, source_info
import pandas as pd


def read_json_file(file_path):
    with open(file_path, 'r', encoding="utf-8") as file:
        data = json.load(file)
    return data


def read_excel_file(file_path):
    df = pd.read_excel(file_path, header=0)
    data = df.to_dict(orient='records')
    return data


def save_json_file(data, file_path):
    with open(file_path, 'w', encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=4)


def generate_json_name(data_source, current_number):
    json_name = f"{data_source}_{current_number:06d}"
    return json_name


def main():
    API_URL = "https://api.moonshot.cn/v1/"
    API_KEY = ""
    MODEL = "moonshot-v1-128k"

    data_source = 'cxg'
    for item in source_info:
        if item["type"] == data_source:
            file_name = item["file_name"]
            break
    input_metadata_list = read_json_file(rf"D:\projects\BSM\jsons\{data_source}.json")
    json_schema = read_excel_file("../../DBS/json_schema.xlsx")
    extractor = ProjectMetadataExtractor(data_source, API_URL, API_KEY, MODEL, json_schema)

    db_name = '../../DBS/projects.db'
    controller = SampleController(db_name)
    logging.basicConfig(filename='log.txt', level=logging.ERROR, format='%(asctime)s:%(levelname)s:%(message)s')
    # batch execution
    batch_size = 5
    num_batches = math.ceil(len(input_metadata_list) / batch_size)
    sum_token_usage = 0
    sum_input_token = 0
    sum_output_token = 0
    failed_tasks_all_batches = []
    for i in tqdm(range(num_batches), desc="Batch Tasks", unit="batch"):
        start_index = i * batch_size
        end_index = min((i + 1) * batch_size, len(input_metadata_list))
        batch = input_metadata_list[start_index:end_index]

        results, failed_tasks = extractor.extract_batch(batch, max_workers=5)
        print(f"Failed tasks of {data_source} (batch {i + 1}): {failed_tasks}")
        for task in failed_tasks:
            logging.error(f"Failed task {task} in batch {i + 1}: No {batch_size * i + task + 1}")
            failed_tasks_all_batches.append(batch_size * i + task + 1)
        for j, result in enumerate(results):
            task_id = result[0]
            content = result[1]
            result_data, token_usage = extractor.post_process_data(content)
            sum_input_token += token_usage['input_tokens']
            sum_output_token += token_usage['output_tokens']
            sum_token_usage += token_usage['total_tokens']

            original_task_id = start_index + task_id
            result_json_path = rf"D:\programs\BioSampleManager\Bio_Data\qwen_output\{generate_json_name(data_source, original_task_id + 1)}.json"
            save_json_file(result_data, result_json_path)
            res = controller.insert_sample(result_data)
            print(f'task {original_task_id} status: {res.get("status")}')
    print("All failed tasks (original numbers):", failed_tasks_all_batches)
    print(f"total token usage: {sum_token_usage}, input token usage: {sum_input_token}, output token usage: {sum_output_token}")


if __name__ == "__main__":
    main()
