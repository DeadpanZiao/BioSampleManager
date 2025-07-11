from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import asyncio
import json
import logging
import math
import os
from typing import Optional, List, Dict, Any

from tqdm import tqdm
from BSM.Fetcher.SingleCellDBs import SingleCellPortalFetcher, ExploreDataFetcher, CellxgeneFetcher
from BSM.Downloader.downloader import Downloader
from BSM.DataController.data_controller import SampleController
from BSM.Processors.ProjectMetadataExtractor import ProjectMetadataExtractor
from BSM.Retriever.vanna_backend import BSMVannaWrapper
import pandas as pd
import uvicorn

app = FastAPI(title="BioSampleManager API", version="1.0.0")

# 全局变量存储控制器和提取器实例
controller = None
extractor = None
vanna_wrapper = None

class DownloadRequest(BaseModel):
    type: str
    database_path: str
    table_name: str
    save_dir: str
    workers: int = 1
    timeout: int = 7200
    dcp: Optional[str] = None
    cookie_path: Optional[str] = None

class FetchRequest(BaseModel):
    database: str
    output_path: str
    domain: Optional[str] = None
    dcp: Optional[str] = None

class ProcessMetadataRequest(BaseModel):
    source: str
    input_path: str
    output_dir: str
    database_path: str
    schema_path: str
    api_url: str = "https://api.openai.com/v1/"
    api_key: str
    model: str = "gpt-4o"
    batch_size: int = 5
    workers: int = 5
    log_file: str = "process.log"

class VannaRequest(BaseModel):
    api_key: str
    db_path: str
    question: str
    table: str = "Sample"
    model: str = "gpt-4o"
    base_url: str = "https://api.openai.com/v1/"

def read_excel_file(file_path: str) -> List[Dict]:
    df = pd.read_excel(file_path, header=0)
    return df.to_dict(orient='records')

@app.post("/init_controller")
async def init_controller(database_path: str):
    """Initialize the sample controller"""
    global controller
    controller = SampleController(database_path)
    return {"message": "Sample controller initialized"}

@app.post("/init_extractor")
async def init_extractor(
    source: str,
    api_url: str,
    api_key: str,
    model: str,
    schema_path: str
):
    """Initialize the metadata extractor"""
    global extractor
    extractor = ProjectMetadataExtractor(
        source,
        api_url,
        api_key,
        model,
        json_schema=read_excel_file(schema_path)
    )
    return {"message": "Metadata extractor initialized"}

@app.post("/init_vanna")
async def init_vanna(
    api_key: str,
    db_path: str,
    model: str = "gpt-4o",
    base_url: str = "https://api.openai.com/v1/"
):
    """Initialize the Vanna wrapper"""
    global vanna_wrapper
    vanna_wrapper = BSMVannaWrapper(
        api_key=api_key,
        db_path=db_path,
        model=model,
        base_url=base_url
    )
    return {"message": "Vanna wrapper initialized"}

@app.post("/download")
async def download_data(request: DownloadRequest):
    """Download data from specified source"""
    downloader_kwargs = {
        'database_path': request.database_path,
        'table_name': request.table_name,
        'save_root': request.save_dir,
        'downloader_type': request.type,
        'num_workers': request.workers,
        'timeout': request.timeout
    }

    if request.type == 'hca' and request.dcp:
        downloader_kwargs['dcp'] = request.dcp
    elif request.type == 'scp' and request.cookie_path:
        try:
            with open(request.cookie_path, 'r') as f:
                downloader_kwargs['cookie'] = json.load(f)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Error reading cookie file: {str(e)}")

    downloader = Downloader(**downloader_kwargs)
    await downloader.main()
    return {"message": "Download completed"}

@app.post("/fetch")
async def fetch_data(request: FetchRequest):
    """Fetch data from specified database"""
    if request.database == 'scp':
        fetcher = SingleCellPortalFetcher(
            domain_name=request.domain if request.domain else "singlecell.broadinstitute.org"
        )
    elif request.database == 'hca':
        fetcher = ExploreDataFetcher(dcp_num=request.dcp)
    elif request.database == 'cxg':
        fetcher = CellxgeneFetcher(
            domain_name=request.domain if request.domain else "cellxgene.cziscience.com/curation/v1"
        )
    else:
        raise HTTPException(status_code=400, detail="Invalid database type")
    
    fetcher.fetch(request.output_path)
    return {"message": f"Data fetched from {request.database} and saved to {request.output_path}"}

@app.post("/process_metadata")
async def process_metadata(request: ProcessMetadataRequest):
    """Process metadata in batches"""
    global extractor, controller
    
    if not extractor:
        extractor = ProjectMetadataExtractor(
            request.source,
            request.api_url,
            request.api_key,
            request.model,
            json_schema=read_excel_file(request.schema_path)
        )
    
    if not controller:
        controller = SampleController(request.database_path)

    # Setup logging
    logging.basicConfig(
        filename=request.log_file,
        level=logging.ERROR,
        format='%(asctime)s:%(levelname)s:%(message)s'
    )

    # Read input data
    with open(request.input_path, 'r', encoding='utf-8') as f:
        input_metadata_list = json.load(f)

    # Process in batches
    batch_size = request.batch_size
    num_batches = math.ceil(len(input_metadata_list) / batch_size)
    sum_token_usage = sum_input_token = sum_output_token = 0
    failed_tasks_all_batches = []
    results = []

    for i in tqdm(range(num_batches), desc="Processing Batches", unit="batch"):
        start_index = i * batch_size
        end_index = min((i + 1) * batch_size, len(input_metadata_list))
        batch = input_metadata_list[start_index:end_index]

        batch_results, failed_tasks = extractor.extract_batch(batch, max_workers=request.workers)

        # Log failed tasks
        for task in failed_tasks:
            task_num = batch_size * i + task + 1
            logging.error(f"Failed task {task} in batch {i + 1}: No {task_num}")
            failed_tasks_all_batches.append(task_num)
        
        os.makedirs(request.output_dir, exist_ok=True)
        
        # Process results
        for j, result in enumerate(batch_results):
            task_id, content = result
            result_data, token_usage = extractor.post_process_data(content)

            # Update token counts
            sum_input_token += token_usage['input_tokens']
            sum_output_token += token_usage['output_tokens']
            sum_token_usage += token_usage['total_tokens']

            # Save result and update database
            original_task_id = start_index + task_id
            result_json_path = f"{request.output_dir}/{request.source}_{original_task_id + 1:06d}.json"
            with open(result_json_path, 'w', encoding='utf-8') as f:
                json.dump(result_data, f, ensure_ascii=False, indent=4)

            res = controller.insert_sample(result_data)
            results.append({
                "task_id": original_task_id,
                "status": res.get("status"),
                "output_path": result_json_path
            })

    return {
        "results": results,
        "failed_tasks": failed_tasks_all_batches,
        "token_usage": {
            "total": sum_token_usage,
            "input": sum_input_token,
            "output": sum_output_token
        }
    }

@app.post("/query_vanna")
async def query_with_vanna(request: VannaRequest):
    """Query database using Vanna AI"""
    global vanna_wrapper
    
    if not vanna_wrapper:
        vanna_wrapper = BSMVannaWrapper(
            api_key=request.api_key,
            db_path=request.db_path,
            model=request.model,
            base_url=request.base_url
        )
    
    sql, df = vanna_wrapper.ask(question=request.question, table=request.table)
    return {
        "sql": sql,
        "data": df.to_dict(orient='records')
    }

@app.get("/")
async def root():
    return {"message": "BioSampleManager API is running"}

# Add this at the bottom of the file
if __name__ == "__main__":
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8080,
        reload=True,
        workers=1
    )

