from fastmcp import FastMCP
from BSM.Fetcher.SingleCellDBs import SingleCellPortalFetcher, ExploreDataFetcher, CellxgeneFetcher
from BSM.Downloader.downloader import Downloader
from BSM.DataController.data_controller import SampleController
from BSM.Processors.ProjectMetadataExtractor import ProjectMetadataExtractor
from BSM.Retriever.vanna_backend import BSMVannaWrapper
import pandas as pd
import json
import asyncio
import os

mcp = FastMCP("BioSampleManager Server 🧬")

# 初始化控制器和提取器
controller = None
extractor = None
vanna_wrapper = None

@mcp.tool()
def init_controller(database_path: str) -> str:
    """Initialize the sample controller"""
    global controller
    controller = SampleController(database_path)
    return "Sample controller initialized"

@mcp.tool()
def init_extractor(
    source: str,
    api_url: str,
    api_key: str,
    model: str,
    schema_path: str
) -> str:
    """Initialize the metadata extractor"""
    global extractor
    
    def read_excel_file(file_path):
        df = pd.read_excel(file_path, header=0)
        return df.to_dict(orient='records')
    
    extractor = ProjectMetadataExtractor(
        source,
        api_url,
        api_key,
        model,
        json_schema=read_excel_file(schema_path)
    )
    return "Metadata extractor initialized"

@mcp.tool()
def init_vanna(
    api_key: str,
    db_path: str,
    model: str = "gpt-4o",
    base_url: str = "https://api.openai.com/v1/"
) -> str:
    """Initialize the Vanna wrapper"""
    global vanna_wrapper
    vanna_wrapper = BSMVannaWrapper(
        api_key=api_key,
        db_path=db_path,
        model=model,
        base_url=base_url
    )
    return "Vanna wrapper initialized"

@mcp.tool()
def download_data(
    downloader_type: str,
    database_path: str,
    table_name: str,
    save_dir: str,
    workers: int = 1,
    timeout: int = 7200,
    dcp: str = None,
    cookie_path: str = None
) -> str:
    """Download data from specified source"""
    downloader_kwargs = {
        'database_path': database_path,
        'table_name': table_name,
        'save_root': save_dir,
        'downloader_type': downloader_type,
        'num_workers': workers,
        'timeout': timeout
    }

    if downloader_type == 'hca' and dcp:
        downloader_kwargs['dcp'] = dcp
    elif downloader_type == 'scp' and cookie_path:
        try:
            with open(cookie_path, 'r') as f:
                downloader_kwargs['cookie'] = json.load(f)
        except Exception as e:
            return f"Error reading cookie file: {str(e)}"

    downloader = Downloader(**downloader_kwargs)
    asyncio.run(downloader.main())
    return "Download completed"

@mcp.tool()
def fetch_data(
    database: str,
    output_path: str,
    domain: str = None,
    dcp: str = None
) -> str:
    """Fetch data from specified database"""
    if database == 'scp':
        fetcher = SingleCellPortalFetcher(
            domain_name=domain if domain else "singlecell.broadinstitute.org"
        )
    elif database == 'hca':
        fetcher = ExploreDataFetcher(dcp_num=dcp)
    elif database == 'cxg':
        fetcher = CellxgeneFetcher(
            domain_name=domain if domain else "cellxgene.cziscience.com/curation/v1"
        )
    else:
        return "Invalid database type"
    
    fetcher.fetch(output_path)
    return f"Data fetched from {database} and saved to {output_path}"

@mcp.tool()
def process_metadata_batch(
    input_path: str,
    output_dir: str,
    batch_size: int = 5,
    workers: int = 5
) -> dict:
    """Process metadata in batches"""
    if not extractor or not controller:
        return {"error": "Extractor or controller not initialized"}
    
    with open(input_path, 'r', encoding='utf-8') as f:
        input_metadata_list = json.load(f)
    
    os.makedirs(output_dir, exist_ok=True)
    results = []
    
    num_batches = (len(input_metadata_list) + batch_size - 1) // batch_size
    
    for i in range(num_batches):
        start = i * batch_size
        end = min((i + 1) * batch_size, len(input_metadata_list))
        batch = input_metadata_list[start:end]
        
        batch_results, failed_tasks = extractor.extract_batch(batch, max_workers=workers)
        
        for task_id, content in batch_results:
            result_data, _ = extractor.post_process_data(content)
            
            # Save result
            output_path = f"{output_dir}/batch_{i}_task_{task_id}.json"
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(result_data, f, ensure_ascii=False, indent=4)
            
            # Insert to database
            res = controller.insert_sample(result_data)
            results.append({
                "task_id": start + task_id,
                "status": res.get("status"),
                "output_path": output_path
            })
    
    return {"results": results}

@mcp.tool()
def query_with_vanna(question: str, table: str = "Sample") -> dict:
    """Query database using Vanna AI"""
    if not vanna_wrapper:
        return {"error": "Vanna wrapper not initialized"}
    
    sql, df = vanna_wrapper.ask(question=question, table=table)
    return {
        "sql": sql,
        "data": df.to_dict(orient='records')
    }


if __name__ == "__main__":
    mcp.run(transport="http", host="127.0.0.1", port=8000, path="/mcp")