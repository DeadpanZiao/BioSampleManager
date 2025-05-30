import argparse
import asyncio
import json
import logging
import math
import os

from tqdm import tqdm

from BSM.Fetcher.SingleCellDBs import SingleCellPortalFetcher
from BSM.Fetcher.SingleCellDBs import ExploreDataFetcher
from BSM.Fetcher.SingleCellDBs import CellxgeneFetcher
from BSM.Downloader.downloader import Downloader
from BSM.DataController.data_controller import SampleController
from BSM.Processors.ProjectMetadataExtractor import ProjectMetadataExtractor, source_info
import pandas as pd
from BSM.Retriever.vanna_backend import BSMVannaWrapper


def read_excel_file(file_path):
    df = pd.read_excel(file_path, header=0)
    data = df.to_dict(orient='records')
    return data


def process_metadata(args):
    # Setup logging
    logging.basicConfig(
        filename=args.log_file,
        level=logging.ERROR,
        format='%(asctime)s:%(levelname)s:%(message)s'
    )

    # Initialize extractor and controller
    extractor = ProjectMetadataExtractor(
        args.source,
        args.base_url,  # 使用统一的 base-url
        args.api_key,
        args.model,
        json_schema=read_excel_file(args.schema)
    )
    controller = SampleController(args.database)

    # Read input data
    logging.info('loading')
    with open(args.input, 'r', encoding='utf-8') as f:
        input_metadata_list = json.load(f)

    # Process in batches
    batch_size = args.batch_size
    num_batches = math.ceil(len(input_metadata_list) / batch_size)
    sum_token_usage = sum_input_token = sum_output_token = 0
    failed_tasks_all_batches = []

    for i in tqdm(range(num_batches), desc="Processing Batches", unit="batch"):
        start_index = i * batch_size
        end_index = min((i + 1) * batch_size, len(input_metadata_list))
        batch = input_metadata_list[start_index:end_index]

        results, failed_tasks = extractor.extract_batch(batch, max_workers=args.workers)

        # Log failed tasks
        for task in failed_tasks:
            task_num = batch_size * i + task + 1
            logging.error(f"Failed task {task} in batch {i + 1}: No {task_num}")
            failed_tasks_all_batches.append(task_num)
        os.makedirs(args.output_dir, exist_ok=True)
        # Process results
        for j, result in enumerate(results):
            task_id, content = result
            result_data, token_usage = extractor.post_process_data(content)

            # Update token counts
            sum_input_token += token_usage['input_tokens']
            sum_output_token += token_usage['output_tokens']
            sum_token_usage += token_usage['total_tokens']

            # Save result and update database
            original_task_id = start_index + task_id
            result_json_path = f"{args.output_dir}/{args.source}_{original_task_id + 1:06d}.json"
            with open(result_json_path, 'w', encoding='utf-8') as f:
                json.dump(result_data, f, ensure_ascii=False, indent=4)

            res = controller.insert_sample(result_data)
            print(f'Task {original_task_id} status: {res.get("status")}')

    # Print summary
    print("Failed tasks (original numbers):", failed_tasks_all_batches)
    print(f"Token usage - Total: {sum_token_usage}, Input: {sum_input_token}, Output: {sum_output_token}")


def retrieve_query(args):
    wrapper = BSMVannaWrapper(
        api_key=args.api_key,
        db_path=args.db_path,
        model=args.model,
        base_url=args.base_url
    )
    sql, df = wrapper.ask(question=args.query, table=args.table)
    print("Generated SQL Query:")
    print(sql)
    print("\nResult DataFrame:")
    print(df.to_numpy().tolist())


def main():
    parser = argparse.ArgumentParser(description='Single Cell Data Management Tool')
    parser.add_argument('--version', action='version', version='Data Management CLI 1.0.0')

    # 创建通用参数组
    common_parser = argparse.ArgumentParser(add_help=False)
    common_parser.add_argument('--api-key', required=True, help='API key for the language model')
    common_parser.add_argument('--model', default='gpt-4o', help='Language model name')
    common_parser.add_argument('--base-url', default='https://api.openai.com/v1/',
                               help='API base URL for the language model')

    # Create main subparsers for different modules
    subparsers = parser.add_subparsers(dest='module', help='Available modules')

    # Download module
    download_parser = subparsers.add_parser('download', help='Download management')
    download_parser.add_argument('--type', choices=['hca', 'scp', 'cxg'], required=True,
                                 help='Downloader type (hca, scp or cxg)')
    download_parser.add_argument('--database', required=True, help='Database path')
    download_parser.add_argument('--table', required=True, help='Table name')
    download_parser.add_argument('--save-dir', required=True, help='Save directory')
    download_parser.add_argument('--workers', type=int, default=1, help='Number of parallel downloads')
    download_parser.add_argument('--timeout', type=int, default=7200, help='Download timeout in seconds')
    download_parser.add_argument('--dcp', help='DCP value for HCA downloader')
    download_parser.add_argument('--cookie', help='Cookie file path (JSON format) for SCP downloader')

    # Fetch module
    fetch_parser = subparsers.add_parser('fetch', help='Data fetching')
    fetch_parser.add_argument('--database', choices=['scp', 'hca', 'cxg'], required=True,
                              help='Database to fetch from (scp: Single Cell Portal, hca: Human Cell Atlas, cxg: CellxGene)')
    fetch_parser.add_argument('--output', required=True, help='Output JSON file path')
    fetch_parser.add_argument('--domain', help='Custom domain name (optional)')
    fetch_parser.add_argument('--dcp', help='DCP server address (optional)')

    # Add metadata processing module
    process_parser = subparsers.add_parser('process', help='Process metadata', parents=[common_parser])
    process_parser.add_argument('--source', required=True, choices=['scp', 'hca', 'cxg'],
                                help='Source database type')
    process_parser.add_argument('--input', required=True,
                                help='Input JSON file containing metadata')
    process_parser.add_argument('--output-dir', required=True,
                                help='Output directory for processed JSON files')
    process_parser.add_argument('--database', required=True,
                                help='SQLite database path')
    process_parser.add_argument('--schema', required=True,
                                help='JSON schema file path')
    process_parser.add_argument('--batch-size', type=int, default=5,
                                help='Number of items to process in each batch')
    process_parser.add_argument('--workers', type=int, default=5,
                                help='Number of parallel workers')
    process_parser.add_argument('--log-file', default='process.log',
                                help='Log file path')

    # Add Vanna query module
    vanna_parser = subparsers.add_parser('vanna', help='Query database using Vanna AI', parents=[common_parser])
    vanna_parser.add_argument('--db-path', required=True, help='SQLite database path')
    vanna_parser.add_argument('--question', required=True, help='Question to ask the database')
    vanna_parser.add_argument('--table', default='Sample', help='Table name to query')

    # Add retrieve module
    retrieve_parser = subparsers.add_parser('retrieve', help='Retrieve query using Vanna AI', parents=[common_parser])
    retrieve_parser.add_argument('--query', required=True, help='Query question to ask the database')
    retrieve_parser.add_argument('--db-path', required=True, help='SQLite database path')
    retrieve_parser.add_argument('--table', default='Sample', help='Table name to query')

    args = parser.parse_args()

    try:
        if args.module == 'download':
            downloader_kwargs = {
                'database_path': args.database,
                'table_name': args.table,
                'save_root': args.save_dir,
                'downloader_type': args.type,
                'num_workers': args.workers,
                'timeout': args.timeout
            }

            if args.type == 'hca' and args.dcp:
                downloader_kwargs['dcp'] = args.dcp
            elif args.type == 'scp' and args.cookie:
                try:
                    with open(args.cookie, 'r') as f:
                        downloader_kwargs['cookie'] = json.load(f)
                except (json.JSONDecodeError, FileNotFoundError) as e:
                    print(f"Error reading cookie file: {e}")
                    return 1

            # Create and run downloader
            downloader = Downloader(**downloader_kwargs)
            asyncio.run(downloader.main())

        elif args.module == 'fetch':
            if args.database == 'scp':
                fetcher = SingleCellPortalFetcher(
                    domain_name=args.domain if args.domain else "singlecell.broadinstitute.org",
                )
                fetcher.fetch(args.output)

            elif args.database == 'hca':
                fetcher = ExploreDataFetcher(dcp_num=args.dcp)
                fetcher.fetch(args.output)

            elif args.database == 'cxg':
                fetcher = CellxgeneFetcher(
                    domain_name=args.domain if args.domain else "cellxgene.cziscience.com/curation/v1"
                )
                fetcher.fetch(args.output)

        elif args.module == 'process':
            process_metadata(args)

        elif args.module == 'retrieve':
            retrieve_query(args)

        else:
            parser.print_help()

    except Exception as e:
        print(f"Error: {str(e)}")
        return 1

    return 0


if __name__ == '__main__':
    exit(main())
