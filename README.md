

# ![Logo](resources/logo_transparent.png)BioSampleManager
Fetch, process and manage metadata and data samples for following databases: 
- [GEO](https://www.ncbi.nlm.nih.gov/geo/)

- [Cellxgene](https://cellxgene.cziscience.com/datasets)

- [Human Cell Atlas (data explorer)](https://explore.data.humancellatlas.org/projects)

- [Broad Institue - single cell portal](https://singlecell.broadinstitute.org/single_cell)

<img src="README/singlecelldb.png" alt="singlecelldb" style="zoom: 50%;" />

## Installation

```
pip install -r requirements.txt
```

## Usage

### Fetchers -- Fetch meta data

```angular2html
# Fetch from Single Cell Portal
python cli.py fetch --database scp --output scp_data.json

# Fetch from Human Cell Atlas
python cli.py fetch --database hca --output hca_data.json

# Fetch from CellxGene
python cli.py fetch --database cxg --output cxg_data.json

```

### Processors -- Alignment 
```angular2html
python cli.py process \
    --source scp \
    --input scp_data.json \
    --output-dir output/processed \
    --database processed_data.db \
    --schema DBS/json_schema.xlsx \
    --api-url your-api-url \
    --api-key your-api-key \
    --model gpt-4o 

# Advanced usage with custom parameters
python cli.py process \
    --source hca \
    --input data/hca_metadata.json \
    --output-dir output/hca_processed \
    --database projects.db \
    --schema custom_schema.json \
    --api-url "https://custom-api.example.com/v1/" \
    --api-key "your-api-key" \
    --model "custom-model" \
    --batch-size 10 \
    --workers 8 \
    --log-file logs/processing.log
```
### Downloaders -- Download samples
```
python cli.py download \
    --type scp \
    --database path/to/database.db \
    --table your_table \
    --save-dir test_downloader \
    --workers 1 \
    --timeout 7200 \
    --cookie path/to/cookie.json
```

  ### Vanna -- Text to SQL

```
python cli.py retrieve \
	--query "What's the title corresponding to GSE204684? The column geo_ids may contain more than one ID." \
	--api-key "your_api_key" \
	--model "gpt-4o" \
	--db-path "path/to/your/xx.db" \
	--table "Sample"
```

### MCP Intergration

**MCP server**

To start a local mcp server, run: 

```
python -m mcp_server.py
```

```
[07/11/25 16:42:56] INFO     Starting MCP server                 server.py:1429
                             'BioSampleManager Server 🧬' with                 
                             transport 'http' on                               
                             http://127.0.0.1:8000/mcp 
```

**MCP client**

Here we use openai-agents as an example. See examples/mcp_demo/ for detail.

```
# linux or mac-os
export OPENAI_API_KEY='your api key'
# windows
$env:OPENAI_API_KEY='your api key'

python -m examples/mcp_demo/openai_agents.py
```

We get the following output:
```
I have several tools within the MCP functionalities. Here they are:

1. **init_controller**: Initialize the sample controller with a given database path.

2. **init_extractor**: Initialize the metadata extractor using specified details like API key and URL.

3. **init_vanna**: Initialize the Vanna wrapper with configurations like API key and model.

4. **download_data**: Download data from a specified source and save it in a designated directory.

5. **fetch_data**: Fetch data from a specified database and save it to an output path.

6. **process_metadata_batch**: Process metadata in batches with provided input paths and output directories.

7. **query_with_vanna**: Query the database using Vanna AI with a specific question and table. 

Do you need details or help using any of these tools?
```



## Evaluation

pass
