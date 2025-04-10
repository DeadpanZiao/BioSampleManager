

# ![Logo](resources/logo_transparent.png)BioSampleManager
Fetch, process and manage metadata and data samples for following databases: 
- [GEO](https://www.ncbi.nlm.nih.gov/geo/)

- [Cellxgene](https://cellxgene.cziscience.com/datasets)

- [Human Cell Atlas (data explorer)](https://explore.data.humancellatlas.org/projects)

- [Broad Institue - single cell portal](https://singlecell.broadinstitute.org/single_cell)

## Fetchers
```angular2html
# Fetch from Single Cell Portal
python cli.py fetch --database scp --output scp_data.json

# Fetch from Human Cell Atlas
python cli.py fetch --database hca --output hca_data.json

# Fetch from CellxGene
python cli.py fetch --database cxg --output cxg_data.json

```

## Processors
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
## Downloaders
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

  
