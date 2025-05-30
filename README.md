

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



## Evaluation

#### task 1 Data entry accuracy 数据入库准确率

对给定的json格式存储的meta数据进行清洗存入数据库，再读取，恢复为原 json，对指定的 json key进行比较，如一致则为成功

| 指标及说明  | 正确率 - accuracy (入库正确的样本/成功入库样本数) | 失败率 - failure rate (入库失败，如json格式解析错误，不合法字符串引起的入库失败样本/总样本数) |
| ----------- | ------------------------------------------------- | ------------------------------------------------------------ |
| kimi        |                                                   |                                                              |
| qwen        |                                                   |                                                              |
| gpt4-o      |                                                   |                                                              |
| deepseek v3 |                                                   |                                                              |

数据源：不需要额外标注数据源，可直接用fetcher获得的数据测试

| 数据源                                                       | 样本数 |
| ------------------------------------------------------------ | ------ |
| [Cellxgene](https://cellxgene.cziscience.com/datasets)       | 100    |
| [Human Cell Atlas (data explorer)](https://explore.data.humancellatlas.org/projects) | 100    |
| [Broad Institue - single cell portal](https://singlecell.broadinstitute.org/single_cell) | 100    |

#### task 2 Data Cleaning Quality Assessment

对齐后的样本与人工标注样本比较

| 指标及说明  | Field-level accuracy (入库正确的itm/总item) | **Overall Sample Accuracy** (正确入库的行/总行数) | missing rate (模型未填写的item/总item) |
| ----------- | ------------------------------------------- | ------------------------------------------------- | -------------------------------------- |
| kimi        |                                             |                                                   |                                        |
| qwen        |                                             |                                                   |                                        |
| gpt4-o      |                                             |                                                   |                                        |
| deepseek v3 |                                             |                                                   |                                        |

已收集人类标注样本共63个

| 数据源                                                       | 样本数 |
| ------------------------------------------------------------ | ------ |
| [Cellxgene](https://cellxgene.cziscience.com/datasets)       | 21     |
| [Human Cell Atlas (data explorer)](https://explore.data.humancellatlas.org/projects) | 21     |
| [Broad Institue - single cell portal](https://singlecell.broadinstitute.org/single_cell) | 21     |

#### task 3 Vanna text to sql 准确率

设计100个检索任务（query, 如 What's the title corresponding to GSE204684?，可以把列名跟描述给模型，让模型提出query），判断返回值是否符合预期

|             | sql 生成率 (生成sql数/query数) | sql可执行率（可成功执行sql/query数） | 成功率（执行sql返回结果符合预期/总query) |
| ----------- | ------------------------------ | ------------------------------------ | ---------------------------------------- |
| kimi        |                                |                                      |                                          |
| qwen        |                                |                                      |                                          |
| gpt4-o      |                                |                                      |                                          |
| deepseek v3 |                                |                                      |                                          |
