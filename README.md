# pubmed_es

Parse [Pubmed](https://pubmed.ncbi.nlm.nih.gov/) XML into JSON and load it into [Elasticsearch](https://www.elastic.co/guide/index.html). See the [`Downloading Pubmed` documentation](https://dtd.nlm.nih.gov/ncbi/pubmed/doc/out/190101/index.html) for details on obtaining the XML files.

## Installation

`pubmed_es` requires Python 3.7 or higher and Elasticsearch 7. Clone this repo, create a Python `venv`, and install the `requirements.txt`:
```bash
git clone https://github.com/paul-sud/pubmed-es.git
cd pubmed-es
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Usage
From the root of this repo, run the following to read and index the XML, where `DATA_DIR` points to a folder containing the XML files:
```bash
python -m pubmed_es -d $DATA_DIR
```
