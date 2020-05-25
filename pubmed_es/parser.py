import json
from collections import OrderedDict
from gzip import GzipFile
from pathlib import Path
from typing import Any, Dict, Iterator, List, Tuple

import xmltodict


def get_documents_from_pubmed_xml(pubmed_xml: Path) -> List[Dict[str, Any]]:
    docs = []

    def item_callback(path, item):
        unordered_item = ordered_dict_to_dict(item)
        flattened = selectively_flatten_dict(unordered_item)
        docs.append(flattened)
        return True

    xmltodict.parse(
        GzipFile(pubmed_xml),
        force_list=(
            "PubmedArticleSet",
            "AuthorList",
            "PublicationTypeList",
            "ChemicalList",
            "ArticleIdList",
            "MeshHeadingList",
            "History",
        ),
        item_depth=1,
        item_callback=item_callback,
    )

    return docs


def ordered_dict_to_dict(data: OrderedDict) -> Dict[str, Any]:
    return json.loads(json.dumps(data))


def _selectively_flatten_dict(data: Dict[str, Any]) -> Iterator[Tuple[str, Any]]:
    for k, v in data.items():
        if isinstance(v, dict):
            yield from _selectively_flatten_dict(v)
        elif isinstance(v, list):
            new_val = []
            for item in v:
                if isinstance(item, dict):
                    new_val.append({k: v for k, v in _selectively_flatten_dict(item)})
                else:
                    new_val.append(item)
            yield pythonify_key(k), new_val
        else:
            yield pythonify_key(k), v


def selectively_flatten_dict(data: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in _selectively_flatten_dict(data)}


def pythonify_key(key: str) -> str:
    new_key = ""
    found_start = False
    for char in key:
        if not char.isalnum():
            continue
        elif char.isupper():
            if found_start is False:
                new_key += char.lower()
                found_start = True
            else:
                new_key += "_" + char.lower()
        else:
            new_key += char
            found_start = True
    return new_key
