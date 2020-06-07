from __future__ import annotations

from collections import OrderedDict
from datetime import date
from gzip import GzipFile
from pathlib import Path
from typing import Any, Dict, Iterator, List, Tuple

import xmltodict


def get_documents_from_pubmed_xmls(data_dir: Path) -> Iterator[Dict[str, Any]]:
    """
    Implemented as iterator for low memory overhead. Yields individual pubmed documents
    """
    for document_path in data_dir.glob("*.xml.gz"):
        for document in get_documents_from_pubmed_xml(document_path):
            yield document


def get_documents_from_pubmed_xml(pubmed_xml: Path) -> List[Dict[str, Any]]:
    """
    Takes a `pathlib.Path` pointing to a gzipped pubmed XML file and returns a list of
    processed documents. The articles are recursively flattened and ready for indexing
    into Elasticsearch, with the `_id` set to the article's PMID.
    """
    docs = []
    date_fields = ("DateCompleted", "DateRevised")
    ignore_keys = ("History",)
    force_list = ("MeshHeading", "Chemical", "Author")

    # too lazy to figure out what types these are supposed to be from xmltodict source
    def item_callback(path, item):  # type: ignore
        flattened = selectively_flatten_dict(item, date_fields, ignore_keys)
        flattened["_id"] = flattened["pmid"]
        docs.append(flattened)
        return True

    xmltodict.parse(
        GzipFile(pubmed_xml),
        force_list=force_list,
        item_depth=2,
        item_callback=item_callback,
    )

    return docs


def _selectively_flatten_dict(
    data: OrderedDict[str, Any],
    date_fields: Tuple[str, ...],
    ignore_keys: Tuple[str, ...],
) -> Iterator[Tuple[str, Any]]:
    for k, v in data.items():
        if k in ignore_keys:
            continue
        elif k.endswith("YN"):
            yield pythonify_key(k, strip_yn=True), yn_to_bool(v)
        elif k == "PMID":
            yield pythonify_key(k), v["#text"]
        elif isinstance(v, OrderedDict):
            if k in date_fields:
                yield pythonify_key(k), pubmed_date_to_str(v)
            else:
                yield from _selectively_flatten_dict(v, date_fields, ignore_keys)
        elif isinstance(v, list):
            new_val = []
            for item in v:
                if isinstance(item, OrderedDict):
                    new_val.append(
                        {
                            k: v
                            for k, v in _selectively_flatten_dict(
                                item, date_fields, ignore_keys
                            )
                        }
                    )
                else:
                    new_val.append(item)
            yield pythonify_key(k, pluralize=True), new_val
        else:
            yield pythonify_key(k), v


def selectively_flatten_dict(
    data: OrderedDict[str, Any], date_fields: Tuple[str], ignore_keys: Tuple[str]
) -> Dict[str, Any]:
    """
    Does the work to recusively traverse the XML and yield (key, value) pairs when
    appropriate. Certain keys need special handling. `date_fields` indicates keys with
    dict values that should be converted into a single ISO format date string.
    `ignore_keys` indicates keys that should be entirely ignored during parsing and
    are not recursed either.

    Key value pairs like `"@MajorTopicYN": "Y"` are converted into booleans e.g.
    `"major_topic": true`.

    PMID is special cased, in XML it looks like `<PMID @Version=1>1</PMID>` which upon
    deserializing becomes `OrderedDict([('@Version', '1'), ('#text', '1')])`, and here
    is converted to a simple `"pmid": "1"`.
    """
    return {k: v for k, v in _selectively_flatten_dict(data, date_fields, ignore_keys)}


def pythonify_key(key: str, pluralize: bool = False, strip_yn=False) -> str:
    if key == "UI":
        return "id"
    new_key = ""
    found_start = False
    previous_char_was_uppercase = False
    for char in key:
        if not char.isalnum():
            continue
        elif char.isupper():
            if not found_start:
                new_key += char.lower()
                found_start = True
            else:
                if previous_char_was_uppercase:
                    new_key += char.lower()
                else:
                    new_key += "_" + char.lower()
            previous_char_was_uppercase = True
        else:
            new_key += char
            found_start = True
            previous_char_was_uppercase = False
    if pluralize:
        new_key += "s"
    if strip_yn:
        new_key = new_key[:-3]
    return new_key


def pubmed_date_to_str(data: OrderedDict[str, str]) -> str:
    return date(int(data["Year"]), int(data["Month"]), int(data["Day"])).isoformat()


def yn_to_bool(yn: str) -> bool:
    if yn == "Y":
        return True
    if yn == "N":
        return False
    raise ValueError(f"Found odd YN {yn}")
