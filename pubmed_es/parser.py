import json
from collections import OrderedDict
from gzip import GzipFile
from pathlib import Path
from typing import Any, Dict, Iterator, List, Tuple

import xmltodict


def get_documents_from_pubmed_xmls(
    document_paths: List[Path]
) -> Iterator[Dict[str, Any]]:
    """
    Implemented as iterator for low memory overhead. Yields individual pubmed documents
    """
    for document_path in document_paths:
        for document in get_documents_from_pubmed_xml(document_path):
            yield document


def get_documents_from_pubmed_xml(pubmed_xml: Path) -> List[Dict[str, Any]]:
    docs = []
    date_fields = ("DateCompleted", "DateRevised")
    ignore_keys = ("History",)

    # too lazy to figure out what types these are supposed to be from xmltodict source
    def item_callback(path, item):  # type: ignore
        unordered_item = ordered_dict_to_dict(item)
        flattened = selectively_flatten_dict(unordered_item, date_fields, ignore_keys)
        docs.append(flattened)
        return True

    xmltodict.parse(
        GzipFile(pubmed_xml),
        force_list=("MeshHeading", "Chemical"),
        item_depth=2,
        item_callback=item_callback,
    )

    return docs


def ordered_dict_to_dict(data: OrderedDict) -> Dict[str, Any]:
    return json.loads(json.dumps(data))


def _selectively_flatten_dict(
    data: Dict[str, Any], date_fields: Tuple[str, ...], ignore_keys: Tuple[str, ...]
) -> Iterator[Tuple[str, Any]]:
    for k, v in data.items():
        if k in ignore_keys:
            continue
        # elif k == "@MajorTopicYN":
        elif k.endswith("YN"):
            yield pythonify_key(k, strip_yn=True), yn_to_bool(v)
        elif isinstance(v, dict):
            if k in date_fields:
                yield pythonify_key(k), pubmed_date_to_str(v)
            else:
                yield from _selectively_flatten_dict(v, date_fields, ignore_keys)
        elif isinstance(v, list):
            new_val = []
            for item in v:
                if isinstance(item, dict):
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
    data: Dict[str, Any], date_fields: Tuple[str], ignore_keys: Tuple[str]
) -> Dict[str, Any]:
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


def pubmed_date_to_str(data: Dict[str, str]) -> str:
    return "-".join([data["Year"], data["Month"], data["Day"]])


def yn_to_bool(yn: str) -> bool:
    if yn == "Y":
        return True
    if yn == "N":
        return False
    raise ValueError(f"Found odd YN {yn}")
