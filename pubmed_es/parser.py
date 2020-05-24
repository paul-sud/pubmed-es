import json
from collections import OrderedDict
from gzip import GzipFile
from pathlib import Path
from typing import Any, Dict, List

import xmltodict


def get_documents_from_pubmed_xml(pubmed_xml: Path) -> List[Dict[str, Any]]:
    docs = []

    def item_callback(path, item):
        docs.append(item)
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

    return [ordered_dict_to_dict(i) for i in docs]


def ordered_dict_to_dict(data: OrderedDict) -> Dict[str, Any]:
    return json.loads(json.dumps(data))
