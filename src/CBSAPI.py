
import requests
from requests import Response
from dataclasses import dataclass
import copy
from typing import List, NamedTuple, Union, Iterable
from concurrent.futures import ThreadPoolExecutor
import re
from errors import *

@dataclass
class Download:
    """ handle API request param download state"""
    TRUE = 'true'
    FALSE = 'false'
@dataclass
class Method:
    """
    ### handle the method of request sent to the CBS API\n
    two methods are supported:\n
    `LEVEL` requires a single `id` and optional `subject`\n
    `PATH` requires a path of `ids`\n
    """
    LEVEL='level'
    PATH='path'
class Paging(NamedTuple):
    total_items:int
    page_size: int
    current_page: int
    last_page: int
    first_url: int
    previous_url: Union[str, None]
    current_url: str
    next_url: Union[str, None]
    last_url: str
    base_url: Union[str, None]

class Catalog(NamedTuple):
    path: List[int]
    name: str
    pathDesc: Union[str,None]
class ILCBS_CatalogResponse(NamedTuple):
    catalog: List[Catalog]
    level: int
    paging: Paging

class ILCBS_API:
    """
    ### a Python API for the Israeli Gov Central Bureau of Statistics (IL-CBS)\n
    the API contains different subjects arranged in 5 levels `L1-L5`
    ! currently only JSON format supports advanced processing,\n

    #### Example:
    >>> # initiate the API requires setting both language and format, additional query
    >>> # params can be set later using function: set_general_query_params
    >>> cbs = ILCBS()
    """
    ILCBS_API_URL = "https://apis.cbs.gov.il"

    def __init__(self):
        self.set_general_query_params()
        return None

    def set_general_query_params(self, download: Download=Download.FALSE, page: int=1, page_size: int=100)->None:
        """
        ### set the CBS API general query parameters\n
        `lang` is defaulted to `en`
        `format` is defaulted to `json` -> currently there is no need to support other formats.

        #### parameters:
        `download` defines if the retrieved data will downloaded as a file: `true/false`\n
        `page` current page\n
        `pagesize` (page_size) number of objects to be displayed in a `page` (max 1_000)

        #### example:
        >>> # change the  output format to XML
        >>> cbs.set_general_query_parameters()
        """
        query_params = {"lang": 'en', "format": 'json',
                        "download": download, "page": page, "page_size": page_size}
        self.general_query_params = query_params

    def get_catalog_subjects_by_path(self, _id: Iterable[int], scrape_all_pages:bool=False):
        """
        query the subjects available in the ILCBS API by proding an id path [L1,L2,L3,L4,L5].

        #### Parameters:
        `method` see `CatalogMethod`\n
        `_id` requested subject level (1-5)\n
        `subject` the subject number of the level above it. (required for id > 2)\n
        `scrape_all_pages` return the
        """
        # update the query parameters for a subjects path query
        _params = copy.deepcopy(self.general_query_params)
        _params.update({"id": ','.join(str(item) for item in _id)})
        return self._gather_catalogs(Method.PATH, _params, scrape_all_pages)

    def get_catalog_subjects_by_level(self, _id: int, subject: int, scrape_all_pages:bool=False):
        """
        query the subjects available in the IL CBS API by providing a level `id` and L1 subject.

        #### Parameters:
        `_id` requested subject level (1-5)\n
        `subject` the subject id in L1. (required for id > 2)\n
        `scrape_all_pages` set to `True` if all results are to be returned.
        """
        # update the query parameters for a subjects levels query
        _params = copy.deepcopy(self.general_query_params)
        _params.update({"id": _id})
        if subject:
            _params.update({"subject": subject})
        return self._gather_catalogs(Method.LEVEL, _params, scrape_all_pages)


    def _gather_catalogs(self, method: Method, _params, scrape_all_pages)->List[Catalog]:
        """ gather the `Catalog`s from the subject request"""

        url = f"{self.ILCBS_API_URL}/series/catalog/{method}"
        response = requests.get(url, params = _params)
        catalogs, _, paging = self._process_API_catalog_response(response)
        if scrape_all_pages is True:
            catalogs.extend(self._query_remaining_pages(paging))
        return catalogs

    def _query_remaining_pages(self, paging:Paging, n_workers: int=None)->list[Catalog]:
        """
        query all remaining pages based on a given `Paging`.
        query is multithreaded, number of workers is provided by `n_workers`
        """
        catalogs = []
        workers = n_workers or 4
        args = ((paging.current_url, i) for i in range(2, paging.last_page + 1))
        with ThreadPoolExecutor(max_workers = workers) as executor:
            for response in executor.map(self._request_page, args):
                catalog, *_ = self._process_API_catalog_response(response)
                catalogs.extend(catalog)
        return catalogs


    def _request_page(self, args):
        url, page_num = args
        re.sub('Page=\d+', f"Path={page_num}", url)
        return requests.get(url)
    
    def _process_API_catalog_response(self, response: Response)-> ILCBS_CatalogResponse:
        """Process a JSON response from the IL-CBS API"""
        if response.text == '{"Message":"Error: Series Level Catalog"}':
            raise APIPathNotFound
        res = response.json()['catalogs']
        level = res['level']
        catalog = [Catalog(**subject) for subject in res['catalog']]
        paging = Paging(**res['paging'])
        return (catalog, level, paging)


    def _request_url(url: str):
        """return the response from a general url, used to get results in pages > 1"""
        return requests.get(url)

    def find_phrase_in_subject(self, phrase: str, subject: int, levels: Iterable[int]=None)->List[Catalog]:
        """lookup a `phrase` in a `subject` in `level`
        `level` defaults to iterate over all levels to look in the entire subject catalog"""
        #!!! NOT WORKING YET
        levels = levels or range(2,6)
        subjects = []
        for level in levels:
            res = self.get_catalog_subjects_by_level(level, subject, scrape_all_pages=True)
            subjects.extend(res)
        return subjects