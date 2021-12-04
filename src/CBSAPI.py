
import requests
from requests import Response
from dataclasses import dataclass
import copy
from typing import List, NamedTuple, Union
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
from itertools import repeat
import re
@dataclass
class LANG:
    """ handle API request param output language"""
    HE = 'he'
    EN = 'en'

@dataclass
class FORMAT:
    """ handle API request param output format"""
    JSON = 'json'
    XML = 'xml'
    CSV = 'csv'
    XLS = 'xls'

@dataclass
class DOWNLOAD:
    """ handle API request param download state"""
    TRUE = 'true'
    FALSE = 'false'

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
    ILCBS_API_URL = "https://apis.cbs.gov.il"

    def __init__(self,lang:LANG, format: FORMAT):
        """
        ### a Python API for the Israeli Gov Central Bureau of Statistics (IL-CBS)\n
        ! currently only JSON format supports advanced processing,\n
        ! other results would be returned as Response Objects (Not Recommended)
        #### Example:
        >>> # initiate the API requires setting both language and format, additional query
        >>> # params can be set later using function: set_general_query_params
        >>> cbs = ILCBS(lang=LANG.HE, format=FORMAT.JSON)
        """
        self.set_general_query_params(lang, format)
        return None

    def set_general_query_params(self, lang: LANG, format: FORMAT, download: DOWNLOAD=DOWNLOAD.FALSE,
                         page: int=1, page_size: int=100)->None:
        """
        ### set the CBS API general query parameters\n
        #### parameters:
        `lang` supported API languages: `he/eng`\n
        `format` supproted API response formats: `json/xml/csv/xls`\n
        `download` defines if the retrieved data will downloaded as a file: `true/false`\n
        `page` current page\n
        `pagesize` (page_size) number of objects to be displayed in a `page` (max 1_000)
        #### output `None`
        #### example
        >>> # change the  output format to XML
        >>> cbs.set_general_query_parameters(LANG.ENG, FORMAT.XML)
        """
        query_params = {"lang": lang, "format": format, "download": download,
                        "page": page, "page_size": page_size}
        self.general_query_params = query_params
        return None

    def get_catalog_subjects(self, _id: int, subject: int=None, scrape_all_pages=False) -> List[ILCBS_CatalogResponse]:
        """
        ### query the subjects available in the ILCBS API.
        the subjects are arranged in 5 levels `L1-L5`,
        each level includes all the subjects of the levels above it.
        #### Parameters:
        `_id` requested subject levels (1-5)\n
        `subject` the subject number of the level above it. (required for id > 2)\n
        `scrape_all_pages` return the
        """
        _params = copy.deepcopy(self.general_query_params)
        _params.update({"id": _id})
        if subject:
            _params.update({"subject": subject})
        response = requests.get(f"{self.ILCBS_API_URL}/series/catalog/level", params = _params)
        if self.general_query_params['format'] != FORMAT.JSON:
            return list(response)
        else:
            # output is JSON, currently only this response type is supported
            responses = [self._process_API_catalog_response(response)]
            if scrape_all_pages is True:
                paging = responses[0].paging
                workers = paging.last_page - 1
                args = ((paging.current_url, i) for i in range(2, paging.last_page + 1))
                with ThreadPoolExecutor(max_workers = workers) as executor:
                    for response in executor.map(self._request_page, args):
                        responses.append(self._process_API_catalog_response(response))

        return responses

    def _request_page(self, args):
        url, page_num = args
        re.sub('Page=\d+', f"Path={page_num}", url)
        return requests.get(url)
    def _process_API_catalog_response(self, response: Response)-> ILCBS_CatalogResponse:
        # can be later extended to handle the rest of the response formats....
        if self.general_query_params['format'] == FORMAT.JSON:
             res = self._process_API_catalog_response_JSON(response)
        else:
            res = response
        return res

    def _process_API_catalog_response_JSON(self, response: Response):
        """Process a JSON response from the IL-CBS API"""
        res = response.json()['catalogs']
        level = res['level']
        catalog = [Catalog(**subject) for subject in res['catalog']]
        paging = Paging(**res['paging'])
        catalog_response = ILCBS_CatalogResponse(catalog, level, paging)
        return catalog_response


    def _request_url(url: str):
        """return the response from a general url, used to get results in pages > 1"""
        return requests.get(url)
    
if __name__ == "__main__":
    cbs = ILCBS_API(LANG.EN, FORMAT.JSON)
    cbs.get_catalog_subjects(5,12, scrape_all_pages=True)
    