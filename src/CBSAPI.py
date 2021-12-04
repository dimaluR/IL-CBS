
import requests
from requests import Response
from dataclasses import dataclass
import copy
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


class ILCBS_API:
    ILCBS_API_URL = "https://apis.cbs.gov.il"

    def __init__(self,lang:LANG, format: FORMAT):
        """
        ### a Python API for the Israeli Gov Central Bureau of Statistics (ILCBS)
        #### Example:
        >>> # initiate the API requires setting both language and format, additional query
        >>> # params can be set later using function: set_general_query_params
        >>> cbs = ILCBS(lang=LANG.HE, format=FORMAT.JSON)
        """
        self.set_general_query_params(lang, format)
        pass

    def set_general_query_params(self, lang: LANG, format: FORMAT, download: DOWNLOAD=DOWNLOAD.FALSE,
                         page: int=1, pagesize: int=100)->None:
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
                        "page": page, "pagesize": pagesize}
        self.general_query_params = query_params
        pass

    def get_subjects(self, _id: int, subject: int=None) -> Response:
        """
        ### query the subjects available in the ILCBS API.
        the subjects are arranged in 5 levels `L1-L5`,
        each level includes all the subjects of the levels above it.
        #### Parameters:
        `_id` requested subject levels (1-5)
        `subject` the subject number of the level above it. (required for id > 2)
        """
        _params = copy.deepcopy(self.general_query_params)
        _params.update({"id": _id})
        if subject:
            _params.update({"subject": subject})
        return requests.get(f"{self.ILCBS_API_URL}/series/catalog/level", params = _params)

if __name__ == "__main__":
    cbs = ILCBS_API(LANG.EN, FORMAT.JSON)
    cbs.get_subjects(1)
    