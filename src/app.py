from CBSAPI import *
import pandas as pd
if __name__ == "__main__":
    cbs = ILCBS_API()
    subjects = cbs.get_catalog_subjects_by_level(2, 2, scrape_all_pages=True)
    df = pd.DataFrame(subjects)
    spec = cbs.get_catalog_subjects_by_path([2,1,1], scrape_all_pages=True)
    df2 = pd.DataFrame(spec)
    a1 = cbs.find_phrase_in_subject("Wheat", 12)
    a=1