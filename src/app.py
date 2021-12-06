from CBSAPI import *
import pandas as pd
if __name__ == "__main__":
    cbs = ILCBS_API()
    subjects = cbs.get_catalog_subjects_by_level(12,2, scrape_all_pages=True)
    df = pd.DataFrame(subjects)
    spec = cbs.get_catalog_subjects_by_path([12,1,1], scrape_all_pages=True)
    df2 = pd.DataFrame(spec)
    a=1