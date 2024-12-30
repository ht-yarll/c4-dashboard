import pandas as pd
from pandas_gbq import to_gbq

path = '/home/ht-yarll/Documents/vscode/estudo/py/c4-dashboard/data/raw'
path_hdi = path + 'archive_environment/human_development_index.csv'
path_gnic = path + 'archive_environment/gross_national_income_per_capital.csv'
path_suicide = path + 'archive_suicide/master.csv'