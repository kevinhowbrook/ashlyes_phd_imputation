import pandas as pd

data_file = 'testing.csv'

df = pd.read_csv(data_file, low_memory=False)


df.to_stata('data/out_imputed.dta')