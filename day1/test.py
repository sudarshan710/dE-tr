import pandas as pd 
import time

df = pd.read_csv('hw_200.csv')
st = time.time()
print(df.shape, time.time()-st)

df2 = pd.read_csv('hw_200.csv', chunksize=1)
st = time.time()
print(df.shape, time.time()-st)

## Parquet with pyarrow/fastparquet