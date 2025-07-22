import pandas as pd


df = pd.read_parquet(r'C:\Users\sudarshan.zunja\Desktop\dE-tr\day7\regions\part-00000-15beb71a-96c6-4ca4-93a7-3efb6b13a02f-c000.snappy.parquet')

print(df.head())