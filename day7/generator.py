import pandas as pd

df = pd.DataFrame()

df['id'] = [i for i in range(700)]
regions = ['Asia', 'Australia', 'South America', 'Europe', 'North America', 'Africa', 'Antartica']
df['region'] = [regions[i % len(regions)] for i in range(700)]
df = df.sample(frac=1).reset_index(drop=True)
print(df.head())

df.to_csv('regions.csv')