import argparse
import pandas as pd

def extract():
    df = pd.read_csv(r'C:\Users\sudarshan.zunja\Desktop\day2\data.csv')
    return df

def transform(argName, df):
    new_df = df[df['Department'] == argName]
    return new_df

def load(new_df):
    new_df.to_csv('new_data1.csv')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--department")
    args = parser.parse_args()
    print(args)

    df = extract()
    new_df = transform(args.department, df)
    load(new_df)
    print('ETL is complete.')

if __name__ == "__main__":
    main()