import argparse
import pandas as pd

def extract():
    df = pd.read_csv('data.csv')
    return df

def transform(argName, df):
    new_df = df[df['Department'] == argName]
    return new_df

def load(new_df):
    new_df.to_csv('new_data.csv')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--department")
    args = parser.parse_args()

    df = extract()
    new_df = transform(args.department, df)
    load(new_df)
    print('ETL is complete.')

if __name__ == "__main__":
    main()