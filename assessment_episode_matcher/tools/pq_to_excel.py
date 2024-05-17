import pandas as pd

path = "./data/in/atom_20200106-20240317.parquet"


def main():
  df = pd.read_parquet(path)
  df.to_csv("./data/out/atom_20200106-20240317.csv")

if __name__ == '__main__':
  main()