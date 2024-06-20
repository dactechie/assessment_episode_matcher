import pandas as pd
from enum import Enum
from assessment_episode_matcher.importers.main import LocalFileSource, FileSource
from assessment_episode_matcher.utils.dtypes import convert_to_datetime
# from assessment_episode_matcher.importers import episodes as EpisodesImporter

class NADAStrEnum(Enum):
  EpisodeID = "EpisodeID"
  ClientCode = "ClientCode"
  ClientType = "ClientType"
  Comm_Date = "Comm_Date"
  End_Date = "End_Date"
  Main_Treatment = "Main_Treatment"
  Principal_Drug = "Principal_Drug"
  SLK = "SLK"

def get_cols_of_interest() -> list[str]:
  cols = [attr.value for attr in NADAStrEnum]
  return cols
# cols = "EpisodeID,ClientCode,ClientType,Comm_Date,End_Date,Main_Treatment,Principal_Drug,SLK"
r1 = "430022,2149,Own drug use,04/11/2021,08/04/2022,Support and case management only,Cannabinoids,HILER040519981" 

#  ",Left Involuntarily (non-compliance),

# "427945,5048,Own drug use,30/09/2021,01/10/2021"
#   ",Service completed,
# Support and case management only,Methamphetamine (incl. Speed - Ice),OHSAN140919961"



  

def prepare(ep_df1:pd.DataFrame) -> pd.DataFrame:
  # processed_folder = Bootstrap.get_path("processed_dir")
  cols = get_cols_of_interest()
  ep_df = ep_df1[cols].copy()
  ep_df['Program'] = ep_df1.attrs['Program']
  
#  convert_to_datetime(atom_df['AssessmentDate'], format='%Y%m%d')
  ep_df[NADAStrEnum.Comm_Date.value] = convert_to_datetime(ep_df[NADAStrEnum.Comm_Date.value],  format='%d%m%Y')
                                                  # , fill_blanks=False)
  ep_df[NADAStrEnum.End_Date.value] = convert_to_datetime(ep_df[NADAStrEnum.End_Date.value],  format='%d%m%Y')

  # ep_df.rename(columns=EpCfg.rename_columns
  #           , inplace=True)
  
  # file_path =  processed_folder.joinpath(f"MDS_{start_date}-{end_date}_AllPrograms.parquet")
  
  # io.write_parquet(ep_df, file_path)
  return ep_df

def import_data(eps_st:str,  eps_end:str, file_source:FileSource
                    , prefix:str, suffix:str) -> tuple  [pd.DataFrame, str|None]:
  # file_path, best_start_date, best_end_date = io.load_for_period(
  #                       file_source 
  #                       , eps_st
  #                       , eps_end
  #                       ,prefix=f"{prefix}_"
  #                         , suffix=f"{suffix}.csv"
  #                       )
  # if not file_path:
  #   raise FileNotFoundError("No MDS file was found")

  file_path = f"{prefix}_{eps_st}-{eps_end}_{suffix}.csv"
  raw_df = file_source.load_csv_file_to_df(file_path, dtype=str)
  raw_df.attrs['Program'] = suffix
  return raw_df, None


def main():
  # nada_eps_df = get_nada_export()
  base_path = "data/in"
  # nada_csv = "NADAProd_2010-20240531_MURMPP.csv"
  fs = LocalFileSource(base_path)

  nada_eps_df, _ = import_data(eps_st="2010", eps_end="20240531", file_source=fs, prefix="NADAProd", suffix="MURMPP")

  nada = prepare(nada_eps_df)
  # eps_csv = "MDS_20230701-20231231_AllPrograms.csv"
  # fs = LocalFileSource(base_path)
  # episode_df = fs.load_csv_file_to_df(eps_csv, dtype=str)
  # eps_df = EpisodesImporter.prepare(episode_df)
  # common = find_clients(eps_df, nada_eps_df)
  print(nada.head())


if __name__ == '__main__':
  main()
  

  
# def get_nada_export():
#   data = [r1.split(",")]
#   df = pd.DataFrame(data, columns=cols)
#   return df

# def find_clients(eps_df:pd.DataFrame, nada_eps_df:pd.DataFrame) -> pd.DataFrame:
#   eps_df = eps_df.copy()
#   eps_df["SLK"] = eps_df["SLK"].str.strip()
#   nada_eps_df["SLK"] = nada_eps_df["SLK"].str.strip()
#   eps_df_nada_merge = eps_df.merge(nada_eps_df, on="SLK", how="left", suffixes=("", "_nada"))
#   return eps_df_nada_merge
