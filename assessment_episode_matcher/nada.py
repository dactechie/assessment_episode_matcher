from pathlib import Path
import pandas as pd
from assessment_episode_matcher.data_prep import prep_dataframe_nada
from assessment_episode_matcher.exporters import NADAbase as out_exporter


def generate_nada_export(matched_assessments:pd.DataFrame, outfile:Path):
    res, warnings_aod = prep_dataframe_nada(matched_assessments)

    st = out_exporter.generate_finaloutput_df(res)
    # st.to_parquet('/data/out/surveytxt.parquet')
    st.to_csv(outfile, index=False)
        
    return st
    

def main():
  reporting_start_str, reporting_end_str =  '20220101', '20240331'
  p_str = f"{reporting_start_str}-{reporting_end_str}"
  nada_importfile:Path = Path("data/out") / \
                           f"{p_str}_surveytxt.csv"
  
  df_reindexed = pd.read_parquet(f"data/out/{p_str}_reindexed.parquet")

  nada = generate_nada_export(df_reindexed, outfile=nada_importfile)
  print(nada)


if __name__ == "__main__":
  res = main()