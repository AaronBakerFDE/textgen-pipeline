import pandas as pd
from .utils import load_csv, save_df

def bronze_ingest(api1_path: str, api2_path: str) -> pd.DataFrame:
    df1 = load_csv(api1_path)
    df2 = load_csv(api2_path)
    combined = pd.merge(df1, df2, on ="name", how = "inner")
    return combined
