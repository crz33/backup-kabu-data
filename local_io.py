import os

import pandas as pd

HIST_PATH = "./data/hist/{code}.parquet"


def load_hist(code):
    file_path = HIST_PATH.format(code=code)
    if os.path.exists(file_path):
        return pd.read_parquet(file_path)
    else:
        return None


def save_hist(code, df: pd.DataFrame):
    file_path = HIST_PATH.format(code=code)
    df.to_parquet(file_path)
