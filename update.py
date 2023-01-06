import pandas as pd

import local_io as io
import yahoo

df_stock: pd.DataFrame = io.load_stock_master()

for code in df_stock.index:
    print(code, end="")
    yahoo.make_hist(code, "T")
    print("done")

pass
