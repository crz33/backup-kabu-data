import time
from datetime import datetime

import bs4
import pandas as pd
import requests
from dateutil import relativedelta

import local_io as io

HIST_URL = "https://finance.yahoo.co.jp/quote/{code}.{market}/history?from={start}&to={end}&timeFrame=d&page={page}"

# 日付レンジ
RANGE_MAX = datetime.now()
RANGE_MIN = RANGE_MAX + relativedelta.relativedelta(years=-1)
PARAM_TO = datetime.strftime(RANGE_MAX, "%Y%m%d")
PARAM_FROM = datetime.strftime(RANGE_MIN, "%Y%m%d")


def make_hist(code: str, market: str):
    """
    株価データを最新化する
    """

    # 取得済みのデータを読むが、とりあえずテストでサイトから読む
    df_local = io.load_hist(code)

    # 最新データを取る
    page = 0
    for page in range(1, 100):

        # １ページ分取得
        print(".", end="", flush=True)
        time.sleep(0.1)
        df = get_hist(code, market, page)

        # 終了: ページに時系列がなくなったら
        if df is None:
            break

        if df_local is None:
            # 初めてのデータなら追加して次ページへ
            df_local = df
        else:
            # 結合して重複チェック
            df_local = pd.concat([df_local, df])
            del df
            index_duplicated = df_local.index.duplicated()
            if index_duplicated.any():
                df_local = df_local[~index_duplicated]
                break

    # ソート
    df_local = df_local.sort_index()

    # フィルタ
    df_local = df_local[df_local.index >= datetime.strftime(RANGE_MIN, "%Y-%m-%d")]
    print(
        "[{}/{}]".format(
            datetime.strftime(df_local.index.max(), "%Y-%m-%d"),
            datetime.strftime(df_local.index.min(), "%Y-%m-%d"),
        ),
        end="",
    )
    # 保存
    io.save_hist(code, df_local)


def get_hist(code: str, market: str, page: int = 1):
    """
    株価１ページ分のDataFrame(date,o,h,l,c)を返す。
    日付の昇順。
    """
    # リクエスト
    res = requests.get(HIST_URL.format(code=code, market=market, page=page, start=PARAM_FROM, end=PARAM_TO))
    if res.status_code != 200:
        raise Exception("レスポンスエラー：{}".format(res.url))

    # 解析
    soup = bs4.BeautifulSoup(res.content, "lxml")

    # tableタグが2つあって、１つ目が対象
    tables = soup.find_all("table")
    if len(tables) == 1:
        return None
    hist_table: bs4.element.Tag = tables[0]

    # １行目以降を抽出
    df = pd.DataFrame([_hist_row(row) for row in filter(_hist_filter, hist_table.select("tr")[1:])])

    # 型変換
    df["date"] = [datetime.strptime(x, "%Y年%m月%d日") for x in df["date"]]
    for col in "ohlcv":
        df[col] = [float(x.replace(",", "")) for x in df[col]]

    # インデックス振り直して返す
    return df.set_index("date")


def _hist_filter(row: bs4.element.Tag):
    child_list = list(row.children)
    if len(child_list) == 2:
        if "分割" in str(child_list[1].text):
            return False
        else:
            return True
    else:
        return True


def _hist_row(row: bs4.element.Tag):
    cols = list(row.children)
    return dict(
        date=cols[0].text,  # 日付
        o=cols[1].text,  # 始値
        h=cols[2].text,  # 高値
        l=cols[3].text,  # 安値
        c=cols[4].text,  # 終値
        v=cols[5].text,  # 出来高
    )


if __name__ == "__main__":
    make_hist("1515", "T")
