import pandas as pd
import requests

import local_io as io

EXCEL_URL = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"


def make_master():
    """
    JPXからdaga_jを取得して保存
    """

    # リクエスト
    res = requests.get(EXCEL_URL)
    if res.status_code != 200:
        raise Exception("レスポンスエラー：{}".format(res.url))

    # Excel to DataFrame
    df = pd.read_excel(res.content)

    # 型変換
    df["33業種コード"] = df["33業種コード"].replace("-", "9999")
    df["17業種コード"] = df["17業種コード"].replace("-", "99")
    df["規模コード"] = df["規模コード"].replace("-", "9")

    # 不要なカラム削除
    df.drop(["33業種区分", "17業種区分", "規模区分", "日付"], axis=1, inplace=True)

    # 国内株式のプライム、スタンダード、グロースだけに絞る
    df = df[df["市場・商品区分"].isin(["プライム（内国株式）", "スタンダード（内国株式）", "グロース（内国株式）"])]

    # 市場名を変更する
    df.replace({"市場・商品区分": {"プライム（内国株式）": "PR", "スタンダード（内国株式）": "ST", "グロース（内国株式）": "GR"}}, inplace=True)

    # 列名を揃える
    df.rename(
        columns={
            "コード": "code",
            "銘柄名": "name",
            "市場・商品区分": "market",
            "33業種コード": "type-33",
            "17業種コード": "type-17",
            "規模コード": "type-scale",
        },
        inplace=True,
    )

    # 型変換
    df = df.astype(
        {
            "code": "int",
            "type-33": "int",
            "type-17": "int",
            "type-scale": "int",
        }
    )

    # インデックス振り直して返す
    df = df.set_index("code").sort_index()

    # 保存
    io.save_stock_master(df)


if __name__ == "__main__":
    make_master()
