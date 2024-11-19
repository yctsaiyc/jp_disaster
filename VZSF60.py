from VZSF51 import ETL_VZSF51
import pandas as pd


# アジア太平洋海上悪天２４時間予想図
# 気象庁防災情報XMLフォーマット　技術資料: https://xml.kishou.go.jp/tec_material.html
# 電文毎の解説資料: https://xml.kishou.go.jp/jmaxml_20241031_Manual(pdf).zip


class ETL_VZSF60(ETL_VZSF51):
    pass


if __name__ == "__main__":
    config_path = "disaster.json"
    etl_vzsf60 = ETL_VZSF60(config_path, "regular", "VZSF60")
    etl_vzsf60.xml_to_csv()
