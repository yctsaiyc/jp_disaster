from VPCK50 import ETL_VPCK50
import pandas as pd


# 全般季節予報 (全般１か月予報，全般３か月予報，全般暖・寒候期予報)
# 気象庁防災情報XMLフォーマット　技術資料: https://xml.kishou.go.jp/tec_material.html
# 電文毎の解説資料: https://xml.kishou.go.jp/jmaxml_20241031_Manual(pdf).zip


class ETL_VPZK50(ETL_VPCK50):
    pass


if __name__ == "__main__":
    config_path = "disaster.json"
    etl_vpzk50 = ETL_VPZK50(config_path, "regular", "VPZK50")
    etl_vpzk50.xml_to_csv()
