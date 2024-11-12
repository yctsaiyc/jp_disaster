from VPCK70 import ETL_VPCK70
import pandas as pd


# 全般季節予報（２週間気温予報）
# 気象庁防災情報XMLフォーマット　技術資料: https://xml.kishou.go.jp/tec_material.html
# 電文毎の解説資料: https://xml.kishou.go.jp/jmaxml_20241031_Manual(pdf).zip


class ETL_VPZK70(ETL_VPCK70):
    pass


if __name__ == "__main__":
    pass  # 資料內容只有Comment
    # config_path = "disaster.json"
    # etl_vpzk70 = ETL_VPZK70(config_path, "regular", "VPZK70")
    # etl_vpzk70.xml_to_csv()
