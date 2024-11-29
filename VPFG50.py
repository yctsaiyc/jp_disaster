from disaster import ETL_jp_disaster
from bs4 import BeautifulSoup
import pandas as pd


# 府県天気概況
# 気象庁防災情報XMLフォーマット　技術資料: https://xml.kishou.go.jp/tec_material.html
# 電文毎の解説資料: https://xml.kishou.go.jp/jmaxml_20241031_Manual(pdf).zip


class ETL_VPFG50(ETL_jp_disaster):
    def xml_to_df(self, xml_path, soup):
        df = pd.DataFrame(columns=self.columns)

        ## 資料僅有Text故不採用

        return df


if __name__ == "__main__":
    config_path = "disaster.json"
    etl_vpfg50 = ETL_VPFG50(config_path, "regular", "VPFG50")
    etl_vpfg50.xml_to_csv()
