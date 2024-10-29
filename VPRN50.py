from disaster import ETL_jp_disaster
from bs4 import BeautifulSoup
import pandas as pd

### from airflow.exceptions import AirflowFailException


class ETL_VPRN50(ETL_jp_disaster):
    def xml_to_df(self, xml_path, soup):
        try:
            df = pd.DataFrame(columns=self.columns)

            meteorological_infos_s = soup.find_all("MeteorologicalInfos")

            for meteorological_infos in meteorological_infos_s:
                meteorological_info_s = meteorological_infos.find_all(
                    "MeteorologicalInfo"
                )

                for meteorological_info in meteorological_info_s:
                    date_time = (
                        meteorological_info.find("DateTime")
                        .text.replace("T", " ")
                        .replace("+09:00", "")
                    )

                    for item in meteorological_info.find_all("Item"):
                        kind = item.find("Kind")
                        area = item.find("Area")

                        area_code_type = area["codeType"]
                        area_name = area.find("Name").text

                        area_prefecture = area.find("Prefecture")

                        if area_prefecture is not None:
                            area_prefecture = area_prefecture.text

                        else:
                            area_prefecture = ""

                        kind_significancies = []

                        types = [
                            "大雨危険度",
                            "土砂災害危険度",
                            "浸水害危険度",
                            "洪水害危険度",
                        ]

                        for t in types:
                            kind_significancy = kind.find("Significancy", {"type": t})

                            if kind_significancy is not None:
                                kind_significancies.append(
                                    kind_significancy.find("Name").text
                                )

                            else:
                                kind_significancies.append("")

                        df.loc[len(df)] = [
                            date_time,
                            area_code_type,
                            area_name,
                            area_prefecture,
                            kind_significancies[0],
                            kind_significancies[1],
                            kind_significancies[2],
                            kind_significancies[3],
                        ]

            return df

        except Exception as e:
            raise  ### AirflowFailException(e)


if __name__ == "__main__":
    config_path = "disaster.json"
    etl_vprn50 = ETL_VPRN50(config_path, "regular", "VPRN50")
    etl_vprn50.xml_to_csv()
