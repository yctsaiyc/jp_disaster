from disaster import ETL_jp_disaster
from bs4 import BeautifulSoup
import pandas as pd

### from airflow.exceptions import AirflowFailException


class ETL_VPTW40(ETL_jp_disaster):
    def xml_to_df(self, xml_path, soup):
        print(type(soup))
        try:
            df = pd.DataFrame(columns=self.columns)

            report_datetime = self.format_datetime(soup.find("ReportDateTime").text)
            print(type(report_datetime))
            target_datetime = self.format_datetime(soup.find("TargetDateTime").text)

            meteorological_infos = soup.find_all("MeteorologicalInfo")

            for meteo in meteorological_infos:
                type_ = meteo.find("DateTime")["type"]

                date_time = self.format_datetime(meteo.find("DateTime").text)

                item = meteo.find("Item")
                print(type(item))

                # 1. 呼称
                typhoon_name = item.find("TyphoonNamePart").find("NameKana").text

                # 2. 階級
                class_part = item.find("ClassPart")

                # ・台風の階級:“台風(TY)”、“台風(STS)”、“台風(TS)”、“熱帯低気圧(TD)”、
                # “ハリケーン(Hurricane)”、“発達した熱帯低気圧(Tropical Storm)”、
                # “温帯低気圧(LOW)”、または記述なし(空タグ)。
                typhoon_class = class_part.find("jmx_eb:TyphoonClass").text

                # ・台風の大きさ: “大型”、“超大型”、または記述なし(空タグ)。
                # 「予報 X 時間後」では省略する。
                area_class = class_part.find("jmx_eb:AreaClass").text

                # ・台風の強さ:“強い”、“非常に強い”、“猛烈な”、または記述なし(空タグ)。
                intensity_class = class_part.find("jmx_eb:IntensityClass").text

                # 3. 中心
                center_part = item.find("CenterPart")

                # ・台風の中心位置
                coordinate = center_part.find(
                    "jmx_eb:Coordinate", {"type": "中心位置（度）"}
                )

                if coordinate.text:
                    coordinate = coordinate.text.replace("/", "").split("+")
                    longitude = coordinate[-1]
                    latitude = coordinate[-2]

                else:
                    print("Not found coordinate")
                    longitude = ""
                    latitude = ""

                # ・台風の存在域

                # ・台風の移動方向

                # ・台風の移動速度(ノット)

                # ・台風の移動速度(km/h)

                df.loc[len(df)] = [
                    report_datetime,
                    target_datetime,
                    type_,
                    date_time,
                    typhoon_name,
                    typhoon_class,
                    area_class,
                    intensity_class,
                    longitude,
                    latitude,
                ]

            return df

        except Exception as e:
            raise  ### AirflowFailException(e)


if __name__ == "__main__":
    config_path = "disaster.json"
    etl_vptw40 = ETL_VPTW40(config_path, "extra", "VPTW40")
    etl_vptw40.xml_to_csv()
