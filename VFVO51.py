from disaster import ETL_jp_disaster
from bs4 import BeautifulSoup
import pandas as pd

### from airflow.exceptions import AirflowFailException


class ETL_VFVO51(ETL_jp_disaster):
    def xml_to_df(self, xml_path, soup):
        try:
            df = pd.DataFrame(columns=self.columns)

            # DateTime
            report_date_time = self.format_datetime(soup.find("ReportDateTime").text)
            target_date_time = self.format_datetime(soup.find("TargetDateTime").text)

            # 2.VolcanoInfo【防災気象情報事項】(1 回)
            volcano_info = soup.find("VolcanoInfo")

            # 2-1.Item【個々の防災気象情報要素】(1 回以上)
            for item in volcano_info.find_all("Item"):

                # 2-1-2.Kind【防災気象情報要素】(1 回)
                kind = item.find("Kind")

                # 2-1-2-1.Name【防災気象情報要素名】(1 回)
                kind_name = kind.find("Name").text

                # 2-1-2-4.Condition【状況】(1 回)
                kind_condition = kind.find("Condition").text

                # 2-1-4.Areas【対象地域・地点コード種別】(1 回)
                areas = item.find_all("Area")

                # 2-1-4-1.Area【対象地域・地点】(1 回以上)
                for area in areas:

                    # 2-1-4-1-1.Name【対象地域・地点名称】(1 回)
                    area_name = area.find("Name").text

                    # 2-1-4-1-3.Coordinate【対象火山の位置】(1 回)
                    # <Coordinate description="北緯３１度２６．３８分　東経１４０度０３．０３分　標高１３６ｍ">
                    # +3126.38+14003.03+136/</Coordinate>
                    # <Coordinate description="北緯２６度０７．６０分　東経１４１度０６．１０分　水深９５ｍ">
                    # +2607.60+14106.10-95/</Coordinate>
                    coordinate = self.process_coordinate(
                        area.find("Coordinate").text, format_="dms"
                    )

                    latitude = coordinate[0]
                    longitude = coordinate[1]
                    height = coordinate[2]

                    wkt = self.add_wkt(longitude, latitude)

                    df.loc[len(df)] = [
                        report_date_time,
                        target_date_time,
                        kind_name,
                        kind_condition,
                        area_name,
                        longitude,
                        latitude,
                        height,
                        wkt,
                    ]

            return df

        except Exception as e:
            raise  ### AirflowFailException(e)


if __name__ == "__main__":
    config_path = "disaster.json"
    etl_vfvo51 = ETL_VFVO51(config_path, "eqvol", "VFVO51")
    etl_vfvo51.xml_to_csv()
