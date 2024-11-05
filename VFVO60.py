from disaster import ETL_jp_disaster
import pandas as pd

### from airflow.exceptions import AirflowFailException


# 推定噴煙流向報
class ETL_VFVO60(ETL_jp_disaster):
    def xml_to_df(self, xml_path, soup):
        try:
            df = pd.DataFrame(columns=self.columns)

            # DateTime
            report_date_time = self.format_datetime(soup.find("ReportDateTime").text)
            target_date_time = self.format_datetime(soup.find("TargetDateTime").text)
            event_date_time = self.format_datetime(soup.find("EventDateTime").text)

            # <VolcanoInfo type="推定噴煙流向報">
            # 2.VolcanoInfo【防災気象情報事項】(0 回以上)
            volcano_info = soup.find("VolcanoInfo")

            # 2-1.Item【個々の防災気象情報要素】(1 回以上)
            # 2-1-2.Kind【防災気象情報要素】(1 回)
            # 2-1-2-1.Name【防災気象情報要素名】(1 回)
            kind_name = volcano_info.find("Kind").find("Name").text

            # 2-1-3.Areas【対象地域・地点コード種別】(1 回)
            # 2-1-3-1.Area【対象地域・地点】(1 回以上)
            area = volcano_info_1.find("Area")

            # 2-1-3-1-1.Name【対象地域・地点名称】(1 回)
            area_name = area.find("Name").text

            # 2-1-3-1-3.Coordinate【対象火山の位置】(0 回/1 回)
            coordinate = self.process_coordinate(
                area.find("Coordinate").text, format_="dms"
            )

            latitude = coordinate[0]
            longitude = coordinate[1]
            height = coordinate[2]

            wkt = self.add_wkt(longitude, latitude)

            # 2-1-3-1-5.CraterName【対象火口名称】(0 回/1 回)
            # 2-1-3-1-6.CraterCoordinate【対象火口の位置】(0 回/1 回)
            crater = area.find("CraterName")

            if crater:
                crater_name = crater.text

                crater_coordinate = self.process_coordinate(
                    area.find("CraterCoordinate").text, format_="dms"
                )

                crater_latitude = crater_coordinate[0]
                crater_longitude = crater_coordinate[1]
                crater_height = crater_coordinate[2]

                crater_wkt = self.add_wkt(crater_longitude, crater_latitude)

            else:
                crater_name = ""
                crater_coordinate = ""
                crater_latitude = ""
                crater_longitude = ""
                crater_height = ""
                crater_wkt = ""

            # 3.VolcanoObservation【火山現象の観測内容等】(0 回/1 回)
            volcano_observation = soup.find("VolcanoObservation")

            # 3-1.ColorPlume【有色噴煙】(0 回/1 回)
            # 3-1-1.jmx_eb:PlumeHeightAboveCrater【有色噴煙の火口(縁)上高度】(1 回)
            # 3-1-2.jmx_eb:PlumeHeightAboveSeaLevel【有色噴煙の海抜高度】(0 回/1 回)
            # 3-1-3.jmx_eb:PlumeDirection【有色噴煙の流向】(1 回)
            # 3-2.WindAboveCrater【火口直上の風】(0 回/1 回)
            # 3-2-1.jmx_eb:DateTime【火口直上の風要素の予測時刻】(1 回)
            # 3-2-2.WindAboveCraterElements【火口直上の風要素】(0 回以上)
            # 3-2-2-1.jmx_eb:WindHeightAboveSeaLevel【火口直上の風(高度)】(1 回)
            # 3-2-2-2.jmx_eb:WindDegree【火口直上の風(風向)】(1 回)
            # 3-2-2-3.jmx_eb:WindSpeed【火口直上の風(風速)】(1 回)

            return df

        except Exception as e:
            raise  ### AirflowFailException(e)


if __name__ == "__main__":
    config_path = "disaster.json"
    etl_vfvo60 = ETL_VFVO60(config_path, "eqvol", "VFVO60")
    etl_vfvo60.xml_to_csv()
