from disaster import ETL_jp_disaster
import pandas as pd

### from airflow.exceptions import AirflowFailException


# 噴火警報・予報
class ETL_VFVO50(ETL_jp_disaster):
    def xml_to_df(self, xml_path, soup):
        try:
            df = pd.DataFrame(columns=self.columns)

            # DateTime
            report_date_time = self.format_datetime(soup.find("ReportDateTime").text)
            target_date_time = self.format_datetime(soup.find("TargetDateTime").text)

            # <VolcanoInfo type="噴火警報・予報（対象火山）">
            volcano_info_1 = soup.find(
                "VolcanoInfo", {"type": "噴火警報・予報（対象火山）"}
            )

            kind_1_name = volcano_info_1.find("Kind").find("Name").text

            area_1 = volcano_info_1.find("Area")
            area_1_name = area_1.find("Name").text

            coordinate = self.process_coordinate(
                area_1.find("Coordinate").text, format_="dms"
            )

            latitude = coordinate[0]
            longitude = coordinate[1]
            height = coordinate[2]

            wkt = self.add_wkt(longitude, latitude)

            # # <VolcanoInfo type="噴火警報・予報（対象市町村等）">
            # volcano_info_2 = soup.find("VolcanoInfo", {"type": "噴火警報・予報（対象市町村等）"})

            # <VolcanoInfo type="噴火警報・予報（対象市町村の防災対応等）">
            volcano_info_3 = soup.find(
                "VolcanoInfo", {"type": "噴火警報・予報（対象市町村の防災対応等）"}
            )

            for item in volcano_info_3.find_all("Item"):
                kind_3_name = item.find("Kind").find("Name").text
                areas_3 = item.find_all("Area")

                for area_3 in areas_3:
                    area_3_name = area_3.find("Name").text

                    df.loc[len(df)] = [
                        report_date_time,  # 発表時刻
                        target_date_time,  # 基点時刻
                        kind_1_name,  # 防災気象情報要素名
                        area_1_name,  # 対象地域・地点名称
                        longitude,  # 火山の経度
                        latitude,  # 火山の緯度
                        height,  # 火山の標高
                        kind_3_name,  # 対象市町村情報要素名
                        area_3_name,  # 対象市町村名称
                        wkt,
                    ]

            return df

        except Exception as e:
            raise  ### AirflowFailException(e)


if __name__ == "__main__":
    config_path = "disaster.json"
    etl_vfvo50 = ETL_VFVO50(config_path, "eqvol", "VFVO50")
    etl_vfvo50.xml_to_csv()
