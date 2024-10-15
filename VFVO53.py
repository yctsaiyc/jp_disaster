from disaster import ETL_jp_disaster
from bs4 import BeautifulSoup
import pandas as pd

### from airflow.exceptions import AirflowFailException


class ETL_VFVO53(ETL_jp_disaster):
    def xml_to_df(self, xml_path, soup):
        try:
            df = pd.DataFrame(columns=self.columns)

            volcano_info_1 = soup.find("VolcanoInfo", {"type": "降灰予報（対象火山）"})
            volcano_info_2 = soup.find(
                "VolcanoInfo", {"type": "降灰予報（対象市町村等）"}
            )
            ash_infos = soup.find_all("AshInfo", {"type": "降灰予報（定時）"})

            # 降灰予報（対象火山）
            kind_1_name = volcano_info_1.find("Kind").find("Name").text

            area_1 = volcano_info_1.find("Area")
            area_1_name = area_1.find("Name").text

            coordinate = area_1.find("Coordinate").text.replace("/", "").split("+")

            latitude = self.dms_to_decimal(float(coordinate[1]))
            longitude = self.dms_to_decimal(float(coordinate[2]))
            height = float(coordinate[3])

            df1 = pd.DataFrame(
                {
                    "Kind": [kind_1_name],
                    "Area": [area_1_name],
                    "Latitude": [latitude],
                    "Longitude": [longitude],
                    "Height": [height],
                }
            )

            # 降灰予報（対象市町村等）
            df2 = pd.DataFrame(columns=["Kind", "Area"])

            for item in volcano_info_2.find_all("Item"):
                kind_2_name = item.find("Kind").find("Name").text

                area_2s = item.find_all("Area")
                area_2_names = [area_2.find("Name").text for area_2 in area_2s]

                kind_2_names = [kind_2_name] * len(area_2_names)

                df2 = pd.concat(
                    [
                        df2,
                        pd.DataFrame(
                            {
                                "Kind": kind_2_names,
                                "Area": area_2_names,
                            }
                        ),
                    ],
                    ignore_index=True,
                )

            print(
                pd.concat(
                    [pd.concat([df1] * len(df2), ignore_index=True), df2],
                    axis=1,
                    ignore_index=True,
                )
            )

            exit()

            # Save DataFrame to CSV
            self.df_to_csv(df, xml_path)

        except Exception as e:
            raise  ### AirflowFailException(e)


if __name__ == "__main__":
    etl_vfvo53 = ETL_VFVO53("disaster.json", "eqvol", "VFVO53")
    etl_vfvo53.xml_to_csv()
