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

            df.loc[len(df)] = [
                kind_1_name,
                area_1_name,
                latitude,
                longitude,
                height,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ]

            print(df)
            exit()

            # Save DataFrame to CSV
            self.df_to_csv(df, xml_path)

        except Exception as e:
            raise  ### AirflowFailException(e)


if __name__ == "__main__":
    etl_vfvo53 = ETL_VFVO53("disaster.json", "eqvol", "VFVO53")
    etl_vfvo53.xml_to_csv()
