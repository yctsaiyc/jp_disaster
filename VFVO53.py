from disaster import ETL_jp_disaster
from bs4 import BeautifulSoup
import pandas as pd

### from airflow.exceptions import AirflowFailException


class ETL_VFVO53(ETL_jp_disaster):
    # 降灰予報（対象火山）
    def volcano_info_1_to_df(self, soup, rows_count=1):
        volcano_info = soup.find("VolcanoInfo", {"type": "降灰予報（対象火山）"})
        kind_name = volcano_info.find("Kind").find("Name").text

        area = volcano_info.find("Area")
        area_name = area.find("Name").text

        coordinate = area.find("Coordinate").text.replace("/", "").split("+")

        latitude = self.dms_to_decimal(float(coordinate[1]))
        longitude = self.dms_to_decimal(float(coordinate[2]))
        height = float(coordinate[3])

        df = pd.DataFrame(
            {
                "Kind": [kind_name] * rows_count,
                "Area": [area_name] * rows_count,
                "Latitude": [latitude] * rows_count,
                "Longitude": [longitude] * rows_count,
                "Height": [height] * rows_count,
            }
        )

        return df

    # 降灰予報（対象市町村等）
    def volcano_info_2_to_df(self, soup):
        volcano_info = soup.find("VolcanoInfo", {"type": "降灰予報（対象市町村等）"})

        df = pd.DataFrame(columns=["Kind", "Area"])

        for item in volcano_info.find_all("Item"):
            kind_name = item.find("Kind").find("Name").text

            areas = item.find_all("Area")
            area_names = [area.find("Name").text for area in areas]

            df = pd.concat(
                [
                    df,
                    pd.DataFrame(
                        {
                            "Kind": [kind_name] * len(area_names),
                            "Area": area_names,
                        }
                    ),
                ],
                ignore_index=True,
            )

        return df

    def xml_to_df(self, xml_path, soup):
        try:
            df = pd.DataFrame(columns=self.columns)

            # 降灰予報（対象市町村等）
            df_volcano_info_2 = self.volcano_info_2_to_df(soup)

            # 降灰予報（対象火山）
            df_volcano_info_1 = self.volcano_info_1_to_df(
                soup, rows_count=len(df_volcano_info_2)
            )

            df = pd.concat(
                [df_volcano_info_1, df_volcano_info_2], axis=1, ignore_index=True
            )
            print(df)
            exit()

            # 降灰予報（定時）
            ash_infos = soup.find_all("AshInfo", {"type": "降灰予報（定時）"})

            # Save DataFrame to CSV
            self.df_to_csv(df, xml_path)

        except Exception as e:
            raise  ### AirflowFailException(e)


if __name__ == "__main__":
    etl_vfvo53 = ETL_VFVO53("disaster.json", "eqvol", "VFVO53")
    etl_vfvo53.xml_to_csv()
