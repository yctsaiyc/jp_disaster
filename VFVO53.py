from disaster import ETL_jp_disaster
from bs4 import BeautifulSoup
import pandas as pd
import os
import shutil

### from airflow.exceptions import AirflowFailException


class ETL_VFVO53(ETL_jp_disaster):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.soup = None

    def df_to_csv(self, df, xml_path, subfolder):
        subfolder = os.path.join(self.data_dir, subfolder)
        os.makedirs(subfolder, exist_ok=True)
        csv_path = os.path.join(
            subfolder, os.path.basename(xml_path).replace(".xml", ".csv")
        )
        df.to_csv(csv_path, index=False, encoding="utf-8")
        print("Saved:", csv_path)

    def move_xml(self, xml_path):
        target_dir = os.path.join(self.data_dir, "xml", "converted")
        os.makedirs(target_dir, exist_ok=True)
        target_path = os.path.join(target_dir, os.path.basename(xml_path))

        shutil.move(xml_path, target_path)
        print(f"Moved {xml_path} to {target_path}")

    # 降灰予報（対象火山）
    def volcano_info_1_to_df(self, rows_count=1):
        volcano_info = self.soup.find("VolcanoInfo", {"type": "降灰予報（対象火山）"})
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
    def volcano_info_2_to_df(self):
        volcano_info = self.soup.find(
            "VolcanoInfo", {"type": "降灰予報（対象市町村等）"}
        )

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

    # 降灰予報（定時）
    def ash_info_to_df(self, ash_info):
        df = pd.DataFrame()

        for item in ash_info.find_all("Item"):
            areas = item.find_all("Area")
            area_names = [area.find("Name").text for area in areas]

            if df.empty:
                df = pd.DataFrame({"area_name": area_names})
            else:
                df = pd.concat(
                    [df, pd.DataFrame({"area_name": area_names})], ignore_index=True
                )

            df["start_time"] = self.format_datetime(ash_info.find("StartTime").text)
            df["end_time"] = self.format_datetime(ash_info.find("EndTime").text)

            kind = item.find("Kind")
            df["kind_name"] = kind.find("Name").text

            property_ = kind.find("Property")
            df["polygon"] = property_.find("jmx_eb:Polygon").text
            df["plume_direction"] = property_.find("jmx_eb:PlumeDirection").text
            df["distance"] = property_.find("Distance").text

            col = df.pop("area_name")
            df["area_name"] = col

        return df

    # 降灰予報（定時）
    def ash_infos_to_df(self):
        ash_infos = self.soup.find("AshInfos", {"type": "降灰予報（定時）"})

        df = pd.DataFrame()

        for ash_info in ash_infos.find_all("AshInfo"):
            if df.empty:
                df = self.ash_info_to_df(ash_info)

            else:
                df = pd.concat([df, self.ash_info_to_df(ash_info)], ignore_index=True)

        return df

    def xml_to_df(self, xml_path, soup):
        try:
            self.soup = soup

            # df = pd.DataFrame(columns=self.columns)

            # # 降灰予報（対象火山）
            # df_volcano_info_1 = self.volcano_info_1_to_df()
            # self.df_to_csv(df_volcano_info_1, xml_path, subfolder="volcano_info_1")

            # # 降灰予報（対象市町村等）
            # df_volcano_info_2 = self.volcano_info_2_to_df()
            # self.df_to_csv(df_volcano_info_2, xml_path, subfolder="volcano_info_2")

            # 降灰予報（定時）
            df = self.ash_infos_to_df()
            self.df_to_csv(df, xml_path, subfolder="ash_infos")
            self.move_xml(xml_path)
            exit()

        except Exception as e:
            raise  ### AirflowFailException(e)


if __name__ == "__main__":
    etl_vfvo53 = ETL_VFVO53("disaster.json", "eqvol", "VFVO53")
    etl_vfvo53.xml_to_csv()
