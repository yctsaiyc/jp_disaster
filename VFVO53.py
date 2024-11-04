from disaster import ETL_jp_disaster
import pandas as pd

### from airflow.exceptions import AirflowFailException


class ETL_VFVO53(ETL_jp_disaster):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.type = "降灰予報（定時）"
        self.soup = None

    # 降灰予報（対象火山）
    def volcano_info_1_to_df(self, rows_count=1):
        volcano_info = self.soup.find("VolcanoInfo", {"type": "降灰予報（対象火山）"})
        kind_name = volcano_info.find("Kind").find("Name").text

        area = volcano_info.find("Area")
        area_name = area.find("Name").text

        coordinate = self.process_coordinate(
            area.find("Coordinate").text, format_="dms"
        )

        latitude = coordinate[0]
        longitude = coordinate[1]
        height = coordinate[2]

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
            length = len(area_names)

            kind = item.find("Kind")
            kind_names = [kind.find("Name").text] * length

            property_ = kind.find("Property")

            # ***多角形可能有多個***
            polygons = [property_.find("jmx_eb:Polygon").text] * length
            plume_directions = [property_.find("jmx_eb:PlumeDirection").text] * length
            distances = [property_.find("Distance").text] * length

            new_df = pd.DataFrame(
                {
                    "kind_name": kind_names,
                    "area_name": area_names,
                    "polygon": polygons,
                    "plume_direction": plume_directions,
                    "distance": distances,
                }
            )

            df = pd.concat([new_df, df], ignore_index=True) if not df.empty else new_df

        df.insert(
            0, "start_time", self.format_datetime(ash_info.find("StartTime").text)
        )
        df.insert(1, "end_time", self.format_datetime(ash_info.find("EndTime").text))

        return df

    # 降灰予報（定時）
    def ash_infos_to_df(self):
        ash_infos = self.soup.find("AshInfos", {"type": self.type})

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

            # 降灰予報（対象火山）
            df_volcano_info_1 = self.volcano_info_1_to_df()

            # # 降灰予報（対象市町村等）
            # df_volcano_info_2 = self.volcano_info_2_to_df()

            # 降灰予報（定時）
            df_ash_infos = self.ash_infos_to_df()

            # 合併降灰予報（対象火山）& 降灰予報（定時）
            df_volcano_info_repeated = pd.concat(
                [df_volcano_info_1] * len(df_ash_infos),
                ignore_index=True,
            )

            df = pd.concat(
                [df_volcano_info_repeated, df_ash_infos],
                ignore_index=True,
                axis=1,
            )

            # add DateTime
            ReportDateTime = self.format_datetime(soup.find("ReportDateTime").text)
            TargetDateTime = self.format_datetime(soup.find("TargetDateTime").text)

            df.insert(0, "ReportDateTime", ReportDateTime)
            df.insert(1, "TargetDateTime", TargetDateTime)

            df.columns = self.columns

            return df

        except Exception as e:
            raise  ### AirflowFailException(e)


if __name__ == "__main__":
    etl_vfvo53 = ETL_VFVO53("disaster.json", "eqvol", "VFVO53")
    etl_vfvo53.xml_to_csv()
