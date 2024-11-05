from disaster import ETL_jp_disaster
import pandas as pd

### from airflow.exceptions import AirflowFailException


# 推定噴煙流向報
class ETL_VFVO60(ETL_jp_disaster):
    def xml_to_df(self, xml_path, soup):
        try:
            df = pd.DataFrame(columns=self.columns)
            row_dict = {key: "" for key in self.columns}

            # DateTime
            row_dict["発表時刻"] = self.format_datetime(
                soup.find("ReportDateTime").text
            )

            row_dict["基点時刻"] = self.format_datetime(
                soup.find("TargetDateTime").text
            )

            row_dict["現象の発生時刻"] = self.format_datetime(
                soup.find("EventDateTime").text
            )

            # <VolcanoInfo type="推定噴煙流向報">
            # 2.VolcanoInfo【防災気象情報事項】(0 回以上)
            volcano_info = soup.find("VolcanoInfo")

            # 2-1.Item【個々の防災気象情報要素】(1 回以上)
            # 2-1-2.Kind【防災気象情報要素】(1 回)
            # 2-1-2-1.Name【防災気象情報要素名】(1 回)
            row_dict["防災気象情報要素名"] = volcano_info.find("Kind").find("Name").text

            # 2-1-3.Areas【対象地域・地点コード種別】(1 回)
            # 2-1-3-1.Area【対象地域・地点】(1 回以上)
            area = volcano_info.find("Area")

            # 2-1-3-1-1.Name【対象地域・地点名称】(1 回)
            row_dict["対象地域・地点名称"] = area.find("Name").text

            # 2-1-3-1-3.Coordinate【対象火山の位置】(0 回/1 回)
            coordinate = self.process_coordinate(
                area.find("Coordinate").text, format_="dms"
            )

            row_dict["対象火山の経度"] = coordinate[0]
            row_dict["対象火山の緯度"] = coordinate[1]
            row_dict["対象火山の標高"] = coordinate[2]

            row_dict["対象火山wkt"] = self.add_wkt(coordinate[0], coordinate[1])

            # 2-1-3-1-5.CraterName【対象火口名称】(0 回/1 回)
            # 2-1-3-1-6.CraterCoordinate【対象火口の位置】(0 回/1 回)
            crater = area.find("CraterName")

            if crater:
                row_dict["対象火口名称"] = crater.text

                crater_coordinate = self.process_coordinate(
                    area.find("CraterCoordinate").text, format_="dms"
                )

                row_dict["対象火口の経度"] = crater_coordinate[0]
                row_dict["対象火口の緯度"] = crater_coordinate[1]
                row_dict["対象火口の標高"] = crater_coordinate[2]

                row_dict["対象火口wkt"] = self.add_wkt(
                    crater_coordinate[0], crater_coordinate[1]
                )

            # 3.VolcanoObservation【火山現象の観測内容等】(0 回/1 回)
            volcano_observation = soup.find("VolcanoObservation")

            if volcano_observation:
                # 3-1.ColorPlume【有色噴煙】(0 回/1 回)
                cp = volcano_observation.find("ColorPlume")

                if cp:
                    # 3-1-1.jmx_eb:PlumeHeightAboveCrater【有色噴煙の火口(縁)上高度】(1 回)
                    phac = cp.find("jmx_eb:PlumeHeightAboveCrater")

                    if phac:
                        row_dict["有色噴煙の火口(縁)上高度"] = phac.text

                    # 3-1-2.jmx_eb:PlumeHeightAboveSeaLevel【有色噴煙の海抜高度】(0 回/1 回)
                    phasl = volcano_observation.find("jmx_eb:PlumeHeightAboveSeaLevel")

                    if phasl:
                        row_dict["有色噴煙の海抜高度"] = phasl.text

                    # 3-1-3.jmx_eb:PlumeDirection【有色噴煙の流向】(1 回)
                    pd_ = volcano_observation.find("jmx_eb:PlumeDirection")

                    if pd_:
                        row_dict["有色噴煙の流向"] = pd_.text

                # 3-2.WindAboveCrater【火口直上の風】(0 回/1 回)
                wac = volcano_observation.find("WindAboveCrater")

                if wac:
                    # 3-2-1.jmx_eb:DateTime【火口直上の風要素の予測時刻】(1 回)
                    row_dict["火口直上の風要素の予測時刻"] = wac.find(
                        "jmx_eb:DateTime"
                    ).text

                    # 3-2-2.WindAboveCraterElements【火口直上の風要素】(0 回以上)
                    waces = wac.find_all("WindAboveCraterElements")

                    row_dict_2 = row_dict.copy()

                    for wace in waces:
                        # 3-2-2-1.jmx_eb:WindHeightAboveSeaLevel【火口直上の風(高度)】(1 回)
                        row_dict_2["火口直上の風(高度)"] = int(
                            wace.find("jmx_eb:WindHeightAboveSeaLevel").text
                        )

                        # 3-2-2-2.jmx_eb:WindDegree【火口直上の風(風向)】(1 回)
                        row_dict_2["火口直上の風(風向)"] = int(
                            wace.find("jmx_eb:WindDegree").text
                        )

                        # 3-2-2-3.jmx_eb:WindSpeed【火口直上の風(風速)】(1 回)
                        row_dict_2["火口直上の風(風速)"] = int(
                            wace.find("jmx_eb:WindSpeed").text
                        )

                        df = pd.concat(
                            [df, pd.DataFrame([row_dict_2])], ignore_index=True
                        )

                        row_dict_2 = row_dict.copy()

            return df

        except Exception as e:
            raise  ### AirflowFailException(e)


if __name__ == "__main__":
    config_path = "disaster.json"
    etl_vfvo60 = ETL_VFVO60(config_path, "eqvol", "VFVO60")
    etl_vfvo60.xml_to_csv()
