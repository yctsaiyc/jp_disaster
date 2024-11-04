from disaster import ETL_jp_disaster
import pandas as pd

### from airflow.exceptions import AirflowFailException


# 噴火に関する火山観測報
class ETL_VFVO52(ETL_jp_disaster):
    def get_volcano_observation(self, soup):
        # 3.VolcanoObservation【火山現象の観測内容等】(0 回/1 回)
        volcano_observation = soup.find("VolcanoObservation")

        vo = {
            "c_phac_condition": "",
            "c_phac_description": "",
            "c_phasl_condition": "",
            "c_phasl_description": "",
            "c_pd": "",
            "w_phac_condition": "",
            "w_phac_description": "",
            "w_phasl_condition": "",
            "w_phasl_description": "",
            "w_pd": "",
        }

        if volcano_observation:
            # 3-1.ColorPlume【有色噴煙】(0 回/1 回)
            cp = volcano_observation.find("ColorPlume")

            if cp:

                # 3-1-1.jmx_eb:PlumeHeightAboveCrater【有色噴煙の火口(縁)上高度】(0 回/1 回)
                c_phac = cp.find("jmx_eb:PlumeHeightAboveCrater")

                if c_phac:
                    vo["c_phac_condition"] = c_phac.get("condition", "")
                    vo["c_phac_description"] = c_phac.get("description", "")

                # 3-1-2.jmx_eb:PlumeHeightAboveSeaLevel【有色噴煙の海抜高度】(0 回/1 回)
                c_phasl = volcano_observation.find("jmx_eb:PlumeHeightAboveSeaLevel")

                if c_phasl:
                    vo["c_phasl_condition"] = c_phasl.get("condition", "")
                    vo["c_phasl_description"] = c_phasl.get("description", "")

                # 3-1-3.jmx_eb:PlumeDirection【有色噴煙の流向】(0 回/1 回)
                c_pd = volcano_observation.find("jmx_eb:PlumeDirection")

                if c_pd:
                    vo["c_pd"] = c_pd.text

            # 3-1-4.PlumeComment【有色噴煙の補足】(0 回/1 回)

            # 3-2.WhitePlume【白色噴煙】(0 回/1 回)
            wp = volcano_observation.find("WhitePlume")

            if wp:

                # 3-1-1.jmx_eb:PlumeHeightAboveCrater【白色噴煙の火口(縁)上高度】(0 回/1 回)
                w_phac = wp.find("jmx_eb:PlumeHeightAboveCrater")

                if w_phac:
                    vo["w_phac_condition"] = w_phac.get("condition", "")
                    vo["w_phac_description"] = w_phac.get("description", "")

                # 3-1-2.jmx_eb:PlumeHeightAboveSeaLevel【白色噴煙の海抜高度】(0 回/1 回)
                w_phasl = volcano_observation.find("jmx_eb:PlumeHeightAboveSeaLevel")

                if w_phasl:
                    vo["w_phasl_condition"] = w_phasl.get("condition", "")
                    vo["w_phasl_description"] = w_phasl.get("description", "")

                # 3-1-3.jmx_eb:PlumeDirection【白色噴煙の流向】(0 回/1 回)
                w_pd = volcano_observation.find("jmx_eb:PlumeDirection")

                if w_pd:
                    vo["w_pd"] = w_pd.text

        return vo

    def xml_to_df(self, xml_path, soup):
        try:
            df = pd.DataFrame(columns=self.columns)

            # DateTime
            report_date_time = self.format_datetime(soup.find("ReportDateTime").text)
            target_date_time = self.format_datetime(soup.find("TargetDateTime").text)

            # 3.VolcanoObservation【火山現象の観測内容等】(0 回/1 回)
            vo = self.get_volcano_observation(soup)

            # 2.VolcanoInfo【防災気象情報事項】(1 回)
            volcano_info = soup.find("VolcanoInfo")

            # 2-1.Item【個々の防災気象情報要素】(1 回)
            item = volcano_info.find("Item")

            # 2-1-1-1.EventDateTime【現象の発生時刻】(0 回/1 回)
            event_date_time = self.format_datetime(item.find("EventDateTime").text)

            # 2-1-2.Kind【防災気象情報要素】(1 回)
            # 2-1-2-1.Name【防災気象情報要素名】(1 回)
            kind_name = item.find("Kind").find("Name").text

            # 2-1-4.Areas【対象地域・地点コード種別】(1 回)
            areas = item.find_all("Area")

            # 2-1-4-1.Area【対象地域・地点】(1 回以上)
            for area in areas:

                # 2-1-4-1-1.Name【対象地域・地点名称】(1 回)
                area_name = area.find("Name").text

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
                    event_date_time,
                    kind_name,
                    area_name,
                    longitude,
                    latitude,
                    height,
                    vo["c_phac_condition"],
                    vo["c_phac_description"],
                    vo["c_phasl_condition"],
                    vo["c_phasl_description"],
                    vo["c_pd"],
                    vo["w_phac_condition"],
                    vo["w_phac_description"],
                    vo["w_phasl_condition"],
                    vo["w_phasl_description"],
                    vo["w_pd"],
                    wkt,
                ]

            return df

        except Exception as e:
            raise  ### AirflowFailException(e)


if __name__ == "__main__":
    config_path = "disaster.json"
    etl_vfvo52 = ETL_VFVO52(config_path, "eqvol", "VFVO52")
    etl_vfvo52.xml_to_csv()
