from disaster import ETL_jp_disaster
from bs4 import BeautifulSoup
import pandas as pd

### from airflow.exceptions import AirflowFailException


class ETL_VXSE53(ETL_jp_disaster):
    def xml_to_df(self, xml_path, soup):
        try:
            df = pd.DataFrame(columns=self.columns)

            # Time
            OriginTime = self.format_datetime(soup.find("OriginTime").text)
            ArrivalTime = self.format_datetime(soup.find("ArrivalTime").text)

            # Hypocenter
            hypocenter = soup.find("Hypocenter")
            name = hypocenter.find("Name").text
            coordinate = hypocenter.find("jmx_eb:Coordinate")["description"].split("　")

            latitude = self.full_width_to_float(
                coordinate[0].replace("北緯", "").replace("度", "")
            )

            longitude = self.full_width_to_float(
                coordinate[1].replace("東経", "").replace("度", "")
            )

            depth = self.full_width_to_float(
                coordinate[-1]
                .replace("深さ", "")
                .replace("ｋｍ", "")
                .replace("ごく浅い", "0")
            )

            magnitude = soup.find("jmx_eb:Magnitude").text

            # Observation
            observation = soup.find("Observation")
            max_int = observation.find("MaxInt").text

            # Find all Pref nodes
            for pref in observation.find_all("Pref"):
                pref_name = pref.find("Name").text
                pref_max_int = pref.find("MaxInt").text

                # Find all Area nodes
                for area in pref.find_all("Area"):
                    area_name = area.find("Name").text
                    area_max_int = area.find("MaxInt").text

                    # Find all City nodes
                    for city in area.find_all("City"):
                        city_name = city.find("Name").text
                        city_max_int = city.find("MaxInt").text

                        # Find all IntensityStation nodes
                        for IntensityStation in city.find_all("IntensityStation"):
                            station_name = IntensityStation.find("Name").text
                            station_int = IntensityStation.find("Int").text

                            # Add row to DataFrame
                            df.loc[len(df)] = [
                                OriginTime,
                                ArrivalTime,
                                name,
                                latitude,
                                longitude,
                                depth,
                                magnitude,
                                max_int,
                                pref_name,
                                pref_max_int,
                                area_name,
                                area_max_int,
                                city_name,
                                city_max_int,
                                station_name,
                                station_int,
                            ]

            return df

        except Exception as e:
            raise  ### AirflowFailException(e)


if __name__ == "__main__":
    config_path = "disaster.json"
    etl_vxse53 = ETL_VXSE53(config_path, "eqvol", "VXSE53")
    etl_vxse53.xml_to_csv()
