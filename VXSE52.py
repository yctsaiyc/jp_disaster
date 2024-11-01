from disaster import ETL_jp_disaster
from bs4 import BeautifulSoup
import pandas as pd

### from airflow.exceptions import AirflowFailException


class ETL_VXSE52(ETL_jp_disaster):
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

            # Add row to DataFrame
            df.loc[len(df)] = [
                OriginTime,
                ArrivalTime,
                name,
                latitude,
                longitude,
                depth,
                magnitude,
            ]

            return df

        except Exception as e:
            raise  ### AirflowFailException(e)


if __name__ == "__main__":
    config_path = "disaster.json"
    etl_vxse52 = ETL_VXSE52(config_path, "eqvol", "VXSE52")
    etl_vxse52.xml_to_csv()
