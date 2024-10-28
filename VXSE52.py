from disaster import ETL_jp_disaster
from bs4 import BeautifulSoup
import pandas as pd


class ETL_VXSE52(ETL_jp_disaster):
    def xml_to_df(self, xml_path, soup):
        df = pd.DataFrame(columns=self.columns)

        # Time
        OriginTime = (
            soup.find("OriginTime").text.replace("T", " ").replace("+09:00", "")
        )

        ArrivalTime = (
            soup.find("ArrivalTime").text.replace("T", " ").replace("+09:00", "")
        )

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


def VXSE52(config_path):
    etl_vxse52 = ETL_VXSE52(config_path, "eqvol", "VXSE52")
    etl_vxse52.xml_to_csv()


if __name__ == "__main__":
    VXSE52("disaster.json")
