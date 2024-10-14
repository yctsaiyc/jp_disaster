from disaster import ETL_jp_disaster
import json
import os
from bs4 import BeautifulSoup
import pandas as pd


class ETL_VXSE51(ETL_jp_disaster):
    def xml_to_df(self, xml_path, soup):
        df = pd.DataFrame(columns=self.columns)

        # Find DateTime
        ReportDateTime = (
            soup.find("ReportDateTime").text.replace("T", " ").replace("+09:00", "")
        )
        TargetDateTime = (
            soup.find("TargetDateTime").text.replace("T", " ").replace("+09:00", "")
        )

        # Find all Pref nodes
        for pref in soup.find_all("Pref"):
            pref_name = pref.find("Name").text
            pref_max_int = pref.find("MaxInt").text

            # Find all Area nodes
            for area in pref.find_all("Area"):
                area_name = area.find("Name").text
                area_max_int = area.find("MaxInt").text

                # Add row to DataFrame
                df.loc[len(df)] = [
                    ReportDateTime,
                    TargetDateTime,
                    pref_name,
                    pref_max_int,
                    area_name,
                    area_max_int,
                ]

        # Save DataFrame to CSV
        self.df_to_csv(df, xml_path)


if __name__ == "__main__":
    etl_vxse51 = ETL_VXSE51("disaster.json", "eqvol", "VXSE51")
    etl_vxse51.xml_to_csv()
