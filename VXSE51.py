from disaster import ETL_jp_disaster
from bs4 import BeautifulSoup
import pandas as pd

### from airflow.exceptions import AirflowFailException


class ETL_VXSE51(ETL_jp_disaster):
    def xml_to_df(self, xml_path, soup):
        try:
            df = pd.DataFrame(columns=self.columns)

            # Find DateTime
            ReportDateTime = self.format_datetime(soup.find("ReportDateTime").text)
            TargetDateTime = self.format_datetime(soup.find("TargetDateTime").text)

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

            return df

        except Exception as e:
            raise  ### AirflowFailException(e)


if __name__ == "__main__":
    config_path = "disaster.json"
    etl_vxse51 = ETL_VXSE51(config_path, "eqvol", "VXSE51")
    etl_vxse51.xml_to_csv()
