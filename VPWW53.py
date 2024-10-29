from disaster import ETL_jp_disaster
from bs4 import BeautifulSoup
import pandas as pd

### from airflow.exceptions import AirflowFailException


class ETL_VPWW53(ETL_jp_disaster):
    def xml_to_df(self, xml_path, soup):
        try:
            df = pd.DataFrame(columns=self.columns)

            warning = soup.find("Warning", {"type": "気象警報・注意報（市町村等）"})
            items = warning.find_all("Item")

            for item in items:
                kind = item.find("Kind")
                area = item.find("Area").find("Name").text

                kind_name = kind.find("Name").text

                property_ = kind.find("Property")

                # {TODO}

                df.loc[len(df)] = []

            return df

        except Exception as e:
            raise  ### AirflowFailException(e)


if __name__ == "__main__":
    config_path = "disaster.json"
    etl_vpww53 = ETL_VPWW53(config_path, "extra", "VPWW53")
    etl_vpww53.xml_to_csv()
