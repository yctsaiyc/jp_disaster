import json
import os
import glob
from bs4 import BeautifulSoup
import pandas as pd
import shutil


class ETL_jp_disaster:
    def __init__(self, config_path, feed, code):
        with open(config_path) as file:
            self.config = json.load(file)

        self.feed = feed
        self.code = code
        self.columns = self.config["columns"][feed][code]
        self.data_dir = os.path.join(self.config["data_dir"], self.feed, self.code)


    def df_to_csv(self, df, xml_path):
        csv_path = os.path.join(
            self.data_dir, os.path.basename(xml_path).replace(".xml", ".csv")
        )
        df.to_csv(csv_path, index=False, encoding="utf-8")
        print("Saved:", csv_path)
    
        # Move XML file to "converted" directory
        target_dir = os.path.join(self.data_dir, "xml", "converted")
        os.makedirs(target_dir, exist_ok=True)
        target_path = os.path.join(target_dir, os.path.basename(xml_path))
    
        shutil.move(xml_path, target_path)
        print(f"Moved {xml_path} to {target_path}")


    def xml_to_df(self, xml_path, soup):
        raise NotImplementedError("Subclasses must implement this method")


    def xml_to_csv(self):
        for xml_path in glob.glob(os.path.join(self.data_dir, "xml", "*.xml")):
            print("Processing:", xml_path)
    
            with open(xml_path, "r", encoding="utf-8") as file:
                xml_data = file.read()
    
            soup = BeautifulSoup(xml_data, "xml")
            self.xml_to_df(xml_path, soup)


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
