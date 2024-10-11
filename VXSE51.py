import json
import os
import glob
from bs4 import BeautifulSoup
import pandas as pd
import shutil


def xml_to_csv(config_path):
    with open(config_path) as file:
        config = json.load(file)

    data_dir = os.path.join(config["data_dir"], "eqvol", "VXSE51")
    xml_dir = os.path.join(data_dir, "xml")

    for xml_path in glob.glob(os.path.join(xml_dir, "*.xml")):
        print(xml_path)

        with open(xml_path, "r", encoding="utf-8") as file:
            xml_data = file.read()

        soup = BeautifulSoup(xml_data, "xml")

        columns = [
            "発表時刻",
            "基点時刻",
            "都道府県",
            "最大震度（都道府県）",
            "地域",
            "最大震度（地域）",
        ]
        df = pd.DataFrame(columns=columns)

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
        csv_path = os.path.join(
            data_dir, os.path.basename(xml_path).replace(".xml", ".csv")
        )
        df.to_csv(csv_path, index=False, encoding="utf-8")
        print("Saved:", csv_path)

        # Move XML file to "converted" directory
        target_dir = os.path.join(xml_dir, "converted")
        os.makedirs(target_dir, exist_ok=True)
        target_path = os.path.join(target_dir, os.path.basename(xml_path))

        shutil.move(xml_path, target_path)
        print(f"Moved {xml_path} to {target_path}.")

    return


if __name__ == "__main__":
    config_path = "disaster.json"
    xml_to_csv(config_path)
