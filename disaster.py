import json
import requests
import os
from bs4 import BeautifulSoup
import glob
import shutil


class XML_saver:
    def __init__(self, config_path):
        with open(config_path) as file:
            self.config = json.load(file)

    def save_feed_xml(self):
        for feed in self.config["feed_urls"]:
            dir_path = os.path.join(self.config["data_dir"], feed)
            os.makedirs(dir_path, exist_ok=True)

            url = self.config["feed_urls"][feed]
            print("URL:", url)

            response = requests.get(url)
            response.encoding = "utf-8"
            xml = response.text

            xml_path = os.path.join(dir_path, f"{feed}.xml")

            with open(xml_path, "w") as file:
                file.write(xml)

            print("Saved:", xml_path)

    def get_data_urls(self):
        urls = {}

        for feed in self.config["feed_urls"]:
            urls[feed] = []
            xml_path = os.path.join(self.config["data_dir"], feed, f"{feed}.xml")

            with open(xml_path, "r", encoding="utf-8") as file:
                content = file.read()

            soup = BeautifulSoup(content, "xml")

            for entry in soup.find_all("entry"):
                urls[feed].append(entry.find("id").text)

        return urls

    def save_data_xml(self, urls):
        for feed in urls:
            for url in urls[feed]:
                xml_name = url.split("/")[-1]
                code = xml_name.split("_")[-2]
                time = xml_name.split("_")[-4]

                dir_path = os.path.join(self.config["data_dir"], feed, code, "xml")
                os.makedirs(dir_path, exist_ok=True)
                xml_path = os.path.join(dir_path, xml_name)

                if os.path.exists(xml_path) or os.path.exists(
                    os.path.join(xml_path, "converted")
                ):
                    print("Exists:", xml_path)
                    break

                else:
                    response = requests.get(url)
                    response.encoding = "utf-8"
                    xml = response.text

                    soup = BeautifulSoup(xml, "xml")
                    pretty_xml = soup.prettify()

                    with open(xml_path, "w") as file:
                        file.write(pretty_xml)

                    print("Saved:", xml_path)

    def save_xml(self):
        self.save_feed_xml()
        urls = self.get_data_urls()
        self.save_data_xml(urls)


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
            df = self.xml_to_df(xml_path, soup)
            self.df_to_csv(df, xml_path)

    def format_datetime(self, text):
        return text.replace("T", " ").replace("+09:00", "")

    def full_width_to_float(self, text):
        return float(
            text.translate(str.maketrans("０１２３４５６７８９．", "0123456789."))
        )

    def dms_to_decimal(self, dms):  # e.g. 3624.38
        degree = int(dms // 100)  # Extract the degree part. e.g. 36
        minutes = dms % 100  # Extract the minutes part. e.g. 24.38
        return degree + round(minutes / 60.0, 4)  # e.g. 36 + 24.38 / 60

    def process_coordinate(self, coordinate, format_="decimal"):
        coordinate = coordinate.replace("/", "").replace("-", "+-").split("+")

        latitude = float(coordinate[1])
        longitude = float(coordinate[2])

        if format_ == "dms":
            latitude = self.dms_to_decimal(latitude)
            longitude = self.dms_to_decimal(longitude)

        height = int(coordinate[3])

        return latitude, longitude, height

    def add_wkt(self, longitude, latitude):
        return f"POINT({longitude} {latitude})"


if __name__ == "__main__":
    xml_saver = XML_saver("disaster.json")
    xml_saver.save_xml()
