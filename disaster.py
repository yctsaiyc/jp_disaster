import json
import requests
import os
from bs4 import BeautifulSoup


config = {}


def load_config(config_path):
    global config

    with open(config_path) as file:
        config = json.load(file)


def save_feed_xml():
    for feed in config["feed_urls"]:
        dir_path = os.path.join(config["data_dir"], feed)
        os.makedirs(dir_path, exist_ok=True)

        url = config["feed_urls"][feed]
        print("URL:", url)

        response = requests.get(url)
        response.encoding = "utf-8"
        xml = response.text

        xml_path = os.path.join(dir_path, f"{feed}.xml")

        with open(xml_path, "w") as file:
            file.write(xml)

        print("Saved:", xml_path)

    return


def get_data_urls():
    urls = {}

    for feed in config["feed_urls"]:
        urls[feed] = []
        xml_path = os.path.join(config["data_dir"], feed, f"{feed}.xml")

        with open(xml_path, "r", encoding="utf-8") as file:
            content = file.read()

        soup = BeautifulSoup(content, "xml")

        for entry in soup.find_all("entry"):
            urls[feed].append(entry.find("id").text)

    return urls


def save_data_xml(urls):
    for feed in urls:
        for url in urls[feed]:
            xml_name = url.split("/")[-1]
            code = xml_name.split("_")[-2]
            time = xml_name.split("_")[-4]

            dir_path = os.path.join(config["data_dir"], feed, code, "xml")
            os.makedirs(dir_path, exist_ok=True)
            xml_path = os.path.join(dir_path, xml_name)

            if os.path.exists(xml_path):
                print("Exists:", xml_path)
                break

            else:
                response = requests.get(url)
                response.encoding = "utf-8"
                xml = response.text

                with open(xml_path, "w") as file:
                    file.write(xml)

                print("Saved:", xml_path)


def main(config_path):
    config = load_config(config_path)
    save_feed_xml()
    urls = get_data_urls()
    save_data_xml(urls)


if __name__ == "__main__":
    main("disaster.json")
