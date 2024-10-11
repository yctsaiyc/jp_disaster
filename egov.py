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
    dir_path = os.path.join(config["data_dir"], "feed")
    os.makedirs(dir_path, exist_ok=True)

    for feed in config["feed_urls"]:
        url = config["feed_urls"][feed]
        print("url", url)

        response = requests.get(url)
        response.encoding = "utf-8"
        xml = response.text

        xml_path = os.path.join(dir_path, f"{feed}.xml")

        with open(xml_path, "w") as file:
            file.write(xml)

        print("saved", xml_path)

    return


def get_data_urls():
    feed_dir_path = os.path.join(config["data_dir"], "feed")
    urls = {}

    for feed in config["feed_urls"]:
        urls[feed] = []
        xml_path = os.path.join(feed_dir_path, f"{feed}.xml")

        with open(xml_path, "r", encoding="utf-8") as file:
            content = file.read()

        soup = BeautifulSoup(content, "xml")

        for entry in soup.find_all("entry"):
            urls[feed].append(entry.find("id").text)

    return urls


def main():
    config = load_config("egov.json")
    # save_feed_xml()
    urls = get_data_urls()


if __name__ == "__main__":
    main()
