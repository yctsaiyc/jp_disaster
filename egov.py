import json
import requests
import os


config = {}


def load_config(config_path):
    global config

    with open(config_path) as file:
        config = json.load(file)


def save_feed_xml():
    dir_path = os.path.join(config["data_dir"], "feed")
    os.makedirs(dir_path, exist_ok=True)

    for name in config["feed_urls"]:
        url = config["feed_urls"][name]
        print("url", url)

        response = requests.get(url)
        response.encoding = 'utf-8'
        xml = response.text

        xml_path = os.path.join(dir_path, f"{name}.xml")

        with open(xml_path, "w") as file:
            file.write(xml)

        print("saved", xml_path)

    return


def main():
    config = load_config("egov.json")
    save_feed_xml()


if __name__ == "__main__":
    main()
