from VPTW40 import ETL_VPTW40


class ETL_VPTW60(ETL_VPTW40):
    pass


if __name__ == "__main__":
    config_path = "disaster.json"
    etl_vptw60 = ETL_VPTW60(config_path, "extra", "VPTW60")
    etl_vptw60.xml_to_csv()
