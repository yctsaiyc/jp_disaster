from VFVO53 import ETL_VFVO53


class ETL_VFVO54(ETL_VFVO53):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.type = "降灰予報（速報）"


if __name__ == "__main__":
    etl_vfvo54 = ETL_VFVO54("disaster.json", "eqvol", "VFVO54")
    etl_vfvo54.xml_to_csv()
