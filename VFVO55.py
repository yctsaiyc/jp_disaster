from VFVO53 import ETL_VFVO53


class ETL_VFVO55(ETL_VFVO53):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.type = "降灰予報（詳細）"


if __name__ == "__main__":
    etl_vfvo55 = ETL_VFVO55("disaster.json", "eqvol", "VFVO55")
    etl_vfvo55.xml_to_csv()
