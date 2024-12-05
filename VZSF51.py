from disaster import ETL_jp_disaster
import pandas as pd
import os
import zipfile
import shutil


# 地上４８時間予想図
# 気象庁防災情報XMLフォーマット　技術資料: https://xml.kishou.go.jp/tec_material.html
# 電文毎の解説資料: https://xml.kishou.go.jp/jmaxml_20241031_Manual(pdf).zip


class ETL_VZSF51(ETL_jp_disaster):
    def xml_to_df(self, xml_path, soup):

        # 低気圧, 高気圧, 熱帯低気圧, 低圧部
        df_pressure = pd.DataFrame(
            columns=[
                "Title",
                "ReportDateTime",
                "TargetDateTime",
                "DateTime_text",
                "DateTime_type",
                "MeteorologicalInfos_type",
                "Property_Type_text",
                "direction_text",
                "speed_text",
                "pressure_text",
                "longitude",
                "latitude",
                "wkt",
            ]
        )

        # 台風
        df_typhoon = pd.DataFrame(
            columns=[
                "Title_text",
                "ReportDateTime_text",
                "TargetDateTime_text",
                "DateTime_text",
                "DateTime_type",
                "MeteorologicalInfos_type",
                "Property_Type_text",
                "direction_text",
                "speed_text",
                "pressure_text",
                "wind_speed_text",
                "name_text",
                "name_kana_text",
                "number_text",
                "typhoon_class_text",
                "longitude",
                "latitude",
                "wkt",
            ]
        )

        # 等圧線
        df_isobar = pd.DataFrame(
            columns=[
                "Title_text",
                "ReportDateTime_text",
                "TargetDateTime_text",
                "DateTime_text",
                "DateTime_type",
                "MeteorologicalInfos_type",
                "Property_Type_text",
                "pressure_text",
                "line_text",
            ]
        )

        # 鋒面
        df_front = pd.DataFrame(
            columns=[
                "Title_text",
                "ReportDateTime_text",
                "TargetDateTime_text",
                "DateTime_text",
                "DateTime_type",
                "MeteorologicalInfos_type",
                "Property_Type_text",
                "line_text",
            ]
        )

        # 悪天情報（強風）
        df_wind = pd.DataFrame(
            columns=[
                "Title_text",
                "ReportDateTime_text",
                "TargetDateTime_text",
                "DateTime_text",
                "DateTime_type",
                "MeteorologicalInfos_type",
                "Property_Type_text",
                "Kind_Name_text",
                "WindDegree_text",
                "WindSpeed_text",
                "longitude",
                "latitude",
                "wkt",
            ]
        )

        # 悪天情報（霧）
        df_mist = pd.DataFrame(
            columns=[
                "Title_text",
                "ReportDateTime_text",
                "TargetDateTime_text",
                "DateTime_text",
                "DateTime_type",
                "MeteorologicalInfos_type",
                "Kind_Name_text",
                "Area_Name_text",
                "Polygon_text",
            ]
        )

        # 悪天情報（海冰）
        df_sea_ice = pd.DataFrame(
            columns=[
                "Title_text",
                "ReportDateTime_text",
                "TargetDateTime_text",
                "DateTime_text",
                "DateTime_type",
                "MeteorologicalInfos_type",
                "Kind_Name_text",
                "Area_Name_text",
                "linestring_text",
            ]
        )

        Head = soup.find("Head")
        Title_text = Head.find("Title").text
        ReportDateTime_text = self.format_datetime(Head.find("ReportDateTime").text)
        TargetDateTime_text = self.format_datetime(Head.find("TargetDateTime").text)

        # <Body>
        Body = soup.find("Body")

        #   <MeteorologicalInfos>
        for MeteorologicalInfos in Body.find_all("MeteorologicalInfos"):
            MeteorologicalInfos_type = MeteorologicalInfos.get("type")

            # <MeteorologicalInfo>
            MeteorologicalInfo = MeteorologicalInfos.find("MeteorologicalInfo")

            #   <DateTime>
            DateTime = MeteorologicalInfo.find("DateTime")
            DateTime_type = DateTime.get("type")
            DateTime_text = self.format_datetime(DateTime.text)

            #   </DateTime>

            #   <Item>
            for Item in MeteorologicalInfo.find_all("Item"):

                # <Area> 悪天情報才有
                #   <Name></Name>
                #   <jmx_eb:xxx></jmx_eb:xxx>
                # </Area>

                # <Kind>
                Kind = Item.find("Kind")

                #   <Name></Name> 悪天情報才有

                #   <Property>
                Property = Kind.find("Property")

                if Property:

                    # <Type>
                    Property_Type_text = Property.find("Type").text

                    # <xxxPart>
                    Part = Property.find(lambda tag: tag.name and "Part" in tag.name)

                    #   <jmx_eb:xxx>
                    if Property_Type_text in [
                        "低気圧",
                        "高気圧",
                        "熱帯低気圧",
                        "低圧部",
                    ]:

                        # 中心位置
                        coordinate = Part.find("Coordinate", {"type": "中心位置（度）"})
                        latitude, longitude = self.process_coordinate(coordinate.text)
                        wkt = self.add_wkt(longitude, latitude)

                        # 移動方向
                        direction = Part.find(
                            "Direction", {"type": "移動方向", "unit": "度（真方位）"}
                        )

                        direction_text = direction.text

                        # 移動速度
                        speed = Part.find("Speed", {"type": "移動速度", "unit": "km/h"})
                        speed_text = speed.text

                        # 中心気圧
                        pressure = Part.find(
                            "Pressure", {"type": "中心気圧", "unit": "hPa"}
                        )

                        pressure_text = pressure.text

                        df_pressure.loc[len(df_pressure)] = [
                            Title_text,
                            ReportDateTime_text,
                            TargetDateTime_text,
                            DateTime_text,
                            DateTime_type,
                            MeteorologicalInfos_type,
                            Property_Type_text,
                            direction_text,
                            speed_text,
                            pressure_text,
                            longitude,
                            latitude,
                            wkt,
                        ]

                    elif Property_Type_text == "台風":

                        # 中心位置
                        coordinate = Item.find("Coordinate", {"type": "中心位置（度）"})
                        latitude, longitude = self.process_coordinate(coordinate.text)
                        wkt = self.add_wkt(longitude, latitude)

                        # 移動方向
                        direction = Item.find(
                            "Direction", {"type": "移動方向", "unit": "度（真方位）"}
                        )

                        direction_text = direction.text

                        # 移動速度
                        speed = Item.find("Speed", {"type": "移動速度", "unit": "km/h"})
                        speed_text = speed.text

                        # 中心気圧
                        pressure = Item.find(
                            "Pressure", {"type": "中心気圧", "unit": "hPa"}
                        )

                        pressure_text = pressure.text

                        # 最大風速
                        wind_speed = Item.find(
                            "WindSpeed", {"type": "最大風速", "unit": "m/s"}
                        )
                        wind_speed_text = wind_speed.text

                        # 呼称
                        name = Item.find("Name")
                        name_text = name.text

                        name_kana = Item.find("NameKana")
                        name_kana_text = name_kana.text

                        number = Item.find("Number")
                        number_text = number.text

                        # 階級
                        typhoon_class = Item.find(
                            "jmx_eb:TyphoonClass", {"type": "熱帯擾乱種類"}
                        )
                        typhoon_class_text = typhoon_class.text

                        df_typhoon.loc[len(df_typhoon)] = [
                            Title_text,
                            ReportDateTime_text,
                            TargetDateTime_text,
                            DateTime_text,
                            DateTime_type,
                            MeteorologicalInfos_type,
                            Property_Type_text,
                            direction_text,
                            speed_text,
                            pressure_text,
                            wind_speed_text,
                            name_text,
                            name_kana_text,
                            number_text,
                            typhoon_class_text,
                            longitude,
                            latitude,
                            wkt,
                        ]

                    elif Property_Type_text == "等圧線":

                        # 気圧
                        pressure = Part.find(
                            "Pressure", {"type": "気圧", "unit": "hPa"}
                        )
                        pressure_text = pressure.text

                        # 等圧線
                        line = Part.find("Line", {"type": "位置（度）"})
                        line_text = self.convert_to_wkt(line.text, type_="LINESTRING")

                        df_isobar.loc[len(df_isobar)] = [
                            Title_text,
                            ReportDateTime_text,
                            TargetDateTime_text,
                            DateTime_text,
                            DateTime_type,
                            MeteorologicalInfos_type,
                            Property_Type_text,
                            pressure_text,
                            line_text,
                        ]

                    elif Property_Type_text in [
                        "寒冷前線",
                        "温暖前線",
                        "停滞前線",
                        "閉塞前線",
                    ]:

                        # 鋒面
                        line = Part.find("Line", {"type": "前線（度）"})
                        line_text = self.convert_to_wkt(line.text, type_="LINESTRING")

                        df_front.loc[len(df_front)] = [
                            Title_text,
                            ReportDateTime_text,
                            TargetDateTime_text,
                            DateTime_text,
                            DateTime_type,
                            MeteorologicalInfos_type,
                            Property_Type_text,
                            line_text,
                        ]

                    elif Property_Type_text == "風":

                        Kind_Name = Kind.find("Name")
                        Kind_Name_text = Kind_Name.text

                        WindDegree = Part.find(
                            "WindDegree", {"type": "風向", "unit": "度（真方位）"}
                        )
                        WindDegree_text = WindDegree.text

                        WindSpeed = Part.find(
                            "WindSpeed", {"type": "最大風速", "unit": "ノット"}
                        )
                        WindSpeed_text = WindSpeed.text

                        coordinate = Item.find("Coordinate", {"type": "位置（度）"})
                        latitude, longitude = self.process_coordinate(coordinate.text)
                        wkt = self.add_wkt(longitude, latitude)

                        df_wind.loc[len(df_wind)] = [
                            Title_text,
                            ReportDateTime_text,
                            TargetDateTime_text,
                            DateTime_text,
                            DateTime_type,
                            MeteorologicalInfos_type,
                            Property_Type_text,
                            Kind_Name_text,
                            WindDegree_text,
                            WindSpeed_text,
                            longitude,
                            latitude,
                            wkt,
                        ]

                    else:
                        print("Unexpected property type:", Property_Type_text)
                        raise

                #       </jmx_eb:xxx>

                #     </xxxPart>

                #   </Property>

                else:  # 沒有Property (悪天情報（霧）, 悪天情報（海氷）, 悪天情報（船体着氷）)
                    Kind_Name = Kind.find("Name")
                    Kind_Name_text = Kind_Name.text

                    if Kind_Name_text == "悪天情報（霧）":
                        Area_Name = Item.find("Area").find("Name")

                        if Area_Name:
                            Area_Name_text = Area_Name.text

                        polygon = Item.find("Polygon", {"type": "位置（度）"})

                        if polygon:
                            polygon_text = polygon.text
                            polygon_text = self.convert_to_wkt(
                                polygon.text, type_="POLYGON"
                            )

                            df_mist.loc[len(df_mist)] = [
                                Title_text,
                                ReportDateTime_text,
                                TargetDateTime_text,
                                DateTime_text,
                                DateTime_type,
                                MeteorologicalInfos_type,
                                Kind_Name_text,
                                Area_Name_text,
                                polygon_text,
                            ]

                    elif Kind_Name_text in ["悪天情報（海氷）", "悪天情報（船体着氷）"]:
                        Area_Name = Item.find("Area").find("Name")
                        Area_Name_text = Area_Name.text

                        coordinate = Item.find("Coordinate", {"type": "領域（度）"})

                        if coordinate:
                            linestring_text = self.convert_to_wkt(
                                line.text, type_="LINESTRING"
                            )

                            df_sea_ice.loc[len(df_sea_ice)] = [
                                Title_text,
                                ReportDateTime_text,
                                TargetDateTime_text,
                                DateTime_text,
                                DateTime_type,
                                MeteorologicalInfos_type,
                                Kind_Name_text,
                                Area_Name_text,
                                linestring_text,
                            ]

                    else:
                        print("Unexpected kind name:", Kind_Name_text)
                        raise

                # </Kind>

            #   </Item>

            # </MeteorologicalInfo>

        #   </MeteorologicalInfos>

        # </Body>

        df_dict = {
            "pressure": df_pressure,
            "typhoon": df_typhoon,
            "isobar": df_isobar,
            "front": df_front,
            "wind": df_wind,
            "mist": df_mist,
            "sea_ice": df_sea_ice,
        }

        return df_dict

    def df_to_csv(self, df_dict, xml_path):

        for key in df_dict.keys():

            df = df_dict[key]

            csv_path = os.path.join(
                self.config["data_dir"],
                self.feed,
                f"VZS_{key}",
                os.path.basename(xml_path).replace(".xml", f"_{key}.csv"),
            )

            if not df.empty:
                os.makedirs(os.path.dirname(csv_path), exist_ok=True)
                df.to_csv(csv_path, index=False, encoding="utf-8")
                print("Saved:", csv_path)

        # Compress XML file
        zip_path = xml_path.replace(".xml", ".zip")

        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(xml_path, os.path.basename(xml_path))

        print("Zipped:", zip_path)

        # Remove XML file
        os.remove(xml_path)
        print("Removed:", xml_path)

        # Move zip file to "converted" directory
        target_dir = os.path.join(self.data_dir, "xml", "converted")
        os.makedirs(target_dir, exist_ok=True)
        target_path = os.path.join(target_dir, os.path.basename(zip_path))

        shutil.move(zip_path, target_path)
        print(f"Moved {zip_path} to {target_path}\n")


if __name__ == "__main__":
    config_path = "disaster.json"
    etl_vzsf51 = ETL_VZSF51(config_path, "regular", "VZSF51")
    etl_vzsf51.xml_to_csv()
