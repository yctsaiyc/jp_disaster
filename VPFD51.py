from disaster import ETL_jp_disaster
import pandas as pd
import json
import os
import zipfile
import shutil


# 府県天気予報（Ｒ１）
# 気象庁防災情報XMLフォーマット　技術資料: https://xml.kishou.go.jp/tec_material.html
# 電文毎の解説資料: https://xml.kishou.go.jp/jmaxml_20241031_Manual(pdf).zip


class ETL_VPFD51(ETL_jp_disaster):
    def __init__(self, config_path, feed, code):
        with open(config_path) as file:
            self.config = json.load(file)

        self.feed = feed
        self.code = code
        self.columns_1 = self.config["columns"][feed][f"{code}_1"]
        self.columns_2 = self.config["columns"][feed][f"{code}_2"]
        self.data_dir = os.path.join(self.config["data_dir"], self.feed, self.code)

    def df_to_csv(self, df_tuple, xml_path):
        for idx, df in enumerate(df_tuple):
            csv_path = os.path.join(
                self.data_dir,
                str(idx + 1),
                os.path.basename(xml_path).replace(".xml", f"_{idx+1}.csv"),
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

    def parse_xml(self, xml, tag_name, DateTime_dict, Name_dict):
        data_dict = {}

        if tag_name == "WeatherPart":
            for jmx in xml.find_all("jmx_eb:Weather"):
                refID = jmx.get("refID")

                data_dict[refID] = {
                    "DateTime": DateTime_dict.get(refID),
                    "Name": Name_dict.get(refID),
                    "type": jmx.get("type"),
                    "value": jmx.text,
                }

        elif tag_name == "WindForecastPart":
            for WindForecast in xml.find_all("WindForecastPart"):
                refID = WindForecast.get("refID")
                jmx = WindForecast.find("jmx_eb:WindDirection")

                data_dict[refID] = {
                    "DateTime": DateTime_dict.get(refID),
                    "Name": Name_dict.get(refID),
                    "type": jmx.get("type"),
                    "value": jmx.text,
                }

        elif tag_name == "WaveHeightForecastPart":
            for WaveHeightForecast in xml.find_all("WaveHeightForecastPart"):
                refID = WaveHeightForecast.get("refID")
                jmx = WaveHeightForecast.find("jmx_eb:WaveHeight")

                data_dict[refID] = {
                    "DateTime": DateTime_dict.get(refID),
                    "Name": Name_dict.get(refID),
                    "type": jmx.get("type"),
                    "value": jmx.text,
                }

        elif tag_name == "ProbabilityOfPrecipitationPart":
            for jmx in xml.find_all("jmx_eb:ProbabilityOfPrecipitation"):
                refID = jmx.get("refID")

                data_dict[refID] = {
                    "DateTime": DateTime_dict.get(refID),
                    "Name": Name_dict.get(refID),
                    "type": jmx.get("type"),
                    "value": jmx.text,
                }

        elif tag_name == "TemperaturePart":
            for jmx in xml.find_all("jmx_eb:Temperature"):
                refID = jmx.get("refID")

                data_dict[refID] = {
                    "DateTime": DateTime_dict.get(refID),
                    "Name": Name_dict.get(refID),
                    "type": jmx.get("type"),
                    "value": jmx.text,
                }

        elif tag_name == "WindDirectionPart":
            for jmx in xml.find_all("jmx_eb:WindDirection"):
                refID = jmx.get("refID")

                data_dict[refID] = {
                    "DateTime": DateTime_dict.get(refID),
                    "Name": Name_dict.get(refID),
                    "type": jmx.get("type"),
                    "value": jmx.text,
                }

        elif tag_name == "WindSpeedPart":
            for jmx in xml.find_all("WindSpeedLevel"):
                refID = jmx.get("refID")

                data_dict[refID] = {
                    "DateTime": DateTime_dict.get(refID),
                    "Name": Name_dict.get(refID),
                    "type": jmx.get("type"),
                    "value": jmx.text,
                }

        return data_dict

    def xml_to_df(self, xml_path, soup):
        tmp_columns = [
            "情報名称",
            "発表時刻",
            "基点時刻",
            "予報の項目",
            "都道府県",
            "対象地域",
            "気象要素名",
            "予報期間の始めの時刻",
            "予報の対象日",
            "type",
            "value",
        ]

        df = pd.DataFrame(columns=tmp_columns)

        # 2 各部の構成と内容
        # (1)管理部
        # 1管理部の構成
        # Control
        # └ Title 情報名称
        Title = soup.find("Title").text

        # └ DateTime 発表時刻
        # └ Status 運用種別
        # └ EditorialOffice 編集官署名
        # └ PublishingOffice 発表官署名

        # (2) ヘッダ部
        # 1 ヘッダ部の構成
        # Head
        # └ Title 標題
        prefecture = soup.find("Head").find("Title").text.replace("府県天気予報", "")

        # └ ReportDateTime 発表時刻
        ReportDateTime = self.format_datetime(soup.find("ReportDateTime").text)

        # └ TargetDateTime 基点時刻
        TargetDateTime = self.format_datetime(soup.find("TargetDateTime").text)

        # └ TargetDuration 基点時刻からの取りうる時間
        # └ EventID 識別情報
        # └ InfoType 情報形態
        # └ Serial 情報番号
        # └ InfoKind スキーマの運用種別情報
        # └ InfoKindVersion スキーマの運用種別情報のバージョン
        # └ Headline 見出し要素

        # (3) 内容部
        # 1 内容部の構成
        # Body
        # └ MeteorologicalInfos 予報の項目
        #     └ TimeSeriesInfo 時系列情報
        #     └ MeteorologicalInfo 予報の内容

        # 2Body 部の詳細
        # MeteorologicalInfos
        MeteorologicalInfos_all = soup.find_all("MeteorologicalInfos")

        for MeteorologicalInfos in MeteorologicalInfos_all:

            # 予報の項目を属性 type で指定する。属性 type は“区域予報”、“地点予報”、“独自予報”の値をとる。
            # “区域予報”の場合は、天気予報文(3個別要素の詳細の※1参照)
            # 又は地域時系列予報の天気・風(3個別要素の詳細の※4参照)、
            # “地点予報”の場合は、予想気温(3個別要素の詳細の※2参照)
            # 又は地域時系列予報の気温(3個別要素の詳細の※5参照)、
            # “独自予報”の場合は、独自予報(3個別要素の詳細の※3参照)を記述する。
            MeteorologicalInfos_type = MeteorologicalInfos.get("type")

            # └ TimeSeriesInfo
            # MeteorologicalInfos の属性 type で指定した予報の項目を時系列情報として記述する。
            TimeSeriesInfo_all = MeteorologicalInfos.find_all("TimeSeriesInfo")

            if not TimeSeriesInfo_all:  ## 独自予報
                continue

            for TimeSeriesInfo in TimeSeriesInfo_all:

                # └ MeteorologicalInfo
                # MeteorologicalInfos の属性 type で指定した予報の項目を記述する。
                ## MeteorologicalInfo = MeteorologicalInfos.find("MeteorologicalInfo")  ## 独自予報

                # 3個別要素の詳細
                # ※1(1) 区域予報「天気予報文」の詳細
                # TimeSeriesInfo 時系列情報
                # └ TimeDefines 時系列の時刻定義セット
                # └ TimeDefine 個々の時刻定義
                # └ DateTime 基点時刻
                # └ Duration 対象期間
                # └ Name 予報時間の内容
                # └ Item ※1(1)-1 「天気・風・波予報」の詳細を参照

                # TimeSeriesInfo
                # └ TimeDefines
                # 予報の対象期間を示すとともに、対応する要素の timeId を記述する。
                #     └ TimeDefine
                #     同一 TimeSeriesInfo 内にある要素の ID(refID)に対応する ID(timeId)を記述する。
                #     ID は 1、2 または 1~3。ID で示す、予報対象数と同数を繰り返して記述する。
                #     └ DateTime
                #     予報期間の始めの時刻を示す。“2008-01-10T05:00:00+09:00”のように日本標準時で記述する。
                #     └ Duration
                #     予報期間の長さを示す。“PT19H”“PT1D”など今日予報は24時までの長さ(時間)、明日予報と明後日予報は1日で記述する。
                #     └ Name
                #     予報の対象日を“今日”、“明日”、“明後日”のいずれかで記述する。
                #     発表する時刻によって“今日”は “今夜”とする場合がある。また、“明後日”がないことがある。
                result = self.parse_TimeDefines(TimeSeriesInfo)

                if isinstance(result, tuple):
                    DateTime_dict, Name_dict = result
                else:
                    DateTime_dict, Name_dict = result, {}

                # └ Item
                # 天気、風、波予報と、予報区を記述する。
                # 府県予報区に含まれる発表予報区の数だけ繰り返す。※1(1)-1参照。
                Item_all = TimeSeriesInfo.find_all("Item")

                for Item in Item_all:
                    # ※1(1)-1 「天気・風・波予報」の詳細
                    # Item 予報の内容
                    # └ Kind 個々の予報の内容
                    # └ Property 予報要素
                    # └ Type 気象要素名(“天気”の場合)
                    # └ DetailForecast ※1(1)-1-1(1) 「天気予報文」の詳細を参照
                    # └ WeatherPart ※1(1)-1-1(2) 「テロップ用天気予報用語の天気」の詳細を参照
                    # └ WeatherCodePart ※1(1)-1-1(3) 「天気予報用テロップ番号」の詳細を参照
                    # └ Kind 個々の予報の内容
                    # └ Property 予報要素
                    # └ Type 気象要素名(“風”の場合)
                    # └ DetailForecast ※1(1)-1-2 「風の予報文」の詳細を参照
                    # └ Kind 個々の予報の内容
                    # └ Property 予報要素
                    # └ Type 気象要素名(“波”の場合)
                    # └ DetailForecast ※1(1)-1-3 「波の予報文」の詳細を参照
                    # └ Area 対象地域
                    # └ Name 対象地域の名称
                    # └ Code 対象地域のコード
                    try:
                        Area_Name = Item.find("Area").find("Name").text

                    except AttributeError:
                        Area_Name = Item.find("Station").find("Name").text

                    # Item
                    # └ Kind 予報を記述する。
                    Kind_all = Item.find_all("Kind")

                    for Kind in Kind_all:
                        # └ Property 予報要素を記述する。
                        Property_all = Kind.find_all("Property")

                        for Property in Property_all:

                            #     └ Type 気象要素名を記述する。Type の値が“天気”の場合は天気予報文を記述する。
                            Property_Type = Property.find("Type").text

                            #     └ DetailForecast 予報文を記述する。※1(1)-1-1(1)参照。
                            data_dict = {}

                            if Property_Type == "天気":
                                ## DetailForecast資訊和WeatherPart重複故不採用

                                # └ WeatherPart テロップ用天気予報用語の天気を記述する。※1(1)-1-1(2)参照。
                                WeatherPart = Property.find("WeatherPart")
                                data_dict = self.parse_xml(
                                    WeatherPart, "WeatherPart", DateTime_dict, Name_dict
                                )

                                # └ WeatherCodePart 天気予報用テロップ番号を記述する。※1(1)-1-1(3)参照。

                            # └ Kind 予報を記述する。
                            # └ Property 予報要素を記述する。
                            #     └ Type 気象要素名を記述する。Type の値が“風”の場合は DetailForecast に風の予報を記述する。
                            elif Property_Type == "風":

                                # └ DetailForecast 予報文を記述する。※1(1)-1-2
                                DetailForecast = Property.find("DetailForecast")
                                data_dict = self.parse_xml(
                                    DetailForecast,
                                    "WindForecastPart",
                                    DateTime_dict,
                                    Name_dict,
                                )

                            # └ Kind 予報を記述する。
                            # └ Property 予報要素を記述する。
                            #     └ Type 気象要素名を記述する。Type の値が“波”の場合は DetailForecast に波の予報を記述する。
                            #             なお、波予報を行っていない発表予報区では Kind 以下を省略する。
                            elif Property_Type == "波":

                                # └ DetailForecast 予報文を記述する。※1(1)-1-3参照。
                                DetailForecast = Property.find("DetailForecast")
                                data_dict = self.parse_xml(
                                    DetailForecast,
                                    "WaveHeightForecastPart",
                                    DateTime_dict,
                                    Name_dict,
                                )

                            # └ Area 発表予報区を記述する。
                            # └ Name 発表予報区の名称を、“東京地方”“大阪府”などと記述する。
                            # └ Code 発表予報区のコード番号を、“130010”“270000”などと記述する。

                            # ※1(2)-1 「降水確率」の詳細
                            # Item 予報の内容
                            # └ Kind 個々の予報の内容
                            # └ Property 予報要素
                            #     └ Type 気象要素名
                            #     └ ProbabilityOfPrecipitationPart ※1(2)-1-1 「降水確率」の予報文の詳細を参照
                            # └ Area 対象地域
                            # └ Name 対象地域の名称
                            # └ Code 対象地域のコード

                            # Item
                            # └ Kind 予報を記述する。
                            # └ Property 予報要素を記述する。
                            # └     Type 気象要素名を記述する。Type の値が“降水確率”の場合、降水確率の予報を記述する。
                            elif Property_Type == "降水確率":

                                # └ ProbabilityOfPrecipitationPart 降水確率(数値)と降水指示符を記述する。※1(2)-1-1参照。
                                ProbabilityOfPrecipitationPart = Property.find(
                                    "ProbabilityOfPrecipitationPart"
                                )

                                data_dict = self.parse_xml(
                                    ProbabilityOfPrecipitationPart,
                                    "ProbabilityOfPrecipitationPart",
                                    DateTime_dict,
                                    Name_dict,
                                )

                                # └ Area 発表予報区を記述する。
                                # └ Name 発表予報区の名称を、“東京地方”“大阪府”などと記述する。
                                # └ Code 発表予報区のコード番号を、“130010”“270000”などと記述する。

                            # ※2 地点予報「予想気温」の詳細
                            # TimeSeriesInfo 時系列情報
                            # └ TimeDefines 時系列の時刻定義セット
                            # └ TimeDefine 個々の時刻定義
                            # └ DateTime 基点時刻
                            # └ Duration 対象期間
                            # └ Name 予報時間の内容
                            # └ Item ※2-1 「予想気温」の詳細を参照

                            # TimeSeriesInfo
                            # └ TimeDefines 予報の対象期間を示すとともに、対応する要素の timeId を記述する。
                            # └ TimeDefine 同一 TimeSeriesInfo 内にある要素の ID(refID)に対応する ID(timeId)を定義する。ID は 1、2 または 1~4。
                            # ID で示す、予報対象数(「今日日中」、「今日」、「明日朝」、「明日日中」の4回または「明日朝」、「明日日中」の2
                            # 回)と同数を繰り返して記述する。

                            # └ DateTime
                            # 予報期間の始めの時刻を示す。“2008-01-10T05:00:00+09:00”のように日本標準時で記述する。
                            # 「今日」は今日0時、「今日日中」は今日9時、「明日朝」は明日0時、「明日日中」は明日9時。

                            # └ Duration
                            # 予報期間の長さを示す。“PT9H”のように記述する。
                            # 「今日」は24時間、「今日日中」、「明日日中」と「明日朝」は9時間。

                            # └ Name
                            # 予報の対象期間の名称を記述する(“今日日中”、“今日”、“明日朝”、“明日日中”)。
                            # 予報発表時刻によって「今日」と「今日日中」が省略される。

                            # └ Item
                            # 予想気温と、予想地点を記述する。府県予報区に含まれる発表予想地点の数だけ繰り返す。※2-1参照。

                            # ※2-1 「予想気温」の詳細
                            # Item 予報の内容
                            # └ Kind 個々の予報の内容
                            # └ Property 予報要素
                            # └ Type 気象要素名
                            # └ TemperaturePart ※2-1-1 「予想気温」の予報文の詳細を参照
                            # └ Station 発表予想地点
                            # └ Name 発表予想地点の名称
                            # └ Code 発表予想地点のコード

                            # Item
                            # └ Kind 予報を記述する。
                            # └ Property 予報要素を記述する。
                            # └ Type 気象要素名を記述する。TemperaturePart に記述する予想気温の内容を示し、
                            # Type の値は“日中の最高気温”、“最高気温”“朝の最低気温”の値をとる。
                            elif Property_Type in [
                                "日中の最高気温",
                                "最高気温",
                                "朝の最低気温",
                            ]:

                                # └ TemperaturePart 予想気温を記述する。※2-1-1参照。
                                TemperaturePart = Property.find("TemperaturePart")

                                data_dict = self.parse_xml(
                                    TemperaturePart,
                                    "TemperaturePart",
                                    DateTime_dict,
                                    Name_dict,
                                )

                                # └ Station 発表予想地点を記述する。※
                                # └ Name 発表予想地点の名称を、“東京”“大阪”などと記述する。
                                # └ Code 発表予想地点のコード番号を、“44132”“62078”などと記述する。

                                # ※対象地点は府県天気予報・府県週間天気予報_解説資料付録を参照のこと

                            # ※3 「独自予報」の詳細
                            # MeteorologicalInfo
                            # └ DateTime 予報の基点時刻
                            # └ Duration 予報期間の長さ
                            # └ Item 予報の内容
                            # └ Kind 個々の予報の内容
                            # └ Property 予報要素
                            # └ Type 要素名
                            # └ Text 予報の内容(テキスト)
                            # └ Area 対象地域
                            # └ Name 対象地域の名称
                            # └ Code 対象地域のコード

                            # MeteorologicalInfo
                            # └ DateTime 予報期間の始めの時刻を示す。
                            # “2008-01-10T05:00:00+09:00”のように日本標準時で記述する。
                            # └ Duration 予報期間の長さを示す。
                            # “P1DT19H”のように予報期間の始めの時刻から明日もしくは明後日24時までの長さを記述する。

                            # └ Item
                            # └ Kind 予報を記述する。
                            # └ Property 予報要素を記述する。
                            # └ Type 気象要素名を記述する。Type の値が、“独自予報”の場合は、独自予報を記述する。
                            # └ Text 独自予報文を平文(かな漢字)で記述する。
                            # └ Area 発表予報区(府県予報区)を記述する。“独自予報”の発表単位は府県予報区とする。
                            # └ Name 府県予報区の名称を、“神奈川県”などと記述する。
                            # └ Code 府県予報区のコード番号を、“140000”などと記述する。

                            # ※4 区域予報「天気、風の地域時系列予報」の詳細
                            # TimeSeriesInfo 時系列情報
                            # └ TimeDefines 時系列の時刻定義セット
                            # └ TimeDefine 個々の時刻定義
                            # └ DateTime 基点時刻
                            # └ Duration 対象期間
                            # └ Item 予報時間の内容 ※4-1 「天気、風の地域時系列予報」の詳細を参照

                            # TimeSeriesInfo
                            # └ TimeDefines 予報の対象期間を示すとともに、対応する要素の timeId を記述する。
                            # └ TimeDefine 同一 TimeSeriesInfo 内にある要素の ID(refID)に対応する ID(timeId)を記述する。
                            # ID は 1~10、1~12、1~14。ID で示す、予報対象数と同数(10回、12回、14回)を繰り返して記述する。
                            # └ DateTime 予報期間の始めの時刻を示す。“2008-01-10T05:00:00+09:00”のように日本標準時で記述する。
                            # └ Duration 予報期間の長さを示す。“PT3H”のように3時間固定で記述する。
                            # └ Item 天気、風の地域時系列予報と、予報区を記述する。
                            # 府県予報区に含まれる発表予報区の数だけ繰り返す。
                            # ※4-1参照。

                            # ※4-1 「天気、風の地域時系列予報」の詳細
                            # Item 予報の内容
                            # └ Kind 個々の予報の内容
                            # └ Property 予報要素
                            # └ Type 気象要素名
                            # └ WeatherPart ※4-1-1(1) 「3時間内卓越天気」の詳細を参照
                            # └ Kind 個々の予報の内容
                            # └ Property 予報要素
                            # └ Type 気象要素名
                            # └ WindDirectionPart ※4-1-1(2) 「3時間内代表風の風向」の詳細を参照
                            # └ WindSpeedPart ※4-1-1(3) 「3時間内代表風の風速階級」の詳細を参照
                            # └ Area 対象地域
                            # └ Name 対象地域の名称
                            # └ Code 対象地域のコード

                            # Item
                            # └ Kind 予報を記述する。
                            # └ Property 予報要素を記述する。
                            # └ Type 気象要素名を記述する。
                            # Type が“3時間内卓越天気”の場合、WeatherPart に3時間内卓越天気の予報を記述する。
                            elif Property_Type == "３時間内卓越天気":
                                # └ WeatherPart 天気を記述する。※4-1-1(1) 「3時間内卓越天気」の詳細を参照。
                                WeatherPart = Property.find("WeatherPart")

                                data_dict = self.parse_xml(
                                    WeatherPart,
                                    "WeatherPart",
                                    DateTime_dict,
                                    Name_dict,
                                )

                            # └ Kind 予報を記述する。
                            # └ Property 予報要素を記述する。
                            # └ Type 気象要素名を記述する。
                            # Type が“3時間内代表風”の場合、WindDirectionPart、WindSpeedPart に3時間内代表風の予報を記述する。
                            elif Property_Type == "３時間内代表風":
                                # └ WindDirectionPart 風向を記述する。※4-1-1(2)参照。
                                WindDirectionPart = Property.find("WindDirectionPart")

                                data_dict = self.parse_xml(
                                    WindDirectionPart,
                                    "WindDirectionPart",
                                    DateTime_dict,
                                    Name_dict,
                                )

                                for row in data_dict.values():
                                    df.loc[len(df)] = [
                                        Title,  # 情報名称
                                        ReportDateTime,  # 発表時刻
                                        TargetDateTime,  # 基点時刻
                                        MeteorologicalInfos_type,  # 予報の項目
                                        prefecture,  # 都道府県
                                        Area_Name,  # 対象地域
                                        Property_Type,  # 気象要素名
                                        row["DateTime"],  # 予報期間の始めの時刻
                                        row["Name"],  # 予報の対象日
                                        row["type"],  # type
                                        row["value"],  # value
                                    ]

                                # └ WindSpeedPart 風速(風速階級)を記述する。※4-1-1(3) 参照。
                                WindSpeedPart = Property.find("WindSpeedPart")

                                data_dict = self.parse_xml(
                                    WindSpeedPart,
                                    "WindSpeedPart",
                                    DateTime_dict,
                                    Name_dict,
                                )

                                # └ Area 発表予報区を記述する。
                                # └ Name 発表予報区の名称を、“東京地方”“大阪府”などと記述する。
                                # └ Code 発表予報区のコード番号を、“130010”“270000”などと記述する。

                            # ※5 地点予報「気温の地域時系列予報」の詳細
                            # TimeSeriesInfo 時系列情報
                            # └ TimeDefines 時系列の時刻定義セット
                            # └ TimeDefine 個々の時刻定義
                            # └ DateTime 基点時刻
                            # └ Item ※5-1 「気温の地域時系列予報」の詳細を参照

                            # TimeSeriesInfo
                            # └ TimeDefines 予報の対象期間を示すとともに、対応する要素の timeId を記述する。
                            # └ TimeDefine 同一 TimeSeriesInfo 内にある要素の ID(refID)に対応する ID(timeId)を記述する。
                            # ID は 1~11、1~13、1~15。ID で示す、予報対象数と同数(11回、13回、15回)を繰り返して記述する。
                            # └ DateTime 予報時刻を示す。“2008-01-10T06:00:00+09:00”のように日本標準時で記述する。
                            # └ Item 気温の地域時系列予報と、予想地点を記述する。府県予報区に含まれる発表予想地点の数だけ繰り返す。※5-1参照。

                            # ※5-1 「気温の地域時系列予報」の詳細
                            # Item 予報の内容
                            # └ Kind 個々の予報の内容
                            # └ Property 予報要素
                            # └ Type 要素名
                            # └ TemperaturePart ※5-1-1 「3時間毎気温」の詳細を参照
                            # └ Station 発表予想地点
                            # └ Name 発表予想地点の名称
                            # └ Code 発表予想地点のコード

                            # Item
                            # └ Kind 予報を記述する。
                            # └ Property 予報要素を記述する。
                            # └ Type 気象要素名を記述する。
                            # Type が“3時間毎気温”の場合、TemperaturePart に3時間毎気温の予想を記述する。
                            elif Property_Type == "３時間毎気温":

                                # └ TemperaturePart 気温を記述する。※5-1-1参照。
                                TemperaturePart = Property.find("TemperaturePart")

                                data_dict = self.parse_xml(
                                    TemperaturePart,
                                    "TemperaturePart",
                                    DateTime_dict,
                                    Name_dict,
                                )

                                # └ Station 発表予想地点を記述する。※
                                # └ Name 発表予想地点の名称を、“東京”“大阪”などと記述する。
                                # └ Code 発表予想地点のコード番号を、“44132”“62078”などと記述する。
                                # ※対象地点は府県天気予報・府県週間天気予報_解説資料付録を参照のこと

                            else:
                                print(
                                    f"Unexpected property type encountered: {Property_Type}"
                                )

                                raise

                            for row in data_dict.values():
                                df.loc[len(df)] = [
                                    Title,  # 情報名称
                                    ReportDateTime,  # 発表時刻
                                    TargetDateTime,  # 基点時刻
                                    MeteorologicalInfos_type,  # 予報の項目
                                    prefecture,  # 都道府県
                                    Area_Name,  # 対象地域
                                    Property_Type,  # 気象要素名
                                    row["DateTime"],  # 予報期間の始めの時刻
                                    row["Name"],  # 予報の対象日
                                    row["type"],  # type
                                    row["value"],  # value
                                ]

        # print(df)

        # df = df.sort_values(by='対象地域', ascending=True)
        for idx, row in df.iterrows():
            if row["気象要素名"] in ["天気", "風", "波"]:
                df.loc[idx, "気象要素名"] = row["予報の対象日"] + row["type"]

            elif row["気象要素名"] == "降水確率":
                day = row["予報期間の始めの時刻"].split(" ")[0].split("-")[-1]
                day_target = row["基点時刻"].split(" ")[0].split("-")[-1]

                if day == day_target:
                    day = "今日"

                elif day == str(int(day_target) + 1):
                    day = "明日"

                else:
                    print(f"Unexpected day: {day}, {day_target}")
                    raise

                df.loc[idx, "気象要素名"] = day + row["予報の対象日"] + "の降水確率"

            elif row["気象要素名"] in ["日中の最高気温", "最高気温", "朝の最低気温"]:
                df.loc[idx, "気象要素名"] = (
                    row["予報の対象日"] + "の" + row["気象要素名"][-4:]
                )

            elif "時間" in row["気象要素名"]:
                day = row["予報期間の始めの時刻"].split(" ")[0].split("-")[-1]
                day_target = row["基点時刻"].split(" ")[0].split("-")[-1]

                if day == day_target:
                    day = "今日"

                elif day == str(int(day_target) + 1):
                    day = "明日"

                elif day == str(int(day_target) + 2):
                    day = "明後日"

                else:
                    print(f"Unexpected day: {day}, {day_target}")
                    raise

                split = row["気象要素名"].split("時間")
                duration = int(split[0].replace("３", "3").replace("６", "6"))
                kind = split[-1].replace("内", "").replace("毎", "")
                start = int(row["予報期間の始めの時刻"].split(" ")[-1].split(":")[0])
                row["気象要素名"] = f"{day}{start}時から{start + duration}時まで{kind}"
                row["気象要素名"] = row["気象要素名"].translate(
                    str.maketrans("0123456789", "０１２３４５６７８９")
                )

                df.loc[idx, "気象要素名"] = row["気象要素名"]

        df = df.pivot_table(
            index=[
                "情報名称",
                "予報の項目",
                "発表時刻",
                "基点時刻",
                "都道府県",
                "対象地域",
            ],
            columns="気象要素名",
            values="value",
            aggfunc="first",
        ).reset_index()

        # 檢查有無未考慮到的情況
        for col in df.columns:
            if col not in self.columns_1 + self.columns_2:
                print(self.columns_1 + self.columns_2)
                print(f"Unexpected column: {col}")
                raise

        df1 = df[df["予報の項目"] == "区域予報"].reindex(columns=self.columns_1)
        df2 = df[df["予報の項目"] == "地点予報"].reindex(columns=self.columns_2)

        return df1, df2


if __name__ == "__main__":
    config_path = "disaster.json"
    etl_vpfd51 = ETL_VPFD51(config_path, "regular", "VPFD51")
    etl_vpfd51.xml_to_csv()
