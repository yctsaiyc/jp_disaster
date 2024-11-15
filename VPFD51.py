from disaster import ETL_jp_disaster
import pandas as pd


# 府県天気予報（Ｒ１）
# 気象庁防災情報XMLフォーマット　技術資料: https://xml.kishou.go.jp/tec_material.html
# 電文毎の解説資料: https://xml.kishou.go.jp/jmaxml_20241031_Manual(pdf).zip


class ETL_VPFD51(ETL_jp_disaster):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.soup = None

        self.title = ""
        self.report_date_time = ""
        self.target_date_time = ""
        self.prefecture = ""

        self.df = None
        self.df_weather = None
        self.df_wind = None
        self.df_wave = None
        self.df_6hr_precipitation = None

    def int_df_weather(self):
        columns = [
            "今日予報の始めの時刻",
            "今日天気",
            "今日風向",
            "今日波高",
            "明日予報の始めの時刻",
            "明日天気",
            "明日風向",
            "明日波高",
            "明後日予報の始めの時刻",
            "明後日天気",
            "明後日風向",
            "明後日波高",
        ]

        self.df_weather = pd.DataFrame(columns=columns)

    def parse_xml(self, xml, tag_name, DateTime_dict, Name_dict):
        data_dict = {}

        # ※1(1)-1-1(1) 「天気予報文」の詳細(A) (/Kind/Property/Type = 天気) /DetailForecast/WeatherForecastPart
        # <DetailForecast>
        #   <WeatherForecastPart refID="1">  # @refID は今日または今夜=1、明日=2、明後日=3 で、明後日はないことがある。
        #     <Sentence>雨</Sentence>  # /Sentence はかな漢字電文と同じ天気予報文が入る。
        #     <Base>  # 卓越天気 /Base のみの例。
        #       <jmx_eb:Weather type="天気">雨</jmx_eb:Weather>
        #     </Base>
        #   </WeatherForecastPart>
        #   <WeatherForecastPart refID="2">
        #     <Sentence>雨 所により 夜のはじめ頃 雷を伴い 激しく 降る</Sentence>  # 地域天気が付加される例
        #     <Base>
        #       <jmx_eb:Weather type="天気">雨</jmx_eb:Weather>
        #     </Base>
        #     <SubArea>  # 地域天気は、かな漢字電文の文章がそのまま入る。
        #       <Sentence type="地域天気">所により 夜のはじめ頃 雷を伴い 激しく 降る</Sentence>
        #     </SubArea>
        #   </WeatherForecastPart>
        #   <WeatherForecastPart refID="3">
        #     <Sentence>雪 で ふぶく 後 くもり</Sentence>  # 「ふぶく」の例
        #     <Base>
        #       <jmx_eb:Weather type="天気" condition="ふぶく">雪</jmx_eb:Weather>  # 「 雪 」の天気の場合、@condition に「ふぶく」が付加されることがある。
        #     </Base>
        #     <Becoming>
        #       <TimeModifier>後</TimeModifier>
        #       <jmx_eb:Weather type="天気">くもり</jmx_eb:Weather>
        #     </Becoming>
        #   </WeatherForecastPart>
        # </DetailForecast>

        # ※1(1)-1-1(1) 「天気予報文」の詳細(B) (/Kind/Property/Type = 天気) /DetailForecast/WeatherForecastPart
        # <DetailForecast>
        #   <WeatherForecastPart refID="1">
        #     <Sentence>くもり 昼前 から 晴れ 所により 未明 雪</Sentence>
        #     <Base>  # 卓越天気/Base → /Becoming の例。
        #       <jmx_eb:Weather type="天気">くもり</jmx_eb:Weather>
        #     </Base>
        #     <Becoming>
        #       <TimeModifier>昼前 から</TimeModifier>
        #       <jmx_eb:Weather type="天気">晴れ</jmx_eb:Weather>
        #     </Becoming>
        #     <SubArea>
        #       <Sentence type="地域天気">所により 未明 雪</Sentence>
        #     </SubArea>
        #   </WeatherForecastPart>
        #   <WeatherForecastPart refID="2">
        #     <Sentence>くもり 昼過ぎ から 時々 晴れ</Sentence>
        #     <Base>  # 卓越天気/Base → /Becoming の例
        #       <jmx_eb:Weather type="天気">くもり</jmx_eb:Weather>
        #     </Base>
        #     <Becoming>
        #       <TimeModifier>昼過ぎ から 時々</TimeModifier>  # /TimeModifier に時間の表現が2種類入る例。
        #       <jmx_eb:Weather type="天気">晴れ</jmx_eb:Weather>
        #     </Becoming>
        #   </WeatherForecastPart>
        #   <WeatherForecastPart refID="3">
        #     <Sentence>くもり 時々 雪</Sentence>
        #     <Base>  # 卓越天気/Base → /Temporary の例。
        #       <jmx_eb:Weather type="天気">くもり</jmx_eb:Weather>
        #     </Base>
        #     <Temporary>
        #       <TimeModifier>時々</TimeModifier>
        #       <jmx_eb:Weather type="天気">雪</jmx_eb:Weather>
        #     </Temporary>
        #   </WeatherForecastPart>
        # </DetailForecast>

        # ※1(1)-1-1(1) 「天気予報文」の詳細(C) (/Kind/Property/Type = 天気) /DetailForecast/WeatherForecastPart
        # ...

        # ※1(1)-1-1(1) 「天気予報文」の詳細(D) (/Kind/Property/Type = 天気) /DetailForecast/WeatherForecastPart
        # ...

        # ※1(1)-1-1(1) 「天気予報文」の詳細(E) (/Kind/Property/Type = 天気) /DetailForecast/WeatherForecastPart
        # ...

        # ※1(1)-1-1(2) 「テロップ用天気予報用語の天気」の詳細 (/Kind/Property/Type = 天気) /WeatherPart
        # <WeatherPart>
        #   <jmx_eb:Weather refID="1" type="天気">雨後雪</jmx_eb:Weather>
        #   <jmx_eb:Weather refID="2" type="天気">雪</jmx_eb:Weather>
        # </WeatherPart>

        # 値は「天気予報用テロップ番号」に対応した天気(テロップ用天気予報用語の天気)が入る。
        # 対応は府県天気予報・府県週間天気予報_解説資料付録を参照のこと。

        # ※4-1-1(1) 「3時間内卓越天気」の詳細 (/Kind/Property/Type = 3時間内卓越天気) /WeatherPart
        # <WeatherPart>
        #   <jmx_eb:Weather refID="1" type="天気">雨</jmx_eb:Weather>
        #   <jmx_eb:Weather refID="2" type="天気">雨</jmx_eb:Weather>
        # </WeatherPart>

        if tag_name == "WeatherPart":
            for jmx in xml.find_all("jmx_eb:Weather"):
                refID = jmx.get("refID")

                data_dict[refID] = {
                    "DateTime": DateTime_dict.get(refID),
                    "Name": Name_dict.get(refID),
                    "type": jmx.get("type"),
                    "weather": jmx.text,
                }

        # ※1(1)-1-1(3) 「天気予報用テロップ番号」の詳細 (/Kind/Property/Type = 天気) /WeatherCodePart
        # ...

        # ※1(1)-1-2 「風の予報文」の詳細(A) (/Kind/Property/Type = 風) /DetailForecast/WindForecastPart
        # 代表風予報/Base のみの例。
        # /condition を利用して風の強さの階級を述べる例。
        # 階級は「やや強く」「強く」「非常に強く」。
        # 代表風予報/Base → /Becoming の例。
        # ...

        # ※1(1)-1-2 「風の予報文」の詳細(B) (/Kind/Property/Type = 風) /DetailForecast/WindForecastPart
        # 地域風予報を述べる例
        # /SubArea を使用。
        # ※対象地域は府県天気予報・府県週間天気予報_解説資料付録を参照のこと
        # 「おさまり」の例。
        # /Base と/Becoming の風向が同じで、
        # /Base の風の強さの階級が指定されていて、
        # /Becoming の風の強さの階級が指定されない場合に、「おさまり」を用いる。
        # ...

        # ※1(1)-1-2 「風の予報文」の詳細(C) (/Kind/Property/Type = 風) /DetailForecast/WindForecastPart
        # 「風弱く」の例。
        # 1日を通して弱い風が予想されるとき、「風弱く」を用いる
        # 「風弱く」では、風向は特定されない。
        # 地域風予報で「海上」を使用する例。
        # また、地域風予報で「後」表現となる例。
        # /SubArea を使用。
        # ※対象地域は府県天気予報・府県週間天気予報_解説資料付録を参照のこと。
        # /Base を省略。
        # ...

        elif tag_name == "WindForecastPart":
            for WindForecast in xml.find_all("WindForecastPart"):
                refID = WindForecast.get("refID")
                jmx = WindForecast.find("jmx_eb:WindDirection")

                data_dict[refID] = {
                    "DateTime": DateTime_dict.get(refID),
                    "Name": Name_dict.get(refID),
                    "type": jmx.get("type"),
                    "wind_direction": jmx.text,
                }

        # ※1(1)-1-3 「波の予報文」の詳細(A) (/Kind/Property/Type = 波) /DetailForecast/WaveHeightForecastPart
        # ...

        # ※1(1)-1-3 「波の予報文」の詳細(B) (/Kind/Property/Type = 波) /DetailForecast/WaveHeightForecastPart
        # ...

        # ※1(1)-1-3 「波の予報文」の詳細(C) (/Kind/Property/Type = 波) /DetailForecast/WaveHeightForecastPart
        # ...

        elif tag_name == "WaveHeightForecastPart":
            for WaveHeightForecast in xml.find_all("WaveHeightForecastPart"):
                refID = WaveHeightForecast.get("refID")
                jmx = WaveHeightForecast.find("jmx_eb:WaveHeight")

                data_dict[refID] = {
                    "DateTime": DateTime_dict.get(refID),
                    "Name": Name_dict.get(refID),
                    "type": jmx.get("type"),
                    "wave_height": jmx.text,
                }

        # ※1(2)-1-1 「降水確率」の予報文の詳細 (/Kind/Property/Type = 降水確率) /ProbabilityOfPrecipitationPart
        # /condition は、降水種別(雨雪判別)が入る。
        # 「雨」「雨か雪」「雪か雨」
        # 「雪」の4種類。
        # 値は、0 から 100 の11個の整数の1つが入る。
        # <ProbabilityOfPrecipitationPart>
        #   <jmx_eb:ProbabilityOfPrecipitation condition="雨" description="0パーセント" refID="1" type="6時間降水確率" unit="%">
        #     0
        #   </jmx_eb:ProbabilityOfPrecipitation>
        #   <jmx_eb:ProbabilityOfPrecipitation condition="雨か雪" description="30パーセント" refID="2" type="6時間降水確率" unit="%">
        #     30
        #   </jmx_eb:ProbabilityOfPrecipitation>
        #   <jmx_eb:ProbabilityOfPrecipitation condition="雪か雨" description="60パーセント" refID="3" type="6時間降水確率" unit="%">
        #     60
        #   </jmx_eb:ProbabilityOfPrecipitation>
        # </ProbabilityOfPrecipitationPart>
        elif tag_name == "ProbabilityOfPrecipitationPart":
            for jmx in xml.find_all("jmx_eb:ProbabilityOfPrecipitation"):
                refID = jmx.get("refID")

                data_dict[refID] = {
                    "DateTime": DateTime_dict.get(refID),
                    "Name": Name_dict.get(refID),
                    "type": jmx.get("type"),
                    "precipitation": jmx.text,
                }

        # ※2-1-1 「予想気温」の予報文の詳細 (/Kind/Property/Type = 日中の最高気温/最高気温/朝の最低気温) /TemperaturePart
        # 予想気温は、整数。マイナスの値も取りうる。
        # <TemperaturePart>
        #   <jmx_eb:Temperature description="7度" refID="1" type="日中の最高気温" unit="度">7</jmx_eb:Temperature>
        # </TemperaturePart>
        # <TemperaturePart>
        #   <jmx_eb:Temperature description="7度" refID="2" type="最高気温" unit="度">7</jmx_eb:Temperature>
        # </TemperaturePart>
        # <TemperaturePart>
        #   <jmx_eb:Temperature description=" マ イ ナ ス 3 度 " refID="3" type=" 朝 の 最 低 気 温 " unit=" 度 ">-3</jmx_eb:Temperature>
        # </TemperaturePart>

        # ※5-1-1 「3時間毎気温」の詳細 (/Kind/Property/Type = 3時間毎気温) /TemperaturePart
        # 地域時系列予報の時別気温が入る。
        # マイナスの値も取りうる。
        # <TemperaturePart>
        #   <jmx_eb:Temperature description="2度" refID="1" type="気温" unit="度">2</jmx_eb:Temperature>
        #   <jmx_eb:Temperature description="3度" refID="2" type="気温" unit="度">3</jmx_eb:Temperature>
        # </TemperaturePart>

        elif tag_name == "TemperaturePart":
            for jmx in xml.find_all("jmx_eb:Temperature"):
                refID = jmx.get("refID")

                data_dict[refID] = {
                    "DateTime": DateTime_dict.get(refID),
                    "Name": Name_dict.get(refID),
                    "type": jmx.get("type"),
                    "temperature": jmx.text,
                }

        # ※4-1-1(2) 「3時間内代表風の風向」の詳細 (/Kind/Property/Type = 3時間内代表風) /WindDirectionPart
        # 地域時系列予報の時別風(風向)が入る。
        # <WindDirectionPart>
        #   <jmx_eb:WindDirection refID="1" type="風向" unit="8方位漢字">東</jmx_eb:WindDirection>
        #   <jmx_eb:WindDirection refID="2" type="風向" unit="8方位漢字">東</jmx_eb:WindDirection>
        # </WindDirectionPart>
        elif tag_name == "WindDirectionPart":
            for jmx in xml.find_all("jmx_eb:WindDirection"):
                refID = jmx.get("refID")

                data_dict[refID] = {
                    "DateTime": DateTime_dict.get(refID),
                    "Name": Name_dict.get(refID),
                    "type": jmx.get("type"),
                    "wind_direction": jmx.text,
                }

        # ※4-1-1(3) 「3時間内代表風の風速階級」の詳細 (/Kind/Property/Type = 3時間内代表風) /WindSpeedPart
        # 地域時系列予報の時別風(風のレベル値)が入る。
        # 風のレベル値は1~6。
        # <WindSpeedPart>
        #   <WindSpeedLevel description="毎秒0から2メートル" range="0 2" refID="1" type="風速階級">1</WindSpeedLevel>
        #   <WindSpeedLevel description="毎秒3から5メートル" range="3 5" refID="2" type="風速階級">2</WindSpeedLevel>
        # </WindSpeedPart>

        elif tag_name == "WindSpeedPart":
            for jmx in xml.find_all("WindSpeedLevel"):
                refID = jmx.get("refID")

                data_dict[refID] = {
                    "DateTime": DateTime_dict.get(refID),
                    "Name": Name_dict.get(refID),
                    "type": jmx.get("type"),
                    "wind_speed": jmx.text,
                }

        return data_dict

    def xml_to_df(self, xml_path, soup):
        df = pd.DataFrame(columns=self.columns)

        # 2 各部の構成と内容
        # (1)管理部
        # 1管理部の構成
        # Control
        # └ Title 情報名称
        self.title = soup.find("Title").text

        # └ DateTime 発表時刻
        # └ Status 運用種別
        # └ EditorialOffice 編集官署名
        # └ PublishingOffice 発表官署名

        # (2) ヘッダ部
        # 1 ヘッダ部の構成
        # Head
        # └ Title 標題
        self.prefecture = (
            soup.find("Head").find("Title").text.replace("府県天気予報", "")
        )

        # └ ReportDateTime 発表時刻
        self.report_date_time = self.format_datetime(soup.find("ReportDateTime").text)

        # └ TargetDateTime 基点時刻
        self.target_date_time = self.format_datetime(soup.find("TargetDateTime").text)

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
                TimeDefines = TimeSeriesInfo.find("TimeDefines")

                #     └ TimeDefine
                #     同一 TimeSeriesInfo 内にある要素の ID(refID)に対応する ID(timeId)を記述する。
                #     ID は 1、2 または 1~3。ID で示す、予報対象数と同数を繰り返して記述する。
                TimeDefine_all = TimeDefines.find_all("TimeDefine")
                DateTime_dict = {}
                Name_dict = {}

                for TimeDefine in TimeDefine_all:
                    timeId = TimeDefine.get("timeId")

                    #     └ DateTime
                    #     予報期間の始めの時刻を示す。“2008-01-10T05:00:00+09:00”のように日本標準時で記述する。
                    DateTime_dict[timeId] = self.format_datetime(
                        TimeDefine.find("DateTime").text
                    )

                    #     └ Duration
                    #     予報期間の長さを示す。“PT19H”“PT1D”など今日予報は24時までの長さ(時間)、明日予報と明後日予報は1日で記述する。

                    #     └ Name
                    #     予報の対象日を“今日”、“明日”、“明後日”のいずれかで記述する。
                    #     発表する時刻によって“今日”は “今夜”とする場合がある。また、“明後日”がないことがある。
                    name = TimeDefine.find("Name")

                    if name:
                        Name_dict[timeId] = name.text

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
                                        self.title,  # 情報名称
                                        self.report_date_time,  # 発表時刻
                                        self.target_date_time,  # 基点時刻
                                        MeteorologicalInfos_type,  # 予報の項目
                                        self.prefecture,  # 都道府県
                                        Area_Name,  # 対象地域
                                        Property_Type,  # 気象要素名
                                        row.get("DateTime"),  # 予報期間の始めの時刻
                                        row.get("Name"),  # 予報の対象日
                                        row.get("type"),  # 気象要素名2
                                        row.get("weather"),  # 天気
                                        row.get("wind_direction"),  # 風向
                                        row.get("wind_speed"),  # 風速階級
                                        row.get("precipitation"),  # 降水確率
                                        row.get("temperature"),  # 気温
                                        row.get("wave_height"),  # 波高
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
                                    self.title,  # 情報名称
                                    self.report_date_time,  # 発表時刻
                                    self.target_date_time,  # 基点時刻
                                    MeteorologicalInfos_type,  # 予報の項目
                                    self.prefecture,  # 都道府県
                                    Area_Name,  # 対象地域
                                    Property_Type,  # 気象要素名
                                    row.get("DateTime"),  # 予報期間の始めの時刻
                                    row.get("Name"),  # 予報の対象日
                                    row.get("type"),  # 気象要素名2
                                    row.get("weather"),  # 天気
                                    row.get("wind_direction"),  # 風向
                                    row.get("wind_speed"),  # 風速階級
                                    row.get("precipitation"),  # 降水確率
                                    row.get("temperature"),  # 気温
                                    row.get("wave_height"),  # 波高
                                ]

        return df


if __name__ == "__main__":
    config_path = "disaster.json"
    etl_vpfd51 = ETL_VPFD51(config_path, "regular", "VPFD51")
    etl_vpfd51.xml_to_csv()
