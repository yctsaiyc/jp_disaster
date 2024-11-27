from disaster import ETL_jp_disaster
from bs4 import BeautifulSoup
import pandas as pd


# 警報級の可能性（明日まで）
# 気象庁防災情報XMLフォーマット　技術資料: https://xml.kishou.go.jp/tec_material.html
# 電文毎の解説資料: https://xml.kishou.go.jp/jmaxml_20241031_Manual(pdf).zip


class ETL_VPFD60(ETL_jp_disaster):
    def xml_to_df(self, xml_path, soup):

        tmp_columns = [
            "発表時刻",
            "基点時刻",
            "基点時刻2",
            "基点時刻3",
            "都道府県",
            "対象地域",
            "気象要素名",
            "気象要素の値",
            "unit",
            "condition",
        ]

        df = pd.DataFrame(columns=tmp_columns)

        # 2 各部の構成と内容

        # (1) 管理部

        # 1 管理部(Control)の構成と内容

        # Control
        # └Title 情報名称
        #     電文の種別を示すための情報名称を示す。“「警報級の可能性(明日まで)」”で固定。
        # └DateTime 発表時刻
        #     発表時刻。未来時刻にはならない。“2008-06-26T01:51:00Z”のように協定世界時で記述する。
        # └Status 運用種別
        #     本情報の位置づけ。“通常”“訓練”“試験”のいずれかを記載。
        #     “訓練”“試験”は正規の情報として利用してはならないことを示す。
        # └EditorialOffice 編集官署名
        #     実際に発表作業を行った「編集官署名」を示す。“気象庁本庁”“大阪管区気象台”のように記述する。
        # └PublishingOffice 発表官署名
        #     本情報を業務的に発表した「発表官署名」を示す。“気象庁”“大阪管区気象台”のように記述する。

        # (2) ヘッダ部

        # 1 ヘッダ部(Head)の構成と内容

        # Head
        # └Title 標題
        #     情報を示す標題。具体的な内容が判別できる名称であり、可視化を目的として利用する。
        #     “○○警報級の可能性(明日まで)”(○○は府県予報区名)と記述する。
        prefecture = (
            soup.find("Head")
            .find("Title")
            .text.replace("警報級の可能性（明日まで）", "")
        )

        # └ReportDateTime 発表時刻
        #     本情報の公式な発表時刻を示す。“2008-06-26T11:00:00+09:00”のように日本標準時で記述する。
        ReportDateTime = self.format_datetime(soup.find("ReportDateTime").text)

        # └TargetDateTime 基点時刻
        #     本情報の対象となる時刻・時間帯の基点時刻を示す。“2008-06-28T06:00:00+09:00”のように日本標準時で記述する。
        TargetDateTime = self.format_datetime(soup.find("TargetDateTime").text)

        # └TargetDuration 基点時刻からの取りうる時間
        #     本情報の対象が時間幅を持つ場合、TargetDateTime を基点とした時間の幅を示す。
        # └EventID 識別情報
        #     警報級の可能性(明日まで)では値は記述しない。
        # └InfoType 情報形態
        #     本情報の形態を示す。“発表”“訂正”“遅延”のいずれかを記述する。
        # └Serial 情報番号
        #     警報級の可能性(明日まで)では値は記述しない。
        # └InfoKind スキーマの運用種別情報
        #     同一スキーマ上における情報分類に応じた運用を示す種別情報である。
        #     “警報級の可能性(明日まで)”と記述する。
        # └InfoKindVersion スキーマの運用種別情報のバージョン
        #     スキーマの運用種別情報におけるバージョン番号を示す。本解説のバージョン番号は“1.2_0”。
        # └Headline 見出し要素
        #     防災気象情報事項となる見出し要素を示す。警報級の可能性(明日まで)では何も記述しない。
        #     └ Text 見出し文
        #         警報級の可能性(明日まで)では値は記述しない。

        # (3) 内容部

        # 1 内容部(Body)の構成と内容

        # Body
        # └MeteorologicalInfos 予報の項目
        MeteorologicalInfos = soup.find("MeteorologicalInfos")

        #     予報の項目を属性 type で指定する。属性 type は“区域予報”の値をとる。
        MeteorologicalInfos_type = MeteorologicalInfos.get("type")

        # └MeteorologicalInfo 予報事項
        #     MeteorologicalInfos の属性 type で指定した予報の項目を記述する。

        # └ TimeSeriesInfo 時系列情報
        #     MeteorologicalInfos の属性 type で指定した予報の項目を時系列情報として記述する。

        # 2内容部の個別要素の詳細

        # ※1 24時間最大雨量、24時間最大降雪量の詳細

        # MeteorologicalInfo 予報事項
        for MeteorologicalInfo in MeteorologicalInfos.find_all("MeteorologicalInfo"):

            # └ DateTime 基点時間
            #     予報期間の始めの時刻を示す。“2008-01-10T00:00:00+09:00”のように日本標準時で記述する。
            DateTime = self.format_datetime(MeteorologicalInfo.find("DateTime").text)

            # └ Duration 対象期間
            #     予報期間の長さを示す。"P1D"などと記述する。
            # └ Name 予報時間の内容
            #     予報の対象時間幅や対象日について、"21日"のように文字列で記述する。
            Name = MeteorologicalInfo.find("Name").text

            # └ Item 予報の内容
            #     24時間最大雨量、 24時間最大降雪量と予報区を記述する。府県予報区に含まれる発表予報区の数だけ繰り返す。
            #     ※1-1「24時間最大雨量、24時間最大降雪量」の詳細を参照

            # ※1-1「24時間最大雨量、24時間最大降雪量の予想、警報級の可能性」の詳細

            # Item 予報の内容
            for Item in MeteorologicalInfo.find_all("Item"):

                Area_Name = Item.find("Area").find("Name").text

                # └ Kind 個々の予報の内容
                #     予報を記述する
                for Kind in Item.find_all("Kind"):

                    # └ Property 予報要素
                    #     予報要素を記述する
                    Property = Kind.find("Property")

                    # └ Type 気象要素名
                    #     気象要素名を記述する。Type の値は"24時間最大雨量”。
                    Property_Type = Property.find("Type").text

                    # └ DetailForecast 詳細な予報
                    #     詳細な予報を記述する
                    # └ PrecipitationForecastPart 24時間最大雨量
                    #     24時間最大雨量を記述する。
                    #     ※1-1-1 「24時間最大雨量」の詳細を参照。
                    if Property_Type == "２４時間最大雨量":
                        jmx = Property.find("jmx_eb:Precipitation")
                        jmx_text = jmx.text
                        unit = jmx.get("unit")
                        condition = jmx.get("condition")

                    # └ Kind 個々の予報の内容
                    #     予報を記述する
                    # └ Property 予報要素
                    #     予報要素を記述する
                    # └ Type 気象要素名
                    #     気象要素名を記述する。Type の値は"24時間最大降雪量”。
                    # └ DetailForecast 詳細な予報
                    #     詳細な予報を記述する
                    # └ SnowfallDepthForecastPart24時間最大降雪量
                    #     24時間最大降雪量を記述する。
                    #     ※1-1-2 「24時間最大降雪量」の詳細を参照。
                    elif Property_Type == "２４時間最大降雪量":
                        jmx = Property.find("jmx_eb:SnowfallDepth")
                        jmx_text = jmx.text
                        unit = jmx.get("unit")
                        condition = jmx.get("condition")

                    else:
                        print(Property_Type)
                        raise

                    df.loc[len(df)] = [
                        ReportDateTime,  # 発表時刻
                        TargetDateTime,  # 基点時刻
                        DateTime,  # 基点時刻2
                        Name,  # 基点時刻3
                        prefecture,  # 都道府県
                        Area_Name,  # 対象地域
                        Property_Type,  # 気象要素名
                        jmx_text,  # 気象要素の値
                        unit,
                        condition,
                    ]

                # └ Area 対象地域
                #     発表予報区を記述する。
                # └ Name 対象地域の名称
                #     発表予報区の名称を、"東京地方""大阪府"などと記述する。
                # └ Code 対象地域のコード
                #     発表予報区のコード番号を、"130010" "270000"などと記述する。

            # ※1-1-1 「24時間最大雨量」の詳細

            # 対象地域全域の場合。
            # / Sentence には対象地域の24時間最大雨量の文字列表現を記載する。
            # type は“24時間最大雨量”の固定。
            # unit は“mm”の固定。
            # description には雨量の文字列表現を記載する。

            # 対象地域の一部を、分割して、記載する場合。
            # 範囲は、type を“24時間最大雨量(範囲の下端)”、“24時間最大雨量(範囲の上端)”として示す。分割した地域は、/
            # SubArea で記載する。

            # 対象領域を、平地・山地などで細分した場合平 地 、 山 地 な ど は 、 /Local で記載する。

            # ある値以上と表現する場合は、condition に“以上”と記載する。

            # ある値以下と表現する場合は、condition に“以下”と記載する。

            # ※1-1-2 「24時間最大降雪量」の詳細

            # 対象地域全域の場合。
            # / Sentence には対象地域の24時間最大降雪量の文字列表現を記載する。
            # type は"24時間最大降雪量"の固定。
            # unit は"cm"の固定。
            # description には降雪量の文字列表現を記載する。

            # 対象地域の一部を、分割して、記載する場合。
            # 範囲は、type を“24時間最大降雪量(範囲の下端)”、“24時間最大降雪量(範囲の上端)”として示す。
            # 分 割 し た 地 域 は 、 /SubArea で記載する。

            # 対象領域を、平野部・山岳部などで細分した場合平野部、山岳部などは、/
            # Local で記載する。

            # ある値以上と表現する場合は、condition に“以上”と記載する。

        # ※2 「1時間最大雨量、3時間最大雨量、6時間最大降雪量、最大風速、波高の最大の予想」の詳細
        # TimeSeriesInfo 時系列情報
        for TimeSeriesInfo in MeteorologicalInfos.find_all("TimeSeriesInfo"):

            # └TimeDefines 時系列の時刻定義セット
            #     予報の対象期間を示すとともに、対応する要素の timeId を記述する。
            #     └ TimeDefine 個々の時刻定義
            #         同一 TimeSeriesInfo 内にある要素の ID(refID)に対応する ID(timeId)を記述する。
            #         └ DateTime 基点時刻
            #             予報期間の始めの時刻を示す。
            #             “2008-01-10T06:00:00+09:00”のように日本標準時で記述する。
            #         └ Duration 対象期間
            #             予報期間の長さを示す。
            #             "PT6H","PT12H","PT18H","PT1D"のように記述する。
            #         └ Name 予報時間の内容
            #             予報の対象時間幅や対象日について、“21日06時から12時”、“22日00時から06時”のように記述する。
            DateTime_dict, Name_dict = self.parse_TimeDefines(TimeSeriesInfo)

            # └ Item 1時間最大雨量、3時間最大雨量、6時間最大降雪量、最大風速、波高の最大予報と、予報区を記述する。
            #     府県予報区に含まれる発表予報区の数だけ繰り返す。
            #     ※2-1「1時間最大雨量、3時間最大雨量、6時間最大降雪量、最大風速、波高の最大の予想」の詳細を参照

            # ※2-1「1時間最大雨量、3時間最大雨量、6時間最大降雪量、最大風速、波高の最大の予想」の詳細

            # Item 予報の内容
            for Item in TimeSeriesInfo.find_all("Item"):

                Area_Name = Item.find("Area").find("Name").text

                # └ Kind 個々の予報の内容
                #     予報を記述する。
                for Kind in Item.find_all("Kind"):

                    # └ Property 予報要素
                    #     予報要素を記述する。
                    Property = Kind.find("Property")

                    #     └ Type 気象要素名
                    #         気象要素名を記述する。
                    #         Type の値は"1時間最大雨量”。
                    Property_Type = Property.find("Type").text

                    #     └ DetailForecast 詳細な予報
                    #         詳細な予報を記述する。
                    DetailForecast = Property.find("DetailForecast")

                    #         └ PrecipitationForecastPart 1時間最大雨量
                    #             1時間最大雨量を記述する。
                    #             ※2-1-1 「1時間最大雨量」の詳細を参照。
                    #             refID は、TimeDefines で定義した timeId に対応する。
                    if Property_Type in ["１時間最大雨量", "３時間最大雨量"]:
                        for PrecipitationForecastPart in DetailForecast.find_all(
                            "PrecipitationForecastPart"
                        ):
                            refID = PrecipitationForecastPart.get("refID")
                            DateTime = DateTime_dict[refID]
                            Name = Name_dict[refID]
                            jmx = PrecipitationForecastPart.find("jmx_eb:Precipitation")
                            jmx_text = jmx.text
                            unit = jmx.get("unit")
                            condition = jmx.get("condition")

                            df.loc[len(df)] = [
                                ReportDateTime,  # 発表時刻
                                TargetDateTime,  # 基点時刻
                                DateTime,  # 基点時刻2
                                Name,  # 基点時刻3
                                prefecture,  # 都道府県
                                Area_Name,  # 対象地域
                                Property_Type,  # 気象要素名
                                jmx_text,  # 気象要素の値
                                unit,
                                condition,
                            ]

                    # └ Kind 個々の予報の内容
                    #     予報を記述する。
                    # └ Property 予報要素
                    #     予報要素を記述する。
                    #     └ Type 気象要素名
                    #         気象要素名を記述する。
                    #         Type の値は"3時間最大雨量”。
                    #     └ DetailForecast 詳細な予報
                    #         詳細な予報を記述する。
                    #         └ PrecipitationForecastPart 3時間最大雨量
                    #             3時間最大雨量を記述する。
                    #             ※2-1-2 「3時間最大雨量」の詳細を参照。
                    #             refID は、TimeDefines で定義した timeId に対応する。

                    # └ Kind 個々の予報の内容
                    #     予報を記述する。
                    # └ Property 予報要素
                    #     予報要素を記述する。
                    #     └ Type 気象要素名
                    #         気象要素名を記述する。
                    #         Type の値は"6時間最大降雪量”。
                    #     └ DetailForecast 詳細な予報
                    #         詳細な予報を記述する。
                    #         └ SnowfallDepthForecastPart 6時間最大降雪量
                    #             6時間最大降雪量を記述する。
                    #             ※2-1-3 「6時間最大降雪量」の詳細を参照。
                    #             refID は、TimeDefinesで定義した timeId に対応する。
                    elif Property_Type == "６時間最大降雪量":
                        for SnowfallDepthForecastPart in DetailForecast.find_all(
                            "SnowfallDepthForecastPart"
                        ):
                            refID = SnowfallDepthForecastPart.get("refID")
                            DateTime = DateTime_dict[refID]
                            Name = Name_dict[refID]
                            jmx = SnowfallDepthForecastPart.find("jmx_eb:SnowfallDepth")
                            jmx_text = jmx.text
                            unit = jmx.get("unit")
                            condition = jmx.get("condition")

                            df.loc[len(df)] = [
                                ReportDateTime,  # 発表時刻
                                TargetDateTime,  # 基点時刻
                                DateTime,  # 基点時刻2
                                Name,  # 基点時刻3
                                prefecture,  # 都道府県
                                Area_Name,  # 対象地域
                                Property_Type,  # 気象要素名
                                jmx_text,  # 気象要素の値
                                unit,
                                condition,
                            ]

                    # └ Kind 個々の予報の内容
                    #     予報を記述する。
                    # └ Property 予報要素
                    #     予報要素を記述する。
                    #     └ Type 気象要素名
                    #         気象要素名を記述する。
                    #         Type の値は"最大風速”。
                    #     └ DetailForecast 詳細な予報
                    #         詳細な予報を記述する。
                    #         └ WindForecastPart 最大風速
                    #             最大風速を記述する。
                    #             ※2-1-4 「最大風速」の詳細を参照。
                    #             refID は、TimeDefines で定義した timeId に対応する。
                    elif Property_Type == "最大風速":
                        for WindForecastPart in DetailForecast.find_all(
                            "WindForecastPart"
                        ):
                            refID = WindForecastPart.get("refID")
                            DateTime = DateTime_dict[refID]
                            Name = Name_dict[refID]
                            jmx = WindForecastPart.find("jmx_eb:WindSpeed")
                            jmx_text = jmx.text
                            unit = jmx.get("unit")
                            condition = jmx.get("condition")

                            df.loc[len(df)] = [
                                ReportDateTime,  # 発表時刻
                                TargetDateTime,  # 基点時刻
                                DateTime,  # 基点時刻2
                                Name,  # 基点時刻3
                                prefecture,  # 都道府県
                                Area_Name,  # 対象地域
                                Property_Type,  # 気象要素名
                                jmx_text,  # 気象要素の値
                                unit,
                                condition,
                            ]

                    # └ Kind 個々の予報の内容
                    #     予報を記述する。
                    # └ Property 予報要素
                    #     予報要素を記述する。
                    #     └ Type 気象要素名
                    #         気象要素名を記述する。
                    #         Type の値は"波”。
                    #     └ DetailForecast 詳細な予報
                    #         詳細な予報を記述する。
                    #         └ WaveHeightForecastPart 波高の最大
                    #             波高の最大を記述する。
                    #             発表予報区で波浪警報等の運用を行なっていない場合は、Kind 以下を省略する。
                    #             ※2-1-5 「波高の最大」の詳細を参照。
                    #             refID は、TimeDefines で定義した timeId に対応する。
                    elif Property_Type == "波":
                        for WaveHeightForecastPart in DetailForecast.find_all(
                            "WaveHeightForecastPart"
                        ):
                            refID = WaveHeightForecastPart.get("refID")
                            DateTime = DateTime_dict[refID]
                            Name = Name_dict[refID]
                            jmx = WaveHeightForecastPart.find("jmx_eb:WaveHeight")
                            jmx_text = jmx.text
                            unit = jmx.get("unit")
                            condition = jmx.get("condition")

                            df.loc[len(df)] = [
                                ReportDateTime,  # 発表時刻
                                TargetDateTime,  # 基点時刻
                                DateTime,  # 基点時刻2
                                Name,  # 基点時刻3
                                prefecture,  # 都道府県
                                Area_Name,  # 対象地域
                                Property_Type,  # 気象要素名
                                jmx_text,  # 気象要素の値
                                unit,
                                condition,
                            ]

                    # └ Area 対象地域 発表予報区を記述する。
                    # └ Name 対象地域の名称 発表予報区の名称を、"東京地方""大阪府"などと記述する。
                    # └ Code 対象地域のコード 発表予報区のコード番号を、"130010" "270000"などと記述する。

                    # ※3 「警報級の可能性の予想」の詳細
                    # TimeSeriesInfo 時系列情報
                    # └TimeDefines 時系列の時刻定義セット
                    #     予報の対象期間を示すとともに、対応する要素の timeId を記述する。
                    # └ TimeDefine 個々の時刻定義
                    #     同一 TimeSeriesInfo 内にある要素の ID(refID)に対応する ID(timeId)を記述する。
                    #     予報対象数と同数を繰り返して記述する。
                    # └ DateTime 基点時刻
                    #     予報期間の始めの時刻を示す。
                    #     “2008-01-10T00:00:00+09:00”のように日本標準時で記述する。
                    # └ Duration 対象期間
                    #     予報期間の長さを示す。
                    #     "PT6H","PT12H","PT18H","PT1D"のように記述する。
                    # └ Name 予報時間の内容
                    #     予報の対象時間幅や対象期間について、"21日昼過ぎから夕方"のように文字列で記述する。
                    # └ Item
                    #     警報級の可能性の予報と、予報区を記述する。府県予報区に含まれる発表予報区の数だけ繰り返す。
                    #     ※3-1「警報級の可能性」の詳細を参照

                    # ※3-1「警報級の可能性」の詳細

                    # Item 予報の内容

                    # └ Kind 個々の予報の内容 予報を記述する。
                    # └ Property 予報要素 予報要素を記述する。
                    # └ Type 気象要素名 気象要素名を記述する。Type の値は"雨の警報級の可能性”。
                    # └ PossibilityRankOfWarningPart 警報級の可能性
                    #     「雨の警報級の可能性」の階級値(※3-1-6)を記述する。
                    #     ※3-1-1 「雨の警報級の可能性」の詳細を参照。refID は、
                    #     TimeDefines で定義した timeId に対応する。
                    elif Property_Type in [
                        "雨の警報級の可能性",
                        "雪の警報級の可能性",
                        "風（風雪）の警報級の可能性",
                        "波の警報級の可能性",
                        "潮位の警報級の可能性",
                    ]:
                        PossibilityRankOfWarningPart = Property.find(
                            "PossibilityRankOfWarningPart"
                        )

                        for jmx in PossibilityRankOfWarningPart.find_all(
                            "jmx_eb:PossibilityRankOfWarning"
                        ):

                            refID = jmx.get("refID")
                            DateTime = DateTime_dict[refID]
                            Name = Name_dict[refID]

                            jmx_text = jmx.text
                            unit = jmx.get("unit")
                            condition = jmx.get("condition")

                            df.loc[len(df)] = [
                                ReportDateTime,  # 発表時刻
                                TargetDateTime,  # 基点時刻
                                DateTime,  # 基点時刻2
                                Name,  # 基点時刻3
                                prefecture,  # 都道府県
                                Area_Name,  # 対象地域
                                Property_Type,  # 気象要素名
                                jmx_text,  # 気象要素の値
                                unit,
                                condition,
                            ]

                    # └ Text 雨、雪、風(風雪)、波若しくは潮位の警報級の可能性が[高]、[中]のとき又は
                    #     condition が“値なし”のとき、警報級の可能性及び対象期間の概要を文字列で記述する。
                    #     記述する内容が無い場合には、タグ自体出現しない。
                    #     同一発表予報区内では、Type の値にかかわらず同じとなる。

                    # └ Kind 個々の予報の内容 予報を記述する。
                    # └ Property 予報要素 予報要素を記述する。
                    # └ Type 気象要素名 気象要素名を記述する。Type の値は"雪の警報級の可能性”。
                    # └ PossibilityRankOfWarningPart 警報級の可能性
                    #     「雪の警報級の可能性」の階級値(※3-1-6)を記述する。
                    #     ※3-1-2 「雪の警報級の可能性」の詳細を参照。refID は、
                    #     TimeDefines で定義した timeId に対応する。
                    # └ Text 雨、雪、風(風雪)、波若しくは潮位の警報級の可能性が[高]、[中]のとき又は
                    #     condition が“値なし”のとき、警報級の可能性及び対象期間の概要を文字列で記述する。
                    #     記述する内容が無い場合には、タグ自体出現しない。
                    #     同一発表予報区内では、Type の値にかかわらず同じとなる。
                    #
                    # └ Kind 個々の予報の内容 予報を記述する。
                    # └ Property 予報要素 予報要素を記述する。
                    # └ Type 気象要素名 気象要素名を記述する。Type の値は"風(風雪)の警報級の可能性”。
                    # └ PossibilityRankOfWarningPart 警報級の可能性
                    #     「風(風雪)の警報級の可能性」の階級値(※3-1-6)を記述する。
                    #     ※3-1-3 「風(風雪)の警報級の可能性」の詳細を参照。refID は、
                    #     TimeDefines で定義した timeId に対応する。
                    # └ Text 雨、雪、風(風雪)、波若しくは潮位の警報級の可能性が[高]、[中]のとき又は
                    #     condition が“値なし”のとき、警報級の可能性及び対象期間の概要を文字列で記述する。
                    #     記述する内容が無い場合には、タグ自体出現しない。
                    #     同一発表予報区内では、Type の値にかかわらず同じとなる。

                    # └ Kind 個々の予報の内容 予報を記述する。
                    # └ Property 予報要素 予報要素を記述する。
                    # └ Type 気象要素名 気象要素名を記述する。Type の値は"波の警報級の可能性”。
                    # └ PossibilityRankOfWarningPart 警報級の可能性
                    #     「波の警報級の可能性」の階級値(※3-1-6)を記述する。
                    #     発表予報区で波浪警報等の運用を行なっていない場合は、Kind 以下を省略する。
                    #     ※3-1-4 「波の警報級の可能性」の詳細を参照。
                    #     refID は、TimeDefines で定義した timeId に対応する。
                    # └ Text 雨、雪、風(風雪)、波若しくは潮位の警報級の可能性が[高]、[中]のとき又は
                    #     condition が“値なし”のとき、警報級の可能性及び対象期間の概要を文字列で記述する。
                    #     記述する内容が無い場合には、タグ自体出現しない。
                    #     同一発表予報区内では、Type の値にかかわらず同じとなる。

                    # └ Kind 個々の予報の内容 予報を記述する。
                    # └ Property 予報要素 予報要素を記述する。
                    # └ Type 気象要素名 気象要素名を記述する。Type の値は"潮位の警報級の可能性”。
                    # └ PossibilityRankOfWarningPart 警報級の可能性
                    #     「潮位の警報級の可能性」の階級値(※3-1-6)を記述する。
                    #     発表予報区で高潮警報等の運用を行なっていない場合は、Kind 以下を省略する。
                    #     ※3-1-5 「潮位の警報級の可能性」の詳細を参照。
                    #     refID は、TimeDefines で定義した timeId に対応する。
                    # └ Text 雨、雪、風(風雪)、波若しくは潮位の警報級の可能性が[高]、[中]のとき又は
                    #     condition が“値なし”のとき、警報級の可能性及び対象期間の概要を文字列で記述する。
                    #     記述する内容が無い場合には、タグ自体出現しない。
                    #     同一発表予報区内では、Type の値にかかわらず同じとなる。

                    # └ Area 対象地域 発表予報区を記述する。
                    # └ Name 対象地域の名称 発表予報区の名称を、"東京地方""大阪府"などと記述する。
                    # └ Code 対象地域のコード 発表予報区のコード番号を、"130010" "270000"などと記述する。

                    else:
                        print(Property_Type)
                        raise

        df.to_csv("kind.csv", index=False, encoding="utf-8")

        df = df.pivot_table(
            index=["発表時刻", "基点時刻", "基点時刻2", "都道府県", "対象地域"],
            columns="気象要素名",
            values="気象要素の値",
            aggfunc="first",
        ).reset_index()

        df = df.reindex(columns=self.columns)

        return df


if __name__ == "__main__":
    config_path = "disaster.json"
    etl_vpfd60 = ETL_VPFD60(config_path, "regular", "VPFD60")
    etl_vpfd60.xml_to_csv()
