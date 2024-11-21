from disaster import ETL_jp_disaster
from bs4 import BeautifulSoup
import pandas as pd


# 府県週間天気予報
# 気象庁防災情報XMLフォーマット　技術資料: https://xml.kishou.go.jp/tec_material.html
# 電文毎の解説資料: https://xml.kishou.go.jp/jmaxml_20241031_Manual(pdf).zip


class ETL_VPFW50(ETL_jp_disaster):
    def xml_to_df(self, xml_path, soup):
        df = pd.DataFrame(columns=self.columns)

        # 2 各部の構成と内容

        # (1) 管理部

        # 1 管理部の構成
        # Control
        # └ Title 情報名称
        # └ DateTime 発表時刻
        # └ Status 運用種別
        # └ EditorialOffice 編集官署名
        # └ PublishingOffice 発表官署名

        # 2 Control 部の詳細

        # Title 「情報名称」
        # 電文の種別を示すための情報名称を示す。“府県週間天気予報”で固定。

        # DateTime 「発表時刻」
        # 発表時刻。未来時刻にはならない。
        # “2008-06-26T01:51:00Z”のように協定世界時で記述する。

        # Status 「運用種別」
        # 本情報の位置づけ。“通常”“訓練”“試験”のいずれかを記載。
        # “訓練”“試験”は正規の情報として利用してはならないことを示す。

        # EditorialOffice 「編集官署名」
        # 実際に発表作業を行った「編集官署名」を示す。“気象庁本庁”“大阪管区気象台”のように記述する。

        # PublishingOffice 「発表官署名」
        # 本情報を業務的に発表した「発表官署名」を示す。“気象庁”“大阪管区気象台”のように記述する。

        # (2) ヘッダ部
        # 1 ヘッダ部の構成
        # Head
        # └ Title 標題
        # └ ReportDateTime 発表時刻
        # └ TargetDateTime 基点時刻
        # └ TargetDuration 基点時刻からの取りうる時間(日数)
        # └ EventID 識別情報
        # └ InfoType 情報形態
        # └ Serial 情報番号
        # └ InfoKind スキーマの運用種別情報
        # └ InfoKindVersion スキーマの運用種別情報のバージョン
        # └ Headline 見出し要素

        # 2ヘッダ部の詳細

        # Title 「標題」
        # 情報を示す標題。具体的な内容が判別できる名称であり、可視化を目的として利用する。

        # ReportDateTime 「発表時刻」
        # 本情報の公式な発表時刻を示す。“2008-06-26T11:00:00+09:00”のように日本標準時で記述する。
        # なお、府県週間天気予報の「発表時刻」は時まで変化し、分と秒は「00」と記述する。
        ReportDateTime = self.format_datetime(soup.find("ReportDateTime").text)

        # TargetDateTime 「基点時刻」
        # 本情報の対象となる時刻・時間帯の基点時刻を示す。“2008-06-27T00:00:00+09:00”のように日本標準時で記述する。
        # 府県週間天気予報は発表日翌日からの予報で、「基点時刻」は発表日翌日となる。
        TargetDateTime = self.format_datetime(soup.find("TargetDateTime").text)

        # TargetDuration 「基点時刻からの取りうる時間」
        # 情報の対象が時間幅を持つ場合、targetDateTime を基点とした時間の幅を示す。
        # “P7D”で、「基点時刻」(発表日翌日)から7日先までの予報であることを示す。

        # EventID 「識別情報」
        # 府県週間天気予報では要素内に何も記述しない。

        # InfoType 「情報形態」
        # 本情報の形態を示す。“発表”“訂正”“遅延”のいずれかを記述する。

        # Serial 「情報番号」
        # 府県週間天気予報では要素内に何も記述しない。

        # InfoKind 「スキーマの運用種別情報」
        # 同一スキーマ上における情報分類に応じた運用を示す種別情報である。“府県週間天気予報”と記述する。

        # InfoKindVersion 「スキーマの運用種別情報のバージョン」スキーマの運用種別情報におけるバージョン番号を示す。
        # 本解説のバージョン番号は“1.0_0”。

        # Headline 「見出し要素」
        # 防災気象情報事項となる見出し要素を示す。府県週間天気予報では何も記述しない。
        # └ Text 「見出し文」
        #     府県週間天気予報では要素内に何も記述しない。

        # (3) 内容部
        # 1 内容部の構成
        # Body
        # └ MeteorologicalInfos 予報の項目
        #     └ TimeSeriesInfo 時系列情報
        #     └ MeteorologicalInfo 予報の内容

        # MeteorologicalInfos 予報や平年値などの項目を属性 type で指定する。
        # 属性 type は“区域予報”、“地点予報”、“日別平年値”、“7日間平年値”の値をとる。
        # “区域予報”の場合は、天気予報文や降水確率予報(2内容部の個別要素の詳細※1参照)、“地点予報”の場合は、
        # 予想気温(2内容部の個別要素の詳細※2参照)、“日別平年値”の場合は、気温の平年値(2内容部の個別要素の詳細※3参照)、
        # “7日間平年値”の場合は、降水量の平年値階級(2内容部の個別要素の詳細※4参照)を記述する。
        MeteorologicalInfos_all = soup.find_all("MeteorologicalInfos")

        for MeteorologicalInfos in MeteorologicalInfos_all:

            # └ TimeSeriesInfo
            #     MeteorologicalInfos の属性 type で指定した予報や平年値を時系列情報として記述する。
            TimeSeriesInfo = MeteorologicalInfos.find("TimeSeriesInfo")

            # └MeteorologicalInfo
            #     MeteorologicalInfos の属性 type で指定した平年値を記述する。
            MeteorologicalInfo = TimeSeriesInfo.find("MeteorologicalInfo")

            # 2内容部の個別要素の詳細

            # ※1 予報に関する事項(天気などの区域予報)の詳細
            # TimeSeriesInfo 時系列情報
            # └ TimeDefines 時系列の時刻定義セット
            #     └ TimeDefine 個々の時刻定義
            #         └ DateTime 基点時刻
            #         └ Duration 対象期間
            # └ Item ※1-1 “区域予報”参照

            # TimeSeriesInfo
            TimeSeriesInfo = MeteorologicalInfos.find("TimeSeriesInfo")

            if TimeSeriesInfo:

                # └ TimeDefines 予報の対象期間を示すとともに、対応する要素の timeId を記述する。
                #     └ TimeDefine 同一 TimeSeriesInfo 内にある要素の ID(refID)に対応する ID(timeId)を記述する。
                #         └ DateTime 予報対象日について記述する。予報対象日の開始時刻を示す。
                #             “2008-06-27T00:00:00+09:00”のように日本標準時で記述する。
                #         └ Duration 予報の対象期間を示す。値「P1D」で、1日を対象とした予報であることを示す。
                #             DateTime と Duration の組み合わせにより TimeDefine の示す期間は、文章形式では DateTime 値の日単位の期間を示す。
                DateTime_dict = self.parse_TimeDefines(
                    TimeSeriesInfo.find("TimeDefines")
                )

                # └ Item 予報の内容と予報区を記述する。府県予報区に含まれる発表予報区の数だけ繰り返す。※1-1参照。
                Item = TimeSeriesInfo.find("Item")

                # ※1-1 区域予報(天気、降水確率の予報、信頼度)の内容
                # Item 予報の内容

                # └ Kind 個々の予報の内容
                # └ Property 予報要素
                # └ Type 気象要素名
                # └ WeatherPart 天気予報文
                # └ WeatherCodePart 天気テロップ番号
                # └ Kind 個々の予報の内容
                # └ Property 予報要素
                # └ Type 気象要素名
                # └ ProbabilityOfPrecipitationPart 降水確率予報
                # └ Kind 個々の予報の内容
                # └ Property 予報要素
                # └ Type 気象要素名
                # └ ReliabilityClassPart 信頼度
                # └ Area 対象地域
                # └ Name 対象地域の名称
                try:
                    Area_Name = Item.find("Area").find("Name").text

                except AttributeError:
                    Area_Name = Item.find("Station").find("Name").text

                # └ Code 対象地域のコード

                # Item
                # └ Kind 予報を記述する。
                Kind_all = Item.find_all("Kind")

                for Kind in Kind_all:

                    # └ Property 予報要素を記述する。
                    Property = Kind.find("Property")

                    #     └ Type 気象要素名を記述する。Type の値が“天気”の場合は天気予報文を記述する。
                    Property_Type = Property.find("Type").text

                    if Property_Type == "天気":

                        # └ WeatherPart 天気予報文を記述する。
                        WeatherPart = Property.find("WeatherPart")

                        # └ jmx_eb:Weather 天気予報文を記述する。
                        jmx_all = WeatherPart.find_all("jmx_eb:Weather")

                        for jmx in jmx_all:

                            # 属性 type は“基本天気”の値をとる。
                            jmx_type = jmx.get("type")

                            # 属性 refID は、予報対象日の参照番号を記述する。
                            refID = jmx.get("refID")

                            # TimeDefines で定義した timeId に対応する。
                            DateTime = DateTime_dict[refID]

                        # └ WeatherCodePart 天気予報文に対応した天気テロップ番号を記述する。※
                        # └ jmx_eb:WeatherCode テロップ番号を記述する。
                        #     属性 type は“天気予報用テロップ番号”の値をとる。
                        #     属性refID は、予報対象日の参照番号を記述する。
                        #     TimeDefines で定義した timeId に対応する。

                    # └ Kind 予報を記述する。
                    # └ Property 予報要素を記述する。
                    #     └ Type 気象要素名を記述する。Type の値が“降水確率”の場合は、降水確率について記述する。
                    elif Property_Type == "降水確率":

                        # └ ProbabilityOfPrecipitationPart 降水確率予報を記述する。
                        ProbabilityOfPrecipitationPart = Property.find(
                            "ProbabilityOfPrecipitationPart"
                        )

                        # └jmx_eb:ProbabilityOfPrecipitation 降水確率予報を記述する。
                        jmx_all = ProbabilityOfPrecipitationPart.find_all(
                            "jmx_eb:ProbabilityOfPrecipitation"
                        )

                        for jmx in jmx_all:

                            # 属性 type は“日降水確率”の値をとり、日単位(24 時間)の降水確率であることを示す。
                            jmx_type = jmx.get("type")

                            # 属性 unit は降水確率の単位を示す。

                            # 属性 refID は、予報対象日の参照番号を記述する。
                            refID = jmx.get("refID")

                            # TimeDefines で定義した timeId に対応する。
                            DateTime = DateTime_dict[refID]

                            # 属性condition は予報値の状態を示し、予報対象でない場合等で予報値が存在しない場合に“値なし”と記述する。

                            # 属性 description には予報値の文字列表現が入る。

                    # └ Kind 予報を記述する。
                    # └ Property 予報要素を記述する。
                    #     └ Type 気象要素名を記述する。Type の値が“信頼度”の場合は、予報の信頼度について記述する。
                    elif Property_Type == "信頼度":

                        # └ ReliabilityClassPart 予報の信頼度を記述する。
                        ReliabilityClassPart = Property.find("ReliabilityClassPart")

                        # └ jmx_eb:ReliabilityClass 予報の信頼度を記述する。
                        jmx_all = ReliabilityClassPart.find_all(
                            "jmx_eb:ReliabilityClass"
                        )

                        for jmx in jmx_all:

                            # 属性 type は“信頼度階級”の値をとり、信頼度を階級値で記述することを示す。
                            jmx_type = jmx.get("type")

                            # 属性 refID は、予報対象日の参照番号を記述する。
                            refID = jmx.get("refID")

                            # TimeDefinesで定義した timeId に対応する。
                            DateTime = DateTime_dict[refID]

                            # 属性 condition は予報値の状態を示し、予報対象でない場合等で予報値が存在しない場合に“値なし”と記述する。

                    # └ Area 予報対象地域を記述する。
                    # └ Name 予報対象地域(予報区)の名称を記述する。
                    # └ Code 予報対象地域(予報区)のコードを記述する。

                    # ※天気予報文と天気予報用テロップ番号の対応は、府県天気予報・府県週間天気予報_解説資料付録を参照のこと

                    # ※2 予報に関する事項(気温などの地点予報)の詳細
                    # TimeSeriesInfo 時系列情報
                    # └ TimeDefines 時系列の時刻定義セット
                    # └ TimeDefine 個々の時刻定義
                    # └ DateTime 基点時刻
                    # └ Duration 対象期間
                    # └ Item ※2-1 “地点予報”参照

                    # TimeSeriesInfo
                    # └ TimeDefines 予報の対象期間を示すとともに、対応する要素の timeId を記述する。
                    # └ TimeDefine 同一 TimeSeriesInfo 内にある要素の ID(refID)に対応する ID(timeId)を記述する。
                    # └ DateTime 予報対象日について記述する。予報対象日の開始時刻を示す。
                    #     “2008-06-27T00:00:00+09:00”のように日本標準時で記述する。
                    # └ Duration 予報の対象期間を示す。値「P1D」で、1日を対象とした予報であることを示す。
                    #     DateTimeと Duration の組み合わせにより TimeDefine の示す期間は、
                    #     文章形式では DateTime 値の日単位の期間を示す。
                    # └ Item 予報の内容と予報地点を記述する。府県予報区に含まれる発表予報地点の数だけ繰り返す。※2-1参照。

                    # ※2-1 地点予報(気温の予報)の内容
                    # Item 予報の内容
                    # └ Kind 個々の予報の内容
                    # └ Property 予報要素
                    # └ Type 気象要素名
                    # └ TemperaturePart 気温
                    # └ Station 発表予想地点
                    # └ Name 発表予想地点の名称
                    # └ Code 発表予想地点のコード

                    # Item
                    # └ Kind 予報を記述する。
                    # └ Property 予報要素を記述する。
                    #     └ Type 気象要素名を記述する
                    #         TemperaturePart に記述する予想気温の内容を示し“最低気温”、“最低気温予測範囲”、
                    #         “最高気温”、“最高気温予測範囲”の値をとる。
                    elif Property_Type in [
                        "最低気温",
                        " 最低気温予測範囲",
                        "最高気温",
                        "最高気温予測範囲",
                    ]:

                        # └ TemperaturePart 気温に関して記述する。
                        TemperaturePart = Item.find("TemperaturePart")

                        # └ jmx_eb:Temperature 予想気温を記述する。
                        jmx_all = TemperaturePart.find_all("jmx_eb:Temperature")

                        for jmx in jmx_all:

                            # 属性 type は“最低気温”、“最低気温予測範囲(上端)”、“最低気温予測範囲(下端)”、“最高気温”、
                            #     “最高気温予測範囲(上端)”、“最高気温予測範囲(下端)”の値をとり、予想気温の内容を示す。
                            jmx_type = jmx.get("type")

                            # 属性 unit は気温の単位を示す。

                            # 属性 refID は、予報対象日の参照番号を記述する。
                            refID = jmx.get("refID")

                            # TimeDefines で定義した timeId に対応する。
                            DateTime = DateTime_dict[refID]

                            # 属性 condition は予報値の状態を示し、
                            #     予報対象でない場合等で予報値が存在しない場合に“値なし”と記述する。

                            # 属性 description には予報値の文字列表現が入る。

                    # └ Station 予報対象地点について記述する。※
                    # └ Name 対象地点の名称を記述する。
                    # └ Code 対象地点のコードを記述する。
                    # ※対象地点は府県天気予報・府県週間天気予報_解説資料付録を参照のこと

                    # ※3 平年値に関する事項(気温の日別平年値)の詳細
                    # TimeSeriesInfo 時系列情報
                    # └ TimeDefines 時系列の時刻定義セット
                    # └ TimeDefine 個々の時刻定義
                    # └ DateTime 基点時刻
                    # └ Duration 対象期間
                    # └ Item ※3-1 気温の日別平年値を参照

                    # TimeSeriesInfo
                    # └ TimeDefines 記述する平年値の期間を示すとともに、対応する要素の timeId を記述する。
                    # └ TimeDefine 同一 TimeSeriesInfo 内にある要素の ID(refID)に対応する ID(timeId)を記述する。
                    # └ DateTime 平年値の日付について記述する。掲載した平年値の開始時刻(日付)を示す。
                    #     “2008-06-27T00:00:00+09:00”のように日本標準時で記述する。
                    # └ Duration 予報の対象期間を示す。値「P1D」で、日別の平年値であることを示す。
                    #     DateTime と Durationの組み合わせによりTimeDefineの示す期間は、
                    #     文章形式ではDateTime値の日単位の期間を示す。
                    # └ Item 平年値の内容と掲載地点を記述する。掲載地点の数だけ繰り返す。※3-1を参照。

                    # ※3-1 気温の日別平年値の内容
                    # Item 予報の内容
                    # └ Kind 個々の予報の内容
                    # └ Property 予報要素
                    # └ Type 気象要素名
                    # └ TemperaturePart 気温
                    # └ Station 発表予想地点
                    # └ Name 発表予想地点の名称
                    # └ Code 発表予想地点のコード

                    # Item
                    # └ Kind 予報を記述する。
                    # └ Property 予報要素を記述する。
                    # └ Type 気象要素名を記述する。
                    #     TemperaturePart に記述する予想気温の内容を示し“最低気温平年値”、“最高気温平年値”の値をとる。
                    elif Property_Type in ["最低気温平年値", "最高気温平年値"]:

                        # └ TemperaturePart 気温に関して記述する。
                        TemperaturePart = Property.find("TemperaturePart")

                        # └ jmx_eb:Temperature 気温の値を記述する。
                        jmx_all = TemperaturePart.find("jmx_eb:Temperature")

                        for jmx in jmx_all:
                            # 属性 type は、“最低気温平年値”、“最高気温平年値”の値をとり、気温の内容を示す。
                            jmx_type = jmx.get("type")

                            # 属性 unit は気温の単位を示す。

                            # 属性 refID は、対象日の参照番号を記述する。
                            refID = jmx.get("refID")

                            # TimeDefines で定義した timeId に対応する。
                            DateTime = DateTime_dict[refID]

                            # 属性 description には値の文字列表現が入る。

                    # └ Station 対象地点を記述する。※
                    # └ Name 地点の名称を記述する。
                    # └ Code 地点のコードを記述する。
                    # ※対象地点は府県天気予報・府県週間天気予報_解説資料付録を参照のこと

            # ※4 平年値に関する事項(7日間降水量の平年値)の詳細
            # MeteorologicalInfo
            # └ DateTime 基点時刻
            # └ Duration 期間の長さ
            # └ Item ※4-1 降水量平年値を参照
            #
            # タグ 解説
            #
            # MeteorologicalInfo
            # └ DateTime 平年値の開始日を示す。いつからの平年値に関する記述であるかを示す。
            # └ Duration 平年値の期間を示す。値「P7D」で7日間の平年値に関する記述であることを示す。
            # DateTime と Duration の組み合わせにより MeteorologicalInfo の示す期間は、文章形式
            # では DateTime 値の日単位の期間を示す。
            #
            # └ Item 平年値の内容と掲載地点を記述する。掲載地点の数だけ繰り返す。※4-1参照。
            #
            # ※4-1 降水量の平年値の内容
            # └ Item 予報の内容
            # └ Kind 個々の予報の内容
            # └ Property 予報要素
            # └ Type 気象要素名
            # └ PrecipitationClassPart 平年値の階級閾値
            #
            # タグ 解説
            #
            # Item
            # └ Kind 予報を記述する。
            # └ Property 予報要素を記述する。
            # └ Type 気象要素名を記述する。PrecipitationClassPart に記述する降水量の内容を示し
            #
            # “降水量7日間合計階級閾値”の値をとる。
            # └ PrecipitationClassPart 降水量の平年値階級について記述する。
            #
            # 例えば、「平年並」の範囲は
            # jmx_eb:ThresholdOfBelowNormal「平年より少ないとなる閾値」を超え、
            # jmx_eb:ThresholdOfAboveNormal「平年より多いとなる閾値」以下となる。
            # └ jmx_eb:ThresholdOfMinimum かなり少ないときの最小値(参考値)を記述する。属性 type は“降水量7日間合計
            # 階級最小値”の値をとり、階級閾値の内容を示す。属性 unit は降水量の単位を示す。
            # 属性 description には値の文字列表現が入る。
            #
            # └ jmx_eb:ThresholdOfSignificantlyBelowNormal 平年よりかなり少ないとなる閾値を記述する。属性 type は“降水量7日間合計階級
            # かなり少ない”の値をとり、階級閾値の内容を示す。属性 unit は降水量の単位を示
            # す。属性 description には値の文字列表現が入る。
            #
            # └ jmx_eb:ThresholdOfBelowNormal 平年より少ないとなる閾値を記述する。属性 type は“降水量7日間合計階級少ない”
            # の値をとり、階級閾値の内容を示す。属性 unit は降水量の単位を示す。属性
            #
            # description には値の文字列表現が入る。
            #
            # └ jmx_eb:ThresholdOfAboveNormal 平年より多いとなる閾値を記述する。属性 type は“降水量7日間合計階級多い”の
            # 値をとり、階級閾値の内容を示す。属性 unit は降水量の単位を示す。属性
            # description には値の文字列表現が入る。
            #
            # └ jmx_eb:ThresholdOfSignificantlyAboveNormal 平年よりかなり多いとなる閾値を記述する。属性 type は“降水量7日間合計階級か
            # なり多い” の値をとり、階級閾値の内容を示す。属性 unit は降水量の単位を示す。
            # 属性 description には値の文字列表現が入る。
            #
            # └ jmx_eb:ThresholdOfMaximum かなり多いとなるときの最大値(参考値)を記述する。属性 type は“降水量7日間
            # 合計階級最大値”の値をとり、階級閾値の内容を示す。属性 unit は降水量の単位を
            # 示す。属性 description には値の文字列表現が入る。
            #
            # └ Station 対象地点について記述する。※
            # └ Name 地点の名称を記述する。
            # └ Code 地点のコードを記述する。
            # ※対象地点は府県天気予報・府県週間天気予報_解説資料付録を参照のこと

        return df


if __name__ == "__main__":
    config_path = "disaster.json"
    etl_vpfw50 = ETL_VPFW50(config_path, "regular", "VPFW50")
    etl_vpfw50.xml_to_csv()
