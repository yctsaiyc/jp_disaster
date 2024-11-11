from disaster import ETL_jp_disaster
import pandas as pd

### from airflow.exceptions import AirflowFailException


# 地方季節予報 (地方１か月予報，地方３か月予報，地方暖・寒候期予報)
# 気象庁防災情報XMLフォーマット　技術資料: https://xml.kishou.go.jp/tec_material.html
# 電文毎の解説資料: https://xml.kishou.go.jp/jmaxml_20241031_Manual(pdf).zip
class ETL_VPCK50(ETL_jp_disaster):
    def xml_to_df(self, xml_path, soup):
        try:
            df = pd.DataFrame(columns=self.columns)

            # 1.Control 部

            # 1-1 Title  電文の種別を示すための情報名称
            # [解説] 次の値のいずれかをとる
            # "全般1か月予報" "地方1か月予報"
            # "全般3か月予報" "地方3か月予報"
            # "全般暖・寒候期予報" "地方暖・寒候期予報"
            Title = soup.find("jmx:Title").text

            # 2.Head 部

            # 2-1 Title 情報の標題

            # 2-2 ReportDateTime 発表時刻
            ReportDateTime = self.format_datetime(soup.find("ReportDateTime").text)

            # 2-3 TargetDateTime 基点時刻
            TargetDateTime = self.format_datetime(soup.find("TargetDateTime").text)

            # 3.Body 部

            # 3-1 TargetArea 部 発表官署の担当地域を記載する

            # 3-3 MeteorologicalInfos 部
            # Type 属性の属性値は"季節予報"で固定
            # 各地域ごと(MeteorologicalInfo)や各期間ごと(TimeSeriesInfo)の予報内容に関する事項を記載する
            # [解説] 標題(<Head><Title>)によって、この要素の子要素は次のようになる
            # ・寒候期、暖候期以外の場合
            # <MeteorologicalInfo>
            # <TimeSeriesInfo>
            # ・寒候期の場合
            # <MeteorologicalInfo>
            # ・暖候期のとき
            # <MeteorologicalInfo>
            # <MeteorologicalInfo> ※梅雨の情報を出力する。北海道の場合は出力しない
            # <MeteorologicalInfo> ※沖縄、奄美の梅雨の情報を出力する。出力する梅雨情報に沖縄、奄美がない場合は出力しない

            # 3-3-1 MeteorologicalInfo 部
            # [解説] 各地域ごとの予報内容(予報基点時刻(DateTime)、予報期間の長さ(Duration)、予報期間の内容(Name)、
            # 予想確率値・確率文等(Item))を記載する
            MeteorologicalInfo = soup.find("MeteorologicalInfo")

            # 3-3-1-1 DateTime
            # 予報・観測の基点時刻
            # [解説] significant 属性値により、時刻の有効部分が["日"]までであることを示す。
            # JST 表記の DateTime 型で表記する
            DateTime = self.format_datetime(MeteorologicalInfo.find("DateTime").text)

            # 3-3-1-2 Duration
            # 予報期間の長さ
            # [解説] "P1M" : 1か月予報の場合
            # "P3M" : 3か月予報・寒候期予報・暖候期予報の場合
            # "P2M" : 梅雨情報をもつ<MeteorologicalInfo>の場合 ※暖候期予報のみ

            # 3-3-1-3 Name
            # 予報時間の内容
            # [解説] 予報期間の文字表現として、次の値のいずれかをとる
            # "向こう1か月" : 1か月予報の場合
            # "向こう3か月" : 3か月予報の場合
            # "冬(12月から2月)" : 寒候期予報の場合
            # "夏(6月から8月)" : 暖候期予報の場合
            # "梅雨の時期(6月から7月、沖縄・奄美では5月から6月)": 全般暖候期予報の場合
            # "梅雨の時期(5月から6月)" : 沖縄地方暖候期予報の場合
            # "梅雨の時期(九州南部では6月から7月、奄美地方では5月から6月)" : 九州南部・奄美地方暖候期予報の場合
            # "梅雨の時期(6月から7月)" : (九州北部~東北の各地方暖候期予報)の場合
            Name = MeteorologicalInfo.find("Name").text

            # 3-3-1-4 Item 部
            # 概況文・特徴のある確率文を出力する Item
            # [解説]概況文・特徴のある確率文(Kind/Property 部) 及び地域(Areas 部)を記載する。
            # 概況文、特徴のある確率文を出力する Item は 1 つのみ
            Items = MeteorologicalInfo.find_all("Item")

            for Item in Items:

                # 3-3-1-4-1 Property 部
                # 概況文・特徴のある確率文を記載する
                # [解説] ・概況前文(Type)と天候の特徴(ClimatFeaturePert)を記載する
                # ・Property/Type は概況文の前文として、次の値をとる"出現の可能性が最も大きい天候と、特徴のある気温、降水量等の確率"
                if (
                    Item.find("Type").text
                    == "出現の可能性が最も大きい天候と、特徴のある気温、降水量等の確率"
                ):
                    pass  # 資料是文字

                    # 3-3-1-4-1-1 ClimateFeaturePart 部
                    # 天候の特徴
                    # [解説] 出現の可能性が最も大きい天候を平文表現(GeneralSituationText)で記載する。
                    # ※3か月予報の場合の GeneralSituationText は(MeteorologicalInfos/TimeSeires)以下で出現する
                    # 特徴のある気温、降水量、日照時間等の確率(SingnificantClimateElement 部)を記載する

                    # 3-3-1-4-1-1-1 jmx_eb:SignificantClimateElement 部
                    # 特徴のある気温、降水量、日照時間等の確率
                    # [解説] kind 属性により、特徴のある確率の気象要素名を記載する。
                    # 属性値は次のいずれかをとる "気温""降水量" "日照時間" "降雪量"
                    # 複数の気象要素が「特徴のある確率」をもつ場合は、それぞれの気象要素についてこの要素を出力し、こ
                    # の要素の子要素(significantClimateElement/Text)で特徴のある確率を平文で記載する

                    # 3-3-1-4-2 Areas 部
                    # 地域名要素全体
                    # [解説] codeType 属性の属性値は"全国・地方予報区等"で固定。
                    # 対象地域(Area)を記載する
                    # 属性値により、Area の子要素のコード種別(Area/Code)が"全国・地方予報区等"であることを示す
                    # Kind 部で表示する内容の対象となる地域名称(Area/Name)とコード値(Area/Code)を記載する

                # 3-3-1-5 Item 部
                # 各地域の確率値を出力する Item
                # [解説]予報要素・確率値(Kind/Property 部) 及び地域(Areas 部)を記載する。
                # 1地域・1気象要素ごとに Item を出力する(複数個出力可)

                # 3-3-1-5-1 Property 部
                # 予報要素・確率値を記載する
                # [解説] ・概況前文(Type)と天候の特徴(ClimateProbabilityValuesPart)を記載する
                # ・Property/Type は概況文の前文として、次の値をとる"地域・期間平均平年偏差各階級の確率"
                elif Item.find("Type").text == "地域・期間平均平年偏差各階級の確率":

                    # 3-3-1-5-1-1 ClimateProbabilityValuesPart 部
                    # 気温、降水量、日照時間等の確率
                    # [解説] 気温、降水量、日照時間等の確率(ClimateProbabilityValues 部)を記載する

                    # 3-3-1-5-1-1-1 jmx_eb:ClimateProbabilityValues 部
                    # [解説] kind 属性により、気象要素名を記載する。
                    # 属性値は次のいずれかをとる "気温" "降水量" "日照時間" "降雪量"
                    # 確率値は、
                    # ・平年より低い(少ない)確率(ProbabilityOfBelowNormal)
                    # ・平年並の確率(ProbabilityOfNormal)
                    # ・平年より多い(多い)確率(ProbabilityOfAboveNormal)
                    # で記載する
                    kind = Item.find("jmx_eb:ClimateProbabilityValues").get("kind", "")

                    # 3-3-1-5-1-1-1-1 jmx_eb:ProbabillityOfBelowNormal
                    # jmx_eb:ProbabillityOfNormal
                    # jmx_eb:ProbabillityOfAboveNormal
                    # [解説] unit 属性により、単位を示す。確率値の場合は"%"で固定
                    # significant 属性は出力する値が「特徴のある確率」であるときのみ"true"を出力
                    # jmx_eb:ClimateProbabilityValues の kind 属性で指定された気象要素に対する確率値を出力する。
                    pobn = Item.find("jmx_eb:ProbabilityOfBelowNormal")

                    if pobn:
                        pobn = pobn.text

                    pon = Item.find("jmx_eb:ProbabilityOfNormal")

                    if pon:
                        pon = pon.text

                    poan = Item.find("jmx_eb:ProbabilityOfAboveNormal")

                    if poan:
                        poan = poan.text

                    # 3-3-1-5-2 Areas 部
                    # 地域名要素全体
                    # [解説] codeType 属性の属性値は"全国・地方予報区等"で固定。
                    # 対象地域(Area)を記載する
                    # 属性値により、Area の子要素のコード種別(Area/Code)が"全国・地方予報区等"であることを示す
                    # Kind 部で表示する内容の対象となる地域名称(Area/Name)とコード値(Area/Code)を記載する
                    Area_Name = Item.find("Area").find("Name").text

                    df.loc[len(df)] = [
                        Title,  # 電文の種別を示すための情報名称
                        ReportDateTime,  # 発表時刻
                        TargetDateTime,  # 基点時刻
                        DateTime,  # 予報・観測の基点時刻
                        Name,  # 予報・観測時間の内容
                        Area_Name,  # 対象地域
                        kind,  # 気象要素名
                        pobn,  # 平年より低い(少ない)確率
                        pon,  # 平年並の確率
                        poan,  # 平年より多い(多い)確率
                    ]

            # 3-3-2 TimeSeiresInfo 部
            # 時系列情報
            # [解説] 期間の定義(TimeDefines)、各期間ごとの予報内容(Item)を記載する
            # 暖候期予報・寒候期予報の場合は出現しない
            TimeSeriesInfo = soup.find("TimeSeriesInfo")

            # 3-3-2-1 TimeDefines 部
            # 時系列の時刻定義群
            # [解説] この要素が示す時系列の時刻定義(TimeDefine)群を示す
            TimeDefines = TimeSeriesInfo.find("TimeDefines")

            # 3-3-2-1-1 TimeDefine 部
            # 個々の時刻定義
            # [解説] timeId 属性により、各期間の時刻 ID を付加する。属性値は 1~3 の連番を振る
            TimeDefine_DateTime_dict = {}
            TimeDefine_Name_dict = {}

            for TimeDefine in TimeDefines.find_all("TimeDefine"):

                # 3-3-2-1-1-1 DateTime 部
                # 基点時刻
                # [解説] 時刻 ID に対応する各期間の基点時刻を JST 表記の DateTime 型で表記する
                # 時刻の有効部分は["日"]までである

                # 3-3-2-1-1-2 Duration 部
                # 対照期間
                # [解説] 時刻 ID に対応する予報対象期間を記載する。次の値をとる
                # 1か月予報の1週目・2週目 : "P7D"
                #     3~4週目 : "P14D"
                # 3か月予報 : "P1M"

                # 3-3-2-1-1-3 Name 部
                # 予報・観測時間の内容
                # [解説] 予報期間、観測時間を文章で示す
                # 1か月予報 : "1週目" "2週目" "3~4週目"
                # 3か月予報 : "*月" ※予報対象期間内の各月(1か月目の月・2か月目の月・3か月目の月)
                timeId = TimeDefine.get("timeId")
                TimeDefine_DateTime = self.format_datetime(
                    TimeDefine.find("DateTime").text
                )
                TimeDefine_Name = TimeDefine.find("Name").text

                TimeDefine_DateTime_dict[timeId] = TimeDefine_DateTime
                TimeDefine_Name_dict[timeId] = TimeDefine_Name

            # 3-3-2-2 Item 部
            # 期間ごとの概況文・特徴のある確率文を出力する Item
            # [解説] 概況文・特徴のある確率文(Kind/Property 部) 及び地域(Areas 部)を記載する。
            # 期間ごとの概況文、特徴のある確率文を出力する Item は 1 つのみ
            Items = TimeSeriesInfo.find_all("Item")

            for Item in Items:

                # 3-3-2-2-1 Property 部
                # [解説]「3-3-1-4-1 Property 部」と同様
                if (
                    Item.find("Type").text
                    == "出現の可能性が最も大きい天候と、特徴のある気温、降水量等の確率"
                ):
                    pass  # 資料是文字，內容與"地域・期間平均平年偏差各階級の確率"重複

                    # 3-3-2-2-1-1 ClimateFeaturePart 部
                    # 天候の特徴
                    # [解説] 3か月予報の場合のみ、出現の可能性が最も大きい天候を平文表現(GeneralSituationText)で記載する
                    # 特徴のある気温、降水量、日照時間等の確率(SingnificantClimateElement 部)をそれぞれ記載する

                    # 3-3-2-2-1-1-1 GeneralSituationText
                    # 出現の可能性が最も大きい天候の平文表現
                    # [解説] refID 属性の属性値は TimeSeriesInfo/TimeDefines/TimeDefine で定義した timeId の属性値をセットする
                    # 3か月予報の場合のみ出現する

                    # 3-3-2-2-1-1-2 jmx_eb:SignificantClimateElement 部
                    # 特徴のある気温、降水量、日照時間等の確率
                    # [解説] 「3-3-1-4-1-1-1 jmx_eb:SignificantClimateElement 部」 と同様
                    # kind 属性により、特徴のある確率の気象要素名を記載する。
                    # 属性値は次のいずれかをとる "気温" "降水量" "日照時間" "降雪量"
                    # 複数の気象要素が「特徴のある確率」をもつ場合は、それぞれの気象要素についてこの要素を出力し、こ
                    # の要素の子要素(significantClimateElement/Text)で特徴のある確率を平文で記載する

                    # 3-3-2-2-1-1-2-1 Text
                    # 特徴のある確率を平文で記載する
                    # [解説] refID 属性の属性値は TimeSeriesInfo/TimeDefines/TimeDefine で定義した timeId の属性値をセットする

                    # 3-3-2-2-2 Areas 部
                    # 地域名要素全体
                    # [解説] codeType 属性の属性値は"全国・地方予報区等"で固定。
                    # 対象地域(Area)を記載する
                    # 属性値により、Area の子要素のコード種別(Area/Code)が"全国・地方予報区等"であることを示す
                    # Kind 部で表示する内容の対象となる地域名称(Area/Name)とコード値(Area/Code)を記載する

                # 3-3-2-3 Item 部
                # 各期間ごとの確率値を出力する Item
                # [解説]予報要素・確率値(Kind/Property 部) 及び地域(Areas 部)を記載する。
                # 1地域・1気象要素ごとに Item を出力する(複数個出力可)

                # 3-3-2-3-1 Property 部
                # [解説] 「3-3-1-5-1 Property」と同様
                elif Item.find("Type").text == "地域・期間平均平年偏差各階級の確率":

                    # 3-3-2-3-1-1 ClimateProbabilityValuesPart
                    # [解説] 3-3-1-5-1-1 「ClimateProbabilityValuesPart」と同様

                    # 3-3-2-2-1-1-1 jmx_eb:ClimateProbabilityValues 部
                    cpvs = Item.find_all("jmx_eb:ClimateProbabilityValues")

                    for cpv in cpvs:

                        # [解説] refID 属性の属性値は TimeSeriesInfo/TimeDefines/TimeDefine で定義した timeId の属性値をセットする
                        # kind 属性により、気象要素名を記載する。
                        # kind 属性の値は次のいずれかをとる "気温""降水量" "日照時間" "降雪量"
                        # 確率値は、
                        # ・平年より低い(少ない)確率(ProbabilityOfBelowNormal)
                        # ・平年並の確率(ProbabilityOfNormal)
                        # ・平年より多い(多い)確率(ProbabilityOfAboveNormal)
                        # で記載する
                        kind = cpv.get("kind", "")
                        refID = cpv.get("refID", "")

                        # 3-3-2-2-1-1-1-1 jmx_eb:ProbabilityOfBelowNormal
                        # jmx_eb:ProbabilityOfNormal
                        # jmx_eb:ProbabilityOfAboveNormal
                        # [解説] 3-3-1-5-1-1-1-1 と同様
                        pobn = cpv.find("jmx_eb:ProbabilityOfBelowNormal")

                        if pobn:
                            pobn = pobn.text

                        pon = cpv.find("jmx_eb:ProbabilityOfNormal")

                        if pon:
                            pon = pon.text

                        poan = cpv.find("jmx_eb:ProbabilityOfAboveNormal")

                        if poan:
                            poan = poan.text

                        TimeDefine_DateTime = TimeDefine_DateTime_dict[refID]
                        TimeDefine_Name = TimeDefine_Name_dict[refID]

                        # 3-3-2-3-2 Areas 部
                        # 地域名要素全体
                        # [解説] codeType 属性の属性値は"全国・地方予報区等"で固定。
                        # 対象地域(Area)を記載する
                        # 属性値により、Area の子要素のコード種別(Area/Code)が"全国・地方予報区等"であることを示す
                        # Kind 部で表示する内容の対象となる地域名称(Area/Name)とコード値(Area/Code)を記載する
                        Area_Name = Item.find("Area").find("Name").text

                        df.loc[len(df)] = [
                            Title,  # 電文の種別を示すための情報名称
                            ReportDateTime,  # 発表時刻
                            TargetDateTime,  # 基点時刻
                            TimeDefine_DateTime,  # 予報・観測の基点時刻
                            TimeDefine_Name,  # 予報・観測時間の内容
                            Area_Name,  # 対象地域
                            kind,  # 気象要素名
                            pobn,  # 平年より低い(少ない)確率
                            pon,  # 平年並の確率
                            poan,  # 平年より多い(多い)確率
                        ]

            return df

        except Exception as e:
            raise  ### AirflowFailException(e)


if __name__ == "__main__":
    config_path = "disaster.json"
    etl_vpck50 = ETL_VPCK50(config_path, "regular", "VPCK50")
    etl_vpck50.xml_to_csv()
