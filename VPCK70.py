from disaster import ETL_jp_disaster
import pandas as pd


# 地方季節予報（２週間気温予報）
# 気象庁防災情報XMLフォーマット　技術資料: https://xml.kishou.go.jp/tec_material.html
# 電文毎の解説資料: https://xml.kishou.go.jp/jmaxml_20241031_Manual(pdf).zip


class ETL_VPCK70(ETL_jp_disaster):
    def xml_to_df(self, xml_path, soup):
        df = pd.DataFrame(columns=self.columns)

        # 2 各部の構成と内容
        # (1) 管理部
        # 1 管理部の構成
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
        # └ ReportDateTime 発表時刻
        ReportDateTime = self.format_datetime(soup.find("ReportDateTime").text)

        # └ TargetDateTime 基点時刻
        TargetDateTime = self.format_datetime(soup.find("TargetDateTime").text)

        # └ TargetDuration 基点時刻からの取りうる時間(日数)
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
        # └ TimeSeriesInfo 時系列情報

        # MeteorologicalInfos
        # 予報の項目を属性 type で指定する。
        # 属性 type は“区域予報”、“地点予報”の値をとる。
        # “区域予報”の場合は、地域平均気温(2内容部の個別要素の詳細※1参照)、
        MeteorologicalInfos_1 = soup.find("MeteorologicalInfos", type="区域予報")

        # “地点予報”の場合は、最高気温・最低気温に関する情報(2内容部の個別要素の詳細※2参照)を記述する。
        MeteorologicalInfos_2 = soup.find("MeteorologicalInfos", type="地点予報")

        # └ TimeSeriesInfo

        # MeteorologicalInfos の属性 type で指定した予報を時系列情報として記述する。

        # 2内容部の個別要素の詳細
        # ※1 予報に関する事項(区域予報)の詳細
        if MeteorologicalInfos_1:

            # TimeSeriesInfo 時系列情報
            # └ TimeDefines 時系列の時刻定義セット
            #     └ TimeDefine 個々の時刻定義
            #     └ DateTime 基点時刻
            #     └ Duration 対象期間
            #     └ Name 対象期間の内容
            # └ Item ※1-1 “区域予報”参照

            # TimeSeriesInfo
            TimeSeriesInfo_1 = MeteorologicalInfos_1.find("TimeSeriesInfo")

            # └ TimeDefines 予報の対象期間を示すとともに、対応する要素の timeId を記述する。
            TimeDefines = TimeSeriesInfo_1.find("TimeDefines")

            #     └ TimeDefine 同一 TimeSeriesInfo 内にある要素の ID(refID)に対応する ID(timeId)を記述する。
            TimeDefine_all = TimeDefines.find_all("TimeDefine")

            #     └ DateTime 予報対象日について記述する。 “2018-11-08T00:00:00+09:00”のように日本標準時で記述する。
            #                 significat 属性により、この日時が「日」単位のものであることを示す。
            #                 dubious 属性により、この日付が「頃」という程度のあいまいさを持つことを示す。
            DateTime_dict = {}

            #     └ Duration 予報の対象期間を示す。値「P5D」で、5 日を対象とした予報であることを示す。

            #     └ Name 予報の対象期間の内容を示す。予報対象日が 3 日頃の場合は、”1日-5日の5日間平均”のように記述する。
            Name_dict = {}

            for TimeDefine in TimeDefine_all:
                timeId = TimeDefine.get("timeId")

                DateTime_dict[timeId] = self.format_datetime(
                    TimeDefine.find("DateTime").text
                )

                Name_dict[timeId] = TimeDefine.find("Name").text

            # └ Item 予報の内容と予報区を記述する。地方予報区に含まれる発表予報区の数だけ繰り返す。※1-1参照。
            Item_all = TimeSeriesInfo_1.find_all("Item")

            # ※1-1 区域予報の内容
            # Item 予報の内容
            # └ Kind 個々の予報の内容
            #     └ Property 予報要素
            #         └ Type 気象要素名
            #         └ ClimateValuesPart 期間平均・地域平均気温の平年偏差階級
            # └ Area 対象地域
            #     └ Name 対象地域の名称
            #     └ Code 対象地域のコード

            # Item
            for Item in Item_all:

                Area_Name = Item.find("Area").find("Name").text

                # └ Kind 予報を記述する。
                #     └ Property 予報要素を記述する。
                #         └ Type 気象要素名を記述する。「地域平均気温」
                #         └ ClimateValuesPart 期間平均・地域平均気温の平年偏差階級を記述する。
                ClimateValuesPart = Item.find("ClimateValuesPart")
                # cvp_type = ClimateValuesPart.get("type")

                #             └ jmx_eb:Comparison 前後2日5日間平均・地域平均気温の平年偏差階級を-3から3の階級を記述する。
                Comparison_all = ClimateValuesPart.find_all("jmx_eb:Comparison")

                for Comparison in Comparison_all:

                    # 属性refID は、予報対象日の参照番号を記述するもので、TimeDefines で定義した timeId に対応する。
                    compare_type = Comparison.get("type")

                    refID = Comparison.get("refID")
                    DateTime = DateTime_dict[refID]
                    Name = Name_dict[refID]

                    # 属性 description には階級値の文字列表現が入る。
                    description = Comparison.get("description")

                    value = Comparison.text

                    df.loc[len(df)] = [
                        Title,  # 情報名称
                        ReportDateTime,  # 発表時刻
                        TargetDateTime,  # 基点時刻
                        Area_Name,  # 対象地域の名称
                        compare_type,  # 気象要素名
                        DateTime,  # 予報対象基点時刻
                        Name,  # 対象期間の内容
                        description,  # 階級値の文字列
                        value,  # 期間平均・地域平均気温の平年偏差階級
                    ]

                # └ Area 予報対象地域を記述する。※
                #     └ Name 予報対象地域(予報区)の名称を記述する。
                #         └ Code 予報対象地域(予報区)のコードを記述する。

                # ※2週間気温予報の対象地域及び対象地域について、2週間予報解説資料付録を参照

        # ※2 予報に関する事項(気温などの地点予報)の詳細
        if MeteorologicalInfos_2:

            # TimeSeriesInfo 時系列情報
            # └ TimeDefines 時系列の時刻定義セット
            #     └ TimeDefine 個々の時刻定義
            #         └ DateTime 基点時刻
            #         └ Duration 対象期間
            #         └ Name 対象期間の内容
            # └ Item ※2-1 “地点予報”参照

            # TimeSeriesInfo
            TimeSeriesInfo_2 = MeteorologicalInfos_2.find("TimeSeriesInfo")

            # └ TimeDefines 予報の対象期間を示すとともに、対応する要素の timeId を記述する。
            TimeDefines = TimeSeriesInfo_1.find("TimeDefines")

            #     └ TimeDefine 同一 TimeSeriesInfo 内にある要素の ID(refID)に対応する ID(timeId)を記述する。
            TimeDefine_all = TimeDefines.find_all("TimeDefine")

            #         └ DateTime 予報対象日について記述する。 “2018-11-08T00:00:00+09:00”のように日本標準時で記述する。
            #                     significat 属性により、この日時が「日」単位のものであることを示す。
            #                     dubious 属性により、この日付が「頃」という程度のあいまいさを持つことを示す。
            DateTime_dict = {}

            #         └ Duration 予報の対象期間を示す。値「P5D」で、5 日を対象とした予報であることを示す。

            #         └ Name 予報の対象期間の内容を示す。予報対象日が 3 日頃の場合は、”1日-5日の5日間平均”のように記述する。
            Name_dict = {}

            for TimeDefine in TimeDefine_all:
                timeId = TimeDefine.get("timeId")

                DateTime_dict[timeId] = self.format_datetime(
                    TimeDefine.find("DateTime").text
                )

                Name_dict[timeId] = TimeDefine.find("Name").text

            #     └ Item 予報の内容と予報地点を記述する。発表予報地点の数だけ繰り返す。※2-1参照。
            Item_all = TimeSeriesInfo_2.find_all("Item")

            # ※2-1 地点予報の内容
            # Item 予報の内容
            # └ Kind 個々の予報の内容
            #     └ Property 予報要素
            #         └ Type 気象要素名
            #         └ ClimateValuesPart 期間平均の最高・最低気温及び関連する値
            # └ Station 対象地点
            #     └ Name 対象地点の名称
            #     └ Code 対象地点のコード

            # Item
            for Item in Item_all:
                Station_Name = Item.find("Station").find("Name").text

                # └ Kind 予報を記述する。
                Kind = Item.find("Kind")

                #     └ Property 予報要素を記述する。
                property_all = Kind.find_all("Property")

                #         └ Type 気象要素名を記述する。ClimateValuesPart に記述する予想気温の内容を示し“最低気温”、
                #                 “最低気温予測範囲”、“最低気温階級”、“最高気温”、“最高気温予測範囲”、“最高気温階級”の値をとる。
                for property_ in property_all:

                    #     └ ClimateValuesPart 気温に関して記述する。
                    ClimateValuesPart = property_.find("ClimateValuesPart")

                    # cvp_type = ClimateValuesPart.get("type")

                    #         └ jmx_eb:Temperature 予想気温の期間平均値を記述する。
                    Temperature_all = ClimateValuesPart.find_all("jmx_eb:Temperature")

                    if Temperature_all:
                        for Temperature in Temperature_all:

                            #         Type 要素の値が“最低気温”、“最低気温予測範囲”、“最高気温”、“最高気温予測範囲”の場合に出現する。
                            temperature_type = Temperature.get("type")

                            #         属性 type の値により、予想気温の内容を期間平均値で示す。
                            value = Temperature.text

                            #         属性 unitは気温の単位を示す。

                            #         属性 refID は、予報対象日の参照番号を記述する。
                            #         TimeDefines で定義した timeIdに対応する。
                            refID = Temperature.get("refID")
                            DateTime = DateTime_dict[refID]
                            Name = Name_dict[refID]

                            #         属性 description には予報値の文字列表現が入る。
                            description = Temperature.get("description")

                            df.loc[len(df)] = [
                                Title,  # 情報名称
                                ReportDateTime,  # 発表時刻
                                TargetDateTime,  # 基点時刻
                                Station_Name,  # 対象地域の名称
                                temperature_type,  # 気象要素名
                                DateTime,  # 予報対象基点時刻
                                Name,  # 対象期間の内容
                                description,  # 階級値の文字列
                                value,  # 期間平均・地域平均気温の平年偏差階級
                            ]

                        #     └ jmx_eb:Comparison 気温の期間平均値の予想階級を記述する。
                    Comparison_all = ClimateValuesPart.find_all("jmx_eb:Comparison")

                    if Comparison_all:
                        for Comparison in Comparison_all:

                            #     Type 要素が“最低気温階級”又は“最高気温階級”の場合に出現する。
                            compare_type = Comparison.get("type")

                            #     期間平均した最高・最低気温の平年偏差階級を-3から3の階級を記述する。
                            value = Comparison.text

                            #     属性 refID は、予報対象日の参照番号を記述するもので、TimeDefines で定義した timeId に対応する。
                            refID = Comparison.get("refID")
                            DateTime = DateTime_dict[refID]
                            Name = Name_dict[refID]

                            #     属性 description には階級値の文字列表現が入る。
                            description = Comparison.get("description")

                            df.loc[len(df)] = [
                                Title,  # 情報名称
                                ReportDateTime,  # 発表時刻
                                TargetDateTime,  # 基点時刻
                                Station_Name,  # 対象地域の名称
                                compare_type,  # 気象要素名
                                DateTime,  # 予報対象基点時刻
                                Name,  # 対象期間の内容
                                description,  # 階級値の文字列
                                value,  # 期間平均・地域平均気温の平年偏差階級
                            ]

                    # └ Station 予報対象地点について記述する。※
                    # └ Name 対象地点の名称を記述する。
                    # └ Code 対象地点のコードを記述する。

                    # ※2週間気温予報の対象地域及び対象地点について、2週間気温予報解説資料付録を参照
                    #
                    # ※jmx_eb:Comparison 要素による階級の記述例
                    #
                    # <ClimateValuesPart type="地域平均気温階級">
                    # <jmx_eb:Comparison type="前後2日の5日間平均・地域平均気温階級" refID="1" description="高い確率
                    # 50%以上">1</jmx_eb:Comparison>
                    # <jmx_eb:Comparison type="前後2日の5日間平均・地域平均気温階級" refID="2" description="かなり高
                    # い確率30%以上">2</jmx_eb:Comparison>
                    # <jmx_eb:Comparison type="前後2日の5日間平均・地域平均気温階級" refID="3" description="高い確率
                    # 50%以上">1</jmx_eb:Comparison>
                    # <jmx_eb:Comparison type="前後2日の5日間平均・地域平均気温階級" refID="4" description="かなり高
                    # い確率30%以上">2</jmx_eb:Comparison>
                    # <jmx_eb:Comparison type="前後2日の5日間平均・地域平均気温階級" refID="5" description="なし
                    # ">0</jmx_eb:Comparison>
                    # </ClimateValuesPart>
                    #
                    # 気温の階級を記述する。
                    # 気温の階級は、予想気温と、同期間に
                    # おける平年の気温との偏差によって
                    # 算出するもので下記の通り-3から3の
                    # いずれかの値をとる。
                    # -3 かなり低い確率50%以上
                    # -2 かなり低い確率30%以上
                    # -1 低い確率50%以上
                    # 0 なし(他の階級に属さない)
                    # 1 高い確率50%以上
                    # 2 かなり高い確率30%以上
                    # 3 かなり高い確率50%以上
                    #
                    # ※ 「最高気温」の記述例
                    #
                    # <Property>
                    # <Type>最高気温</Type>
                    # <ClimateValuesPart type="最高気温">
                    # <jmx_eb:Temperature type="最高気温の前後2日5日間平均値" unit="度" refID="1" description="2
                    # 4度">24</jmx_eb:Temperature>
                    # <!-- (中略) 他の refID は省略 -->
                    # </ClimateValuesPart>
                    # </Property>
                    # <Property>
                    # <Type>最高気温予測範囲</Type>
                    # <ClimateValuesPart type="最高気温予測範囲(上端)">
                    # <jmx_eb:Temperature type="最高気温の前後2日5日間平均値の予測範囲(上端)" unit="度" refID="1"
                    # description="27度">27</jmx_eb:Temperature>
                    # <!-- (中略) 他の refID は省略 -->
                    # </ClimateValuesPart>
                    # </Property>
                    # <Property>
                    # <Type>最高気温予測範囲</Type>
                    # <ClimateValuesPart type="最高気温予測範囲(下端)">
                    # <jmx_eb:Temperature type="最高気温の前後2日5日間平均値の予測範囲(下端)" unit="度" refID="1"
                    # description="23度">23</jmx_eb:Temperature>
                    # <!-- (中略) 他の refID は省略 -->
                    # </ClimateValuesPart>
                    # </Property>

                    # 最高気温・最低気温の記述には、値そのもののほか予測範囲が含まれる。
                    # 気温予測範囲は、対象日の最高気温や最低気温が、どの範囲に予想されているかを示すもの。
                    # 予想されている範囲を(上端)(下端)で示す。
                    # 左の例では、発表日の 8 日先(refID="1")の最高気温の 5 日間平均値が 24 度で、
                    # 23 度~27 度の範囲に予想されていることを示す。

        return df


if __name__ == "__main__":
    config_path = "disaster.json"
    etl_vpck70 = ETL_VPCK70(config_path, "regular", "VPCK70")
    etl_vpck70.xml_to_csv()
