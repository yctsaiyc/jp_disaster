from disaster import ETL_jp_disaster
from bs4 import BeautifulSoup
import pandas as pd


class ETL_VPRN50(ETL_jp_disaster):
    def xml_to_df(self, xml_path, soup):
        df = pd.DataFrame(columns=self.columns)

        # 2 各部の構成と内容
        # (1) 管理部
        # 1 管理部(Control)の構成と内容

        # Control
        # └Title 情報名称 電文の種別を示すための情報名称を示す。“「大雨危険度通知」”で固定。
        # └DateTime 発表時刻 発表時刻。未来時刻にはならない。
        #     “2008-06-26T01:51:00Z”のように協定世界時で記述する。
        # └Status 運用種別 本情報の位置づけ。“通常”“訓練”“試験”のいずれかを記載。
        #     “訓練”“試験”は正規の情報として利用してはならないことを示す。
        # └EditorialOffice 編集官署名 実際に発表作業を行った「編集官署名」を示す。“気象庁本庁”とする。
        # └PublishingOffice 発表官署名 本情報を業務的に発表した「発表官署名」を示す。“気象庁”とする。

        # (2) ヘッダ部
        # 1 ヘッダ部(Head)の構成と内容
        # Head
        # └Title 標題 情報を示す標題。具体的な内容が判別できる名称であり、
        #     可視化を目的として利用する。“大雨危険度通知”と記述する。
        # └ReportDateTime 発表時刻 本情報の公式な発表時刻を示す。“2008-06-26T11:00:00+09:00”のように日本標準時で記述する。
        #     大雨危険度通知においては、危険度分布の解析時刻に合わせる。
        ReportDateTime = self.format_datetime(soup.find("ReportDateTime").text)

        # └TargetDateTime 基点時刻本情報の対象となる時刻・時間帯の基点時刻を示す。
        #     “ 2008-06-28T06:00:00+09:00”のように日本標準時で記述する。
        #     大雨危険度通知においては、危険度分布の解析時刻に合わせる。
        TargetDateTime = self.format_datetime(soup.find("TargetDateTime").text)

        # └TargetDuration 基点時刻からの取りうる時間 本情報では本要素を用いない。
        # └EventID 識別情報 本情報では値は記述しない。
        # └InfoType 情報形態 本情報の形態を示す。“発表”“訂正”“遅延”のいずれかを記述する。
        # └Serial 情報番号 本情報では値は記述しない。
        # └InfoKind スキーマの運用種別情報 同一スキーマ上における情報分類に応じた運用を示す種別情報である。
        #     “気象危険度通知”と記述する。
        # └InfoKindVersion スキーマの運用種別情報のバージョンスキーマの運用種別情報におけるバージョン番号を示す。
        #     本解説のバージョン番号は“1.3_0”。
        # └Headline 見出し要素 防災気象情報事項となる見出し要素を示す。本情報では何も記述しない。
        # └Text 見出し文 本情報では値は記述しない。

        # (3) 内容部
        # 1 内容部(Body)の構成と内容
        # Body
        # └MeteorologicalInfos 予報の項目 予報の項目を属性typeで指定する。属性typeは“区域予報”の値をとる。
        #     予報区の区分に応じて次の順のとおりに繰り返す。
        #     ・府県予報区の危険度の情報
        #     ・一次細分区域の危険度の情報
        #     ・市町村等をまとめた地域の危険度の情報
        #     ・二次細分区域及びさらに詳細な区域の危険度の情報
        MeteorologicalInfos_all = soup.find_all("MeteorologicalInfos")

        for MeteorologicalInfos in MeteorologicalInfos_all:

            # └MeteorologicalInfo 予報事項 MeteorologicalInfos の属性 type で指定した予報の項目を記述する。
            #     危険度の情報の種別に応じて次の順のとおりに繰り返す。
            #     ・警報等から判定した危険度
            #     ・危険度分布の危険度

            # <Body>
            #     <MeteorologicalInfos type="区域予報">  # 府県予報区を対象
            #         <MeteorologicalInfo> ... </MeteorologicalInfo>  # 警報等から判定した危険度
            #         <MeteorologicalInfo> ... </MeteorologicalInfo>  # 危険度分布の危険度
            #     </MeteorologicalInfos>
            #     <MeteorologicalInfos type="区域予報">  # 一次細分区域を対象
            #         <MeteorologicalInfo> ... </MeteorologicalInfo>  # 警報等から判定した危険度
            #         <MeteorologicalInfo> ... </MeteorologicalInfo>  # 危険度分布の危険度
            #     </MeteorologicalInfos>
            #     <MeteorologicalInfos type="区域予報">  # 市町村等をまとめた地域を対象
            #         <MeteorologicalInfo> ... </MeteorologicalInfo>  # 警報等から判定した危険度
            #         <MeteorologicalInfo> ... </MeteorologicalInfo>  # 危険度分布の危険度
            #     </MeteorologicalInfos>
            #     <MeteorologicalInfos type="区域予報">  # 二次細分区域及びさらに詳細な区域を対象
            #         <MeteorologicalInfo> ... </MeteorologicalInfo>  # 警報等から判定した危険度
            #         <MeteorologicalInfo> ... </MeteorologicalInfo>  # 危険度分布の危険度
            #     </MeteorologicalInfos>
            # </Body>

            # 2内容部の個別要素の詳細
            # MeteorologicalInfo 予報事項
            MeteorologicalInfo_all = MeteorologicalInfos.find_all("MeteorologicalInfo")

            for MeteorologicalInfo in MeteorologicalInfo_all:
                # └DateTime 基点時刻 Body/MeteorologicalInfos/MeteorologicalInfo/Item/Kind/Property/Type
                #     に示す気象要素名が“危険度”の場合は警報等を考慮した危険度の判定時刻を、
                #     “危険度分布”の場合は危険度分布の解析時刻を示す。
                #     “2008-01-10T00:00:00+09:00”のように日本標準時で記述する。
                DateTime = self.format_datetime(
                    MeteorologicalInfo.find("DateTime").text
                )

                # └Item 予報の内容 MeteorologicalInfo の順に応じて、
                #     「警報等から判定した危険度」又は「危険度分布の危険度」を対象予報区分に応じて記述する。
                #     対象予報区分に含まれる予報区の数だけ繰り返す。

                # ※1予報の内容の詳細

                # Item 予報の内容
                for Item in MeteorologicalInfo.find_all("Item"):
                    # └Kind 個々の予報の内容 予報を記述する。
                    Kind = Item.find("Kind")

                    #     └Property 予報要素 予報要素を記述する。
                    Property = Kind.find("Property")

                    #         └Type 気象要素名 気象要素名を記述する。
                    #             Type が"危険度”の場合、SignificancyPart に警報等から判定した危険度を、
                    #             Type が"危険度分布”の場合、SignificancyPart に危険度分布の危険度を記述する。
                    #             ・"危険度"...注意報・警報、土砂災害警戒情報、指定河川洪水予報、早期注意情報、
                    #                 危険度分布の発表状況を考慮した対象地域内の危険度を表す。
                    #             ・"危険度分布"...危険度分布による対象地域内の危険度を表す。
                    Property_Type = Property.find("Type").text

                    #         └SignificancyPart
                    #             危険度 危険度を記述する。
                    #             ※1-1 「警報等から判定した危険度」の詳細、※1-2 「危険度分布の危険度」の詳細を参照。
                    SignificancyPart = Property.find("SignificancyPart")

                    # └Area 対象地域 発表予報区を記述する。
                    Area = Item.find("Area")

                    #     @codeType 対象地域のコードの種別
                    #     発表予報区のコードの種別を、"府県予報区"、"二次細分区域"、"土砂災害警戒情報"、
                    #     "政令指定都市"などと記述する。※2「二次細分区域及びさらに詳細な区域」の詳細を参照。
                    Area_codeType = Area.get("codeType")

                    #     └Name 対象地域の名称 発表予報区の名称を、"東京地方""大阪府"などと記述する。
                    Area_Name = Area.find("Name").text

                    #     └Code 対象地域のコード 発表予報区のコード番号を、"130010" "270000"などと記述する。

                    #     └Prefecture 府県予報区名 Area 要素の codeType 属性値が"一次細分区域"、"市町村等をまとめた地域"、
                    #         "二次細分区域"の場合にのみ記述する。当該 Item 要素で示す対象地域が属する府県予報区名を、
                    #         "東京都"などと記述する。
                    Area_Prefecture = Area.find("Prefecture")

                    if Area_Prefecture:
                        Area_Prefecture = Area_Prefecture.text

                    #     └PrefectureCode 府県予報区コード Prefecture 要素が存在する場合、当該府県予報区を示す区域コード番号を、
                    #         "130000"などと記述する。

                    #     └CityCode 市町村(二次細分区域)コード

                    # MeteorologicalInfos が「二次細分区域及びさらに詳細な区域の危険度の情報」を示す場合にのみ記述する。
                    # 当該 Item 要素で示す対象地域が二次細分区域より詳細な地域を示している場合、
                    # 本対象地域が属する二次細分区域コード番号を、"1300100" "2700100"などと記述する。
                    # 二次細分区域の場合は要素ごと省略する。※2「二次細分区域及びさらに詳細な区域」の詳細を参照。

                    # ※1-1 「警報等から判定した危険度」の詳細

                    # SignificancyPart 危険度
                    # └Base 卓越内容 卓越する内容を記述する。
                    #     └Significancy 危険度 危険度を示す。type 属性値に応じて繰り返す。
                    Significancy_all = SignificancyPart.find_all("Significancy")
                    signigicancy_dict = {}

                    for Significancy in Significancy_all:
                        #     @type 分類 Significancy 要素の示す危険度の分類を記述する。
                        #         Significancy 要素の繰り返しに際して、次の順のとおりに出現する。
                        #         ・"大雨危険度"...注意報・警報、土砂災害警戒情報、指定河川洪水予報、早期注意情報、
                        #             危険度分布の発表状況を考慮した大雨(総合)危険度であることを示す。
                        #         ・"土砂災害危険度"...注意報・警報、土砂災害警戒情報、早期注意情報、
                        #             危険度分布の発表状況から判定した土砂災害の危険度であることを示す。
                        #         ・"浸水害危険度"...注意報・警報、早期注意情報、危険度分布の発表状況から判定した
                        #             浸水害の危険度であることを示す。
                        #         ・"洪水害危険度"...注意報・警報、指定河川洪水予報、早期注意情報、
                        #             危険度分布の発表状況から判定した洪水害の危険度であることを示す。
                        #         それぞれの危険度の判定に用いる情報の詳細は、別表1のとおり。
                        Significancy_type = Significancy.get("type")

                        #     └Name 危険度の内容 危険度の内容を記述する。危険度についての説明的記述を行う。
                        #         原則として、表示に用い、判定にはコードを用いる。詳細は別表2のとおり。
                        Significancy_Name = Significancy.find("Name").text

                        signigicancy_dict[Significancy_type] = Significancy_Name

                        #     └Code 危険度コード 危険度コードを記述する。危険度の判定に用いる。詳細は別表2のとおり。

                        #     └Condition 状況の補足説明 危険度の状況を記述する。
                        #         当該予報区、当該危険度の要素において、前回の解析時刻からの状況の変化を"上昇"、
                        #         "継続"、"下降"で示し、状況の変化を算出できない場合は"欠測"で示す。
                        #         (データを配信できない期間が1時間以内であれば、復旧した際に配信するデータには、
                        #         配信できなくなる直前のデータの危険度と比較した結果を“上昇”、“継続”、
                        #         “下降”のいずれかを示す。1時間以上にわたりデータが配信できないような状況となった場合、
                        #         復旧した際に配信するデータには“欠測”を示す。)

                        #     └Remark 注意事項・付加事項 注意事項・付加事項等を示す。現時点では未使用。

                    df.loc[len(df)] = [
                        ReportDateTime,  # 発表時刻
                        TargetDateTime,  # 基点時刻
                        DateTime,  # 基点時刻2
                        Property_Type,  # 気象要素名
                        Area_codeType,  # 対象地域のコードの種別
                        Area_Name,  # 対象地域の名称
                        Area_Prefecture,  # 府県予報区名
                        signigicancy_dict.get("大雨危険度"),  # 大雨危険度
                        signigicancy_dict.get("土砂災害危険度"),  # 土砂災害危険度
                        signigicancy_dict.get("浸水害危険度"),  # 浸水害危険度
                        signigicancy_dict.get("洪水害危険度"),  # 洪水害危険度
                    ]

                    # ※1-2 「危険度分布の危険度」の詳細

                    # SignificancyPart 危険度
                    #
                    # └Base 卓越内容 卓越する内容を記述する。
                    # └Significancy 危険度 危険度を示す。type 属性値に応じて繰り返す。
                    #     @type 分類 Significancy 要素の示す危険度の分類を記述する。
                    #         Significancy 要素の繰り返しに際して、次の順のとおりに出現する。

                    #         ・"土砂災害危険度"...大雨警報(土砂災害)の危険度分布(土砂災害警戒判定メッシュ情報)
                    #             の危険度であることを示す。
                    #         ・"浸水害危険度"...大雨警報(浸水害)の危険度分布の危険度であることを示す。
                    #         ・"洪水害危険度"...洪水警報の危険度分布の危険度であることを示す。
                    #
                    # └Name 危険度の内容 危険度の内容を記述する。危険度についての説明的記述を行う。
                    #     原則として、表示に用い、判定にはコードを用いる。詳細は別表3のとおり。

                    # └Code 危険度コード 危険度コードを記述する。危険度の判定に用いる。詳細は別表3のとおり。

                    # └Condition 状況の補足説明 危険度の状況を記述する。
                    #     当該予報区、当該危険度の要素において、前回の解析時刻からの状況の変化を"上昇"、
                    #     "継続"、"下降"で示し、状況の変化を算出できない場合は"欠測"で示す。
                    #     (データを配信できない期間が1時間以内であれば、復旧した際に配信するデータには、
                    #     配信できなくなる直前のデータの危険度と比較した結果を“上昇”、“継続”、“下降”
                    #     のいずれかを示す。1時間以上にわたりデータが配信できないような状況となった場合、
                    #     復旧した際に配信するデータには“欠測”を示す。)

                    # └Remark 注意事項・付加事項 注意事項・付加事項等を示す。現時点では未使用。

        return df


if __name__ == "__main__":
    config_path = "disaster.json"
    etl_vprn50 = ETL_VPRN50(config_path, "regular", "VPRN50")
    etl_vprn50.xml_to_csv()
