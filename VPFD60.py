from disaster import ETL_jp_disaster
from bs4 import BeautifulSoup
import pandas as pd


# 警報級の可能性（明日まで）
# 気象庁防災情報XMLフォーマット　技術資料: https://xml.kishou.go.jp/tec_material.html
# 電文毎の解説資料: https://xml.kishou.go.jp/jmaxml_20241031_Manual(pdf).zip


class ETL_VPFD60(ETL_jp_disaster):
    def xml_to_df(self, xml_path, soup):
        df = pd.DataFrame(columns=self.columns)

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
                        jmx_text = Property.find("jmx_eb:Precipitation").text

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
                        jmx_text = Property.find("jmx_eb:SnowfallDepth").text

                    else:
                        print(Property_Type)
                        raise

                    df.loc[len(df)] = [
                        ReportDateTime,  # 発表時刻
                        TargetDateTime,  # 基点時刻
                        DateTime,  # 基点時刻2
                        Name,  # 基点時刻3
                        Area_Name,  # 対象地域
                        Property_Type,  # 気象要素名
                        jmx_text,  # 気象要素の値
                    ]

                # └ Area 対象地域
                #     発表予報区を記述する。
                # └ Name 対象地域の名称
                #     発表予報区の名称を、"東京地方""大阪府"などと記述する。
                # └ Code 対象地域のコード
                #     発表予報区のコード番号を、"130010" "270000"などと記述する。

        return df


if __name__ == "__main__":
    config_path = "disaster.json"
    etl_vpfd60 = ETL_VPFD60(config_path, "regular", "VPFD60")
    etl_vpfd60.xml_to_csv()
