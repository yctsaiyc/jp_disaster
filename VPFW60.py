from disaster import ETL_jp_disaster
from bs4 import BeautifulSoup
import pandas as pd


class ETL_VPFW60(ETL_jp_disaster):
    def xml_to_df(self, xml_path, soup):
        df = pd.DataFrame(columns=self.columns)

        # 2 各部の構成と内容

        # (1) 管理部
        # 1 管理部(Control)の構成と内容

        # Control
        # └Title 情報名称 電文の種別を示すための情報名称を示す。“警報級の可能性(明後日以降)”で固定。
        # └DateTime 発表時刻 発表時刻。未来時刻にはならない。
        #     “2008-06-26T01:51:00Z”のように協定世界時で記述する。
        # └Status 運用種別 本情報の位置づけ。“通常”“訓練”“試験”のいずれかを記載。
        #     “訓練”“試験”は正規の情報として利用してはならないことを示す。
        # └EditorialOffice 編集官署名
        #     実際に発表作業を行った「編集官署名」を示す。“気象庁本庁”“大阪管区気象台”のように記述する。
        # └PublishingOffice 発表官署名
        #     本情報を業務的に発表した「発表官署名」を示す。“気象庁”“大阪管区気象台”のように記述する。

        # (2) ヘッダ部
        # 1 ヘッダ部(Head)の構成と内容

        # Head
        # └Title 標題 情報を示す標題。具体的な内容が判別できる名称であり、可視化を目的として利用する。
        #   “○○警報級の可能性(明後日以降)”(○○は府県予報区名)と記述する。

        # └ReportDateTime 発表時刻 本情報の公式な発表時刻を示す。“2008-06-26T11:00:00+09:00”のように日本標準時で記述する。
        ReportDateTime = self.format_datetime(soup.find("ReportDateTime").text)

        # └TargetDateTime 基点時刻 本情報の対象となる時刻・時間帯の基点時刻を示す。
        #     “2008-06-28T00:00:00+09:00”のように日本標準時で記述する。
        TargetDateTime = self.format_datetime(soup.find("TargetDateTime").text)

        # └TargetDuration 基点時刻からの取りうる時間
        #     情報の対象が時間幅を持つ場合、TargetDateTime を基点とした時間の幅を示す。
        #     “P4D”で、「基点時刻」から 4 日先までの予報であることを示す。
        # └EventID 識別情報 警報級の可能性(明後日以降)では要素内に何も記述しない。
        # └InfoType 情報形態 本情報の形態を示す。“発表”“訂正”“遅延”のいずれかを記述する。
        # └Serial 情報番号 警報級の可能性(明後日以降)では要素内に何も記述しない。
        # └InfoKind スキーマの運用種別情報
        #     同一スキーマ上における情報分類に応じた運用を示す種別情報である。
        #     “警報級の可能性(明後日以降)”と記述する。
        # └InfoKindVersion スキーマの運用種別情報のバージョン
        #     スキーマの運用種別情報におけるバージョン番号を示す。本解説のバージョン番号は“1.2_0”。
        # └Headline 見出し要素 防災気象情報事項となる見出し要素を示す。警報級の可能性(明後日以降)では何も記述しない。
        # └ Text 見出し文 警報級の可能性(明後日以降)では要素内に何も記述しない。

        # (3) 内容部
        # 1 内容部(Body)の構成と内容

        # Body
        # └MeteorologicalInfos 予報の項目
        #  警報級の気象現象が発生する可能性を記述する。

        # └ TimeSeriesInfo 時系列情報 MeteorologicalInfos の予報を時系列情報として記述する。

        # 2内容部の個別要素の詳細
        # ※1 「警報級の可能性の予想」の詳細
        # タグ 内容 解説
        # TimeSeriesInfo 時系列情報
        # └ TimeDefines 時系列の時刻定
        # 義セット
        #
        # 予報の対象期間を示すとともに、対応する要素の timeId を記述する。
        #
        # └ TimeDefine 個々の時刻定義 同一 TimeSeriesInfo 内にある要素の ID(refID)に対応する ID(timeId)を記述する。
        # └ DateTime 基点時刻 予報対象日について記述する。予報対象日の開始時刻を示す。“2008-06-28T00:00:00+09:00”のよう
        #
        # に日本標準時で記述する。
        #
        # └ Duration 対象期間 予報の対象期間を示す。値「P1D」で、1日を対象とした予報であることを示す。
        # └ Item 警報級の可能性の予報と、予報区を記述する。府県予報区に含まれる発表予報区の数だけ繰り返す。
        #
        # ※1-1「警報級の可能性」の詳細を参照。
        #
        # ※1-1 「警報級の可能性」の詳細
        #
        # タグ 内容 解説
        #
        # Item 予報の内容
        # └ Kind 個々の予報の内容 予報を記述する。
        # └ Property 予報要素 予報要素を記述する。
        # └ Type 気象要素名 気象要素名を記述する。Type の値は“雨の警報級の可能性”。
        # └ PossibilityRankOfWarningPart 警報級の可能性 「雨の警報級の可能性」の階級値(※1-2参照)を記述する。
        #
        # ※1-1-1「雨の警報級の可能性」の詳細を参照。属性 refID は、予報対象
        # 日の参照番号を記述する。TimeDefines で定義した timeId に対応する。
        #
        # └ Kind 個々の予報の内容 予報を記述する。
        # └ Property 予報要素 予報要素を記述する。
        # └ Type 気象要素名 気象要素名を記述する。Type の値は“雪の警報級の可能性”。
        # └ PossibilityRankOfWarningPart 警報級の可能性 「雪の警報級の可能性」の階級値(※1-2参照)を記述する。
        #
        # ※1-1-2「雪の警報級の可能性」の詳細を参照。属性 refID は、予報対象
        # 日の参照番号を記述する。TimeDefines で定義した timeId に対応する。
        #
        # └ Kind 個々の予報の内容 予報を記述する。
        #
        # └ Property 予報要素 予報要素を記述する。
        # └ Type 気象要素名 気象要素名を記述する。Type の値は“風(風雪)の警報級の可能性”。
        # └ PossibilityRankOfWarningPart 警報級の可能性 「風(風雪)の警報級の可能性」の階級値(※1-2参照)を記述する。
        # ※1-1-3「風(風雪)の警報級の可能性」の詳細を参照。属性 refID は、
        # 予報対象日の参照番号を記述する。TimeDefines で定義した timeId に対応す
        # る。
        #
        # └ Kind 個々の予報の内容 予報を記述する。
        # └ Property 予報要素 予報要素を記述する。
        # └ Type 気象要素名 気象要素名を記述する。Type の値は“波の警報級の可能性”。
        # └ PossibilityRankOfWarningPart 警報級の可能性 「波の警報級の可能性」の階級値(※1-2参照)を記述する。予報対象地
        # 域で波浪警報等の運用を行なっていない場合は、Kind 以下を省略する。
        # ※1-1-4「波の警報級の可能性」の詳細を参照。属性 refID は、予報対象
        # 日の参照番号を記述する。TimeDefines で定義した timeId に対応する。
        #
        # └ Kind 個々の予報の内容 予報を記述する。
        # └ Property 予報要素 予報要素を記述する。
        # └ Type 気象要素名 気象要素名を記述する。Type の値は“潮位の警報級の可能性”。
        # └ PossibilityRankOfWarningPart 警報級の可能性 「潮位の警報級の可能性」の階級値(※1-2参照)を記述する。予報対象
        # 地域で高潮警報等の運用を行なっていない場合は、Kind 以下を省略する。
        # ※1-1-5「潮位の警報級の可能性」の詳細を参照。属性 refID は、予報対
        # 象日の参照番号を記述する。TimeDefines で定義した timeId に対応する。
        #
        # └ Area 対象地域 予報対象地域を記述する。
        # └ Name 対象地域の名称 予報対象地域(予報区)の名称を記述する。
        # └ Code 対象地域のコード 予報対象地域(予報区)のコードを記述する。

        return df


if __name__ == "__main__":
    config_path = "disaster.json"
    etl_vpfw60 = ETL_VPFW60(config_path, "regular", "VPFW60")
    etl_vpfw60.xml_to_csv()
