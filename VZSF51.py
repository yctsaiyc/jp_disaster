from disaster import ETL_jp_disaster
import pandas as pd


# 地上４８時間予想図
# 気象庁防災情報XMLフォーマット　技術資料: https://xml.kishou.go.jp/tec_material.html
# 電文毎の解説資料: https://xml.kishou.go.jp/jmaxml_20241031_Manual(pdf).zip


class ETL_VZSF51(ETL_jp_disaster):
    def parse_xxxPart(self, Property):
        pass
        # (4) 内容部詳細
        # Item 部は、天気図要素及び悪天情報の数だけ繰り返す。
        # 1要素が“低気圧”“高気圧”“熱帯低気圧”“低圧部”のとき
        #
        # タグ 解説
        # Item 要素詳細があるだけ Item 部を繰り返す。
        # └ Kind これらの天気図要素では、要素がもつ内容は1つで、CenterPart のみ。
        # └ Property
        # └ Type 天気図要素名“低気圧”“高気圧”“熱帯低気圧”“低圧部”のいずれかを記す。
        # └ CenterPart 中心情報 *CenterPart の詳細を参照。
        #
        # ア.CenterPart の詳細
        #
        # タグ 解説
        # CenterPart
        # └ jmx_eb:Coordinate 中心位置。擾乱の中心位置緯度経度を“+31.7+134.7/”等と記す。
        # └ @type “中心位置(度)” と示す。
        # └ @condition Type=“低圧部”の場合、“付近”と記す。それ以外の場合は@condition は省略する。
        # └ jmx_eb:Direction 移動方向。擾乱の移動方向を北を360度とした方位で“310”等と示す。ほとんど停滞しており、方向が定まらない時
        #
        # には、空タグとする。
        # └ @type “移動方向” と記す。
        # └ @unit “度(真方位)” と記す。
        # └ @condition 方向が定まらない時に“不定”と記す。それ以外の場合は@condition は省略する。
        # └ jmx_eb:Speed 移動速度。属性の unit が“km/h”の場合は、1時間に進む距離(km)を“25”等と示す。ほとんど停滞、あるいは、
        #
        # ゆっくり移動している場合は、空タグとする。
        #
        # └ @type “移動速度” と記す。
        # └ @unit “km/h” と記す。
        #
        # └ @description “30km/h”や“ほとんど停滞”、“ゆっくり”などと記す。地上24時間予想図などの予想図では移動速度が無いた
        #
        # め省略する。
        #
        # └ jmx_eb:Speed 移動速度。属性の unit が“ノット”の場合は、1時間に進む距離(海里)を“10”等と記す。
        # └ @type “移動速度” と記す。
        # └ @unit “ノット”と記す。
        # └ @description “15KT”や“ALMOST STNR”などと記す。地上24時間予想図などの予想図では移動情報が無いため省
        #
        # 略する。
        #
        # └ jmx_eb:Pressure 中心気圧。擾乱の中心気圧を“980”等と記す。
        # └ @type “中心気圧” と記す。
        # └ @unit “hPa” と記す。
        #
        # 2要素が“台風”のときは、要素が持つ内容が複数あり、Kind 以下のタグが並列に存在する
        # タグ 解説
        #
        # Item 要素詳細があるだけ Item 部を繰り返す。
        # └ Kind
        # └ Property
        # └ Type 天気図要素名“台風”と記す。
        # └ CenterPart 中心情報 *“低気圧”“高気圧”等と同じで CenterPart の詳細を参照。
        # └ Kind
        # └ Property
        # └ Type 要素名“風”と記す。
        # └ WindSpeedPart 最大風速 *WindSpeedPart の詳細を参照。
        # └ Kind
        # └ Property
        # └ Type 要素名“呼称”と記す。ハリケーン及び実況で台風となっていない場合の予想図では空タグ。
        # └ TyphoonNamePart 台風名称 *TyphoonNamePart の詳細を参照。ハリケーン及び実況で台風となっていない場合の予想図では空タグ。
        # └ Kind
        # └ Property
        # └ Type 要素名“階級”と記す。
        # └ ClassPart 台風階級 *ClassPart の詳細を参照。
        #
        # ア.WindSpeedPart の詳細
        #
        # タグ 解説
        #
        # WindSpeedPart
        # └ jmx_eb:WindSpeed 最大風速。属性の unit が“m/s”の場合は、風速(メートル)を“18”等と記す。
        # └ @type “最大風速” と記す。
        # └ @unit “m/s”と記す。
        # └ jmx_eb:WindSpeed 最大風速。属性の unit が“ノット”の場合は、風速(ノット)を“35”等と記す。
        # └ @type “最大風速” と記す。
        # └ @unit “ノット”と記す。
        # イ.TyphoonNamePart の詳細
        #
        # タグ 解説
        # TyphoonNamePart
        # └ Name 台風英名。“WASHI”等と記す。台風委員会が定める呼名“DAMREY”~“SAOLA”(CREX 表 B19209 に示すものと同じ。)、
        #
        # 域外から入る熱帯低気圧の呼称、または記述なし(空タグ)。
        #
        # └ NameKana 台風かな名。“ワシ”等と記す。台風委員会が定める呼名に対応したカタカナ表記“ダムレイ”~“サオラー”(CREX 表
        #
        # B19209 に示すものと同じ。)、域外から入る熱帯低気圧の呼称のカタカナ表記、または記述なし(空タグ)。
        #
        # └ Number 台風番号。西暦の下2桁と通年の台風番号の4桁で“1205”等と記す。
        # ウ. ClassPart の詳細
        #
        # タグ 解説
        # ClassPart
        # └ jmx_eb:TyphoonClass 台風階級。“台風(TS)”“台風(STS)”“台風(TY)”“ハリケーン(HR)”“発達した熱帯低気圧(Tropic
        #
        # al Storm)”のいずれかを記す。
        #
        # └ @type “熱帯擾乱種類” と記す。
        #
        # 3要素が“等圧線”のとき
        #
        # タグ 解説
        # Item 要素詳細があるだけ Item 部を繰り返す。
        # └ Kind この天気図要素では、要素がもつ内容は1つで、IsobarPart のみ。
        # └ Property
        # └ Type 天気図要素名“等圧線”と記す。
        # └ IsobarPart 等圧線の情報 *IsobarPart の詳細を参照。
        # ア.IsobarPart の詳細
        #
        # タグ 解説
        # IsobarPart
        # └ jmx_eb:Pressure 等圧線の示度。等圧線がもつ値を示す。
        # └ @type “気圧” と記す。
        # └ @unit “hPa” と記す。
        # └ jmx_eb:Line 等圧線が通る位置の情報。 “+41.56+163.76/+41.56+163.76/+41.57+163.76/”等として、等圧線を構成する連続点の緯
        #
        # 度経度を示す。
        # └ @type “位置(度)” と記す。
        #
        # 4要素が“寒冷前線”“温暖前線”“停滞前線”“閉塞前線”のとき
        #
        # タグ 解説
        # Item 要素詳細があるだけ Item 部を繰り返す。
        # └ Kind これらの天気図要素では、要素がもつ内容は1つで、CoordinatePart のみ。
        # └ Property
        # └ Type 天気図要素名“寒冷前線”“温暖前線”“停滞前線”“閉塞前線”のいずれかを記す。
        # └ CoordinatePart 前線の情報 *CoordinatePart の詳細を参照。
        # ア.CoordinatePart の詳細
        #
        # タグ 解説
        # CoordinatePart
        # └ jmx_eb:Line 前線が通る位置の情報。“+33.75+176.80/+33.80+176.91/+33.85+177.02/”等として、前線を構成する連続点の緯度経度
        #
        # を示す。
        #
        # └ @type “前線(度)” と記す。
        # └ @condition 前線の属性情報がある場合 “発生しつつある”“解消しつつある”と記し、属性情報が無い場合は省略する。
        #
        # 5要素が“悪天情報(強風)”のとき
        #
        # タグ 解説
        #
        # Item 予想される強風を表現する位置を示す。なお、この情報は、対象地点の1点のみに関する情報ではなく、周辺領域
        # を代表して表現するものである。海上悪天 24/48 時間予想図(FSAS24/48)(参考:図2)と同等の矢羽根を描画で
        # きるような位置情報を格納している。
        # 強風を表現する地点の数だけ Item 部を繰り返す。
        #
        # └ Kind
        # └ Name 悪天情報要素名“悪天情報(強風)”と記す。
        # └ Property
        # └ Type 悪天情報“風”と記す。
        # └ WindPart 対象海上地点の風向・風速の情報 *WindPart の詳細を参照。
        # └ Area
        # └ Name “強風域”と記す。
        # └ Coordinate 対象海上地点の位置を緯度経度で示す。*Coordinate の詳細を参照
        # ア.WindPart の詳細
        #
        # タグ 解説
        #
        # WindPart
        # └ jmx_eb:WindDegree 対象海上地点の風向を示す。
        # └ @type “風向” と記す。
        # └ @unit “度(真方位)” と記す。
        # └ jmx_eb:WindSpeed 対象海上地点の風速を示す。
        # └ @type “風速” と記す。
        # └ @unit “ノット” と記す。
        #
        # イ.Coordinate の詳細
        #
        # タグ 解説
        #
        # jmx_eb:Coordinate 対象海上地点の位置を緯度経度で示す。
        # └ @type “位置(度)” と記す。
        #
        # 6要素が“悪天情報(霧)”のとき
        #
        # タグ 解説
        #
        # Item 霧が予想される領域または海域の情報を示す。
        # 要素詳細があるだけ Item 部を繰り返す。
        #
        # └ Kind
        # └ Name 悪天情報要素名“悪天情報(霧)”と記す。
        # └ Area 対象領域または海域の情報を示す。 *Area の詳細を参照。
        # ア.Area の詳細(対象多角形領域を示す場合)
        #
        # タグ 解説
        # Area
        # └ Name “霧域” と記す。
        # └ jmx_eb:Polygon 対象領域を囲む多角形の頂点を緯度経度で示す。始点と終点は一致する。
        # └ @type “位置(度)” と記す。
        # イ.Area の詳細(対象海域を示す場合)
        #
        # タグ 解説
        # Area
        # └ @codeType “全般海上海域名”と記す。
        # └ Name 「日本海」「オホーツク海」等の海域名を記す。
        # └ Code 対象海域のコード番号を記す。コード番号は、全般海上警報の海域コード(code.AreaMarineA)を用いる。
        #
        # 7要素が“悪天情報(海氷)”または“悪天情報(船体着氷)”のとき
        #
        # タグ 解説
        # Item 海氷または船体着氷が予想される領域の情報を示す。
        # └ Kind
        # └ Name 悪天情報要素名“悪天情報(海氷)”または“悪天情報(船体着氷)”と記す。
        # └ Area 海氷域または船体着氷域の情報 *Area の詳細を参照。
        # ア.Area の詳細
        #
        # タグ 解説
        # Area
        # └ Name “海氷域”または“船体着氷域”と記す。
        # └ jmx_eb:Coordinate 北緯 60 度、東経 100 度を端点とした 0.5 度四方の各格子内において、海氷または船体着氷が予想される格子の北西端の
        #
        # 緯度経度値を列挙する。“+46.0+161.5/+46.0+162.0/+46.0+162.5/+46.0+163.0/”等と記す。
        #
        # └ @type “領域(度)”と記す。
        #
        # 天気図情報XMLの記述例
        # 3.1 Control Head 部の例
        # 3.2 Body 部の例
        # 3.2.1 Body 部詳細(高気圧の例)
        # 3.2.2 Body 部詳細(台風の例)
        # 3.2.3 Body 部詳細(前線の例)
        # 3.2.4 Body 部詳細(等圧線の例)
        # 3.2.5 Body 部詳細(悪天情報(強風)の例)
        # 3.2.6 Body 部詳細(悪天情報(霧)の例)
        # 3.2.7 Body 部詳細(悪天情報(海氷/船体着氷)の例)
        # ...

    def xml_to_df(self, xml_path, soup):
        df = pd.DataFrame()

        # 天気図情報 XML の解説

        # 天気図情報は、等圧線が通る位置や高低気圧の位置および海上における悪天予想情報等を緯度経度データで格納しており、
        # 実況天気図および予想天気図を拡大縮小等の編集可能なベクトルデータとして提供する事を目的とする。
        # 本書では、データ構造に従って「提供範囲」「全体構成」「各部の構成と内容」「実例」に分けて解説を行う。

        # 0. 提供範囲

        # 各天気図情報の提供範囲は以下の通りである

        # 情報名称 提供範囲

        # 地上実況図、地上24時間予想図、地上48時間予想図
        # 図1の「速報天気図の提供領域」

        # アジア太平洋地上実況図
        # 図1の「アジア太平洋地上天気図の提供領域」

        # アジア太平洋海上悪天24時間予想図、アジア太平洋海上悪天48時間予想図

        # 高低気圧の位置や等圧線・前線の位置等は、図1の「アジア太平洋地上天気図の提供領域」
        # 強風域や霧域などの悪天情報は、図1の「悪天情報の提供領域」

        # 1. 全体構成
        # 天気図情報XMLは、大きく分けて「管理部」「ヘッダ部」「ボディ部」の3つの構成からなる。
        # ...

        # 2. 各部の構成と内容
        # (1) Control 部の詳細
        # Control 部については、他の気象庁XMLとほぼ同様の構造となっている。

        # Control
        # └Title 情報名称
        # “地上実況図”“地上24時間予想図”“地上48時間予想図”“アジア太平洋地上実況図”“アジア太平洋海上悪天24時間予想図”
        # “アジア太平洋海上悪天48時間予想図”のいずれかを記す。
        Title = soup.find("Title").text

        # └DateTime 発表時刻。“2010-05-19T02:03:15Z”のように協定世界時で記す。

        # └Status 運用種別。“通常”“訓練”“試験”のいずれかを記す。
        # “訓練”“試験”は正規の情報として利用してはならないことを示す。

        # └EditorialOffice 編集官署名。“気象庁本庁”で固定。

        # └PublishingOffice 発表官署名。“気象庁”で固定。

        # (2) Head 部の詳細
        # Head 部についても、他の気象庁XMLとほぼ同様の構造となっている。

        # Title 標題。
        # “地上実況図”“地上24時間予想図”“地上48時間予想図”“アジア太平洋地上実況図”“アジア太平洋海上悪天24時間予想図”
        # “アジア太平洋海上悪天48時間予想図”のいずれかを記す。

        # ReportDateTime 発表時刻。“2012-01-10T05:00:00+09:00”のように日本標準時で記す。
        ReportDateTime = self.format_datetime(soup.find("ReportDateTime").text)

        # TargetDateTime 基点時刻。
        # “2012-01-10T03:00:00+09:00”のように日本標準時で記す。
        # Title が“地上実況図”“アジア太平洋地上実況図”であれば、観測日時刻、“地上24時間予想図”“地上48時間予想図”
        # “アジア太平洋海上悪天24時間予想図”“アジア太平洋海上悪天48時間予想図”であれば数値予報初期値時刻を示す。
        TargetDateTime = self.format_datetime(soup.find("TargetDateTime").text)

        # EventID 値は記述しない(空タグとする)。

        # InfoType 情報形態。
        # “発表”“訂正”“取消”のいずれかを記す。

        # Serial 値は記述しない(空タグとする)。

        # InfoKind スキーマの運用種別情報。
        # “天気図情報”で固定。

        # InfoKindVersion スキーマの運用種別情報のバージョン。
        # 本解説のバージョン番号は“1.1_1”。

        # Headline 見出し要素。
        # └ Text 値は記述しない(空タグとする)。

        # (3) Body 部の詳細
        # Body 部の Item タグ1つに対して1つの天気図要素を格納する。
        # Item 以下は、ここでは概要のみとし、詳細は(4)内容部詳細で示す。

        # MeteorologicalInfos 高低気圧や前線等の“天気図情報”と“悪天情報”は、異なる MeteorologicalInfos で示す。
        MeteorologicalInfos = soup.find("MeteorologicalInfos")

        # └@type “天気図情報”または“悪天情報”と記す。
        MeteorologicalInfos_type = MeteorologicalInfos.get("type")

        # └ MeteorologicalInfo
        MeteorologicalInfo = MeteorologicalInfos.find("MeteorologicalInfo")

        #     └ DateTime 天気図の対象となる時刻を、“2011-12-18T12:00:00+09:00”のように日本標準時で記す。
        DateTime = self.format_datetime(MeteorologicalInfo.find("DateTime").text)
        #         例えば type が“予想 24時間後”の場合は数値予報初期時刻から24時間後の時刻を示す。

        #         └@type “実況”“予想 24時間後”“予想 48時間後”のいずれかを記す。
        DateTime_type = MeteorologicalInfo.find("DateTime").get("type")

        # └ Item このタグ1つに対して1つの天気図要素または悪天情報要素を格納する。詳細は、(4)で示す。
        for Item in MeteorologicalInfo.find_all("Item"):

            # └ Kind
            Kind = Item.find("Kind")

            #     └ Name
            #         MeteolorogicalInfos@type=“天気図情報”の場合は、このタグを省略する。
            #         MeteorologicalInfos@type=“悪天情報”の場合は、悪天情報の種類“悪天情報(強風)”
            #             “悪天情報(霧)”“悪天情報(海氷)”“悪天情報(船体着氷)”のいずれかを記す。
            Kind_Name = Kind.find("Name")

            if Kind_Name:
                Kind_Name = Kind_Name.text

            #     └ Property
            Property = Kind.find("Property")

            #         MeteolorogicalInfos@type=“天気図情報”の場合、及び、
            #         MeteorologicalInfos@type=“悪天情報”で Item/Kind/Name=“悪天情報(強風)”の場合に出現する。
            #         MeteorologicalInfos@type=“悪天情報”で Item/Kind/Name= “悪天情報(霧)”
            #             “悪天情報(海氷)”“悪天情報(船体着氷)”の場合はこのタグを省略する。

            #         └ Type 天気図要素名等。
            #             “台風”“熱帯低気圧”“低気圧”“高気圧”“低圧部”“前線”等の天気図要素の種類もしくは悪天情報の“風”、
            #             または“風”“呼称”“階級”等の要素を修飾する種別のいずれかを記す。
            Property_type = Property.get("type")

            #             └ xxxPart 要素がもつ内容。Type により、“CenterPart”“WindSpeedPart”“TyphoonNamePart”
            #                 “ClassPart”“CoodinatePart”“IsobarPart”“WindPart”のいずれか1つを持つ。
            xxxPart = self.parse_xxxPart(Property)

            # └ Area MeteorologicalInfos@type=“悪天情報”の場合に出現し、悪天情報の対象領域を示す。
            #     Item/Kind/Name=“悪天情報(強風)”の場合、強風を表現する対象海上地点(Name と Coordinate)を示す。
            #     Item/Kind/Name=“悪天情報(霧)”の場合、対象多角形領域(Name と Polygon)または対象海域(Name と Code)を示す。
            #     Item/Kind/Name=“悪天情報(海氷)”または“悪天情報(船体着氷)”の場合、対象格子(0.5 度四方の領域)(Name と Coordinate)を示す。
            #     └@codeType Item/Kind/Name=“悪天情報(霧)”において、海域を対象とする場合に“全般海上海域名”と記す。

            #     対象領域を Codeで示さない場合は省略する。
            #     └ Name 悪天情報の対象領域名。
            if Item.find("Area"):
                if Item.find("Area").find("Name"):
                    Area_Name = Item.find("Area").find("Name").text

            #         Item/Kind/Name=“悪天情報(霧)”において、海域を対象とする場合は海域名(「日本海」「オホーツク海」等)を、
            #         多角形領域を対象とする場合は“霧域”と記す。また、Item/Kind/Name=“悪天情報(強風)”においては“強風域”、
            #         Item/Kind/Name=“悪天情報(海氷/船体着氷)”においては“海氷域/船体着氷域”と記す。

            #     └ Code 悪天情報の対象領域コード。
            #         Item/Kind/Name=“悪天情報(霧)”において、海域を対象とする場合は対応するコードを記す。
            #         全般海上海域名“AreaMarineA”コード表が対応する。

            #     └ jmx_eb:Polygon 悪天情報の対象多角形領域。
            #         Item/Kind/Name=“悪天情報(霧)”において対象多角形領域を示す場合に、領域を囲む多角形の頂点を緯度経度で示す。

            #     └ jmx_eb:Coordinate 悪天情報の対象緯度経度値を示す。Item/Kind/Name=“悪天情報(強風)”の場合は、強風を表現する対象海上地点を、
            #         Item/Kind/Name=“悪天情報(海氷)”または“悪天情報(船体着氷)”の場合は、対象格子の北西端の位置を、緯度経度で示す。

        return df


if __name__ == "__main__":
    config_path = "disaster.json"
    etl_vzsf51 = ETL_VZSF51(config_path, "regular", "VZSF51")
    etl_vzsf51.xml_to_csv()
