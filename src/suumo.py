import re
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup
from pandas import Series


def fetch_suumo_real_estate():
    # Suumo検索条件： 東京都 | 茅場町駅まで30～45分以内、乗換回数１回以内 | 5000万円未満 | 専有：60m2以上 | 2K/DK/LDK,3K/DK/LDK | 駅徒歩：10分以内 | 25年以内 | 所有権 | 2階以上
    url = 'https://suumo.jp/jj/bukken/ichiran/JJ012FC001/?ar=030&bs=011&cn=25&ekInputCd=09680&ekInputNk=1&ekInputNm=%E8%8C%85%E5%A0%B4%E7%94%BA&ekInputTj=45&et=10&jc=042&kb=1&kr=A&kt=5000&mb=60&md=2&md=3&mt=9999999&ta=13&pc=30&po=1&pj=2'

    # データ取得
    result = requests.get(url)
    c = result.content

    # HTMLを元に、オブジェクトを作る
    soup = BeautifulSoup(c, "html.parser")

    # 物件リストの部分を切り出し
    summary = soup.find("ul", {'id': 'js-bukkenList'})

    # ページ数を取得
    body = soup.find("body")
    pages = body.find_all("div", {'class': 'pagination pagination_set-nav'})
    pages_text = str(pages)
    pages_split = pages_text.split('</a></li>\n</ol>')
    pages_split0 = pages_split[0]
    pages_split1 = pages_split0[-3:]
    pages_split2 = pages_split1.replace('>', '')
    if '"' in pages_split2:
        pages_split2 = pages_split2.replace('"', '')
    pages_split3 = int(pages_split2)

    # URLを入れるリスト
    urls = []

    # 1ページ目を格納
    urls.append(url)

    # 2ページ目から最後のページまでを格納
    for i in range(pages_split3 - 1):
        pg = str(i + 2)
        url_page = url + '&pn=' + pg
        urls.append(url_page)

    name = []  # マンション名
    price = []  # 販売価格
    address = []  # 住所
    location = []  # 立地1つ目（最寄駅/徒歩~分）
    exclusive_area = []  # 専有面積
    balcony = []  # バルコニー
    floor_plan = []  # 間取り
    age = []  # 築年数
    link = []  # 物件URL
    count = 0

    # 各ページで以下の動作をループ
    for url in urls:
        count += 1
        print(f'Now Count: {count}, urls len:{len(urls)}')
        # 物件リストを切り出し
        result = requests.get(url)
        c = result.content
        soup = BeautifulSoup(c, "html.parser")
        summary = soup.find("ul", {'id': 'js-bukkenList'})

        # 中古マンションの販売価格、所在地、沿線・駅、専有面積、バルコニー、間取り、築年月が入っているエリアを抽出
        cassetteitems = summary.find_all("div", {'class': 'cassette-body'})

        # 各cassetteitemsに対し、以下の動作をループ
        for i in range(len(cassetteitems)):
            # count += 1
            # if count == 2:
            #     break

            # マンション名取得
            subtitle = cassetteitems[i].find_all("h2", {'class': 'listtitleunit-title'})
            subtitle = str(subtitle)
            # 正規表現チェッカー: https://pythex.org/
            subtitle_rep = re.sub('\[<h2 class="listtitleunit-title">.*/">', '', subtitle)
            subtitle_rep2 = subtitle_rep.replace('</a></h2>]', '')
            name.append(subtitle_rep2)

            # マンション名からリンクを取得
            href = cassetteitems[i].find_all("a", limit=1)
            for a in href:
                link_url = f'https://suumo.jp{a.get("href")}'
                link.append(link_url)

            # 階、賃料、管理費、敷/礼/保証/敷引,償却、間取り、専有面積が入っているtableを全て抜き出し
            detail = cassetteitems[i].find_all("div",
                                               {'class': 'detail_contents-body cassette_detail cassette_detail--space'})

            # 各建物（table）に対して、売りに出ている部屋（row）を取得
            rows = []
            for i in range(len(detail)):
                rows.append(detail[i].find_all('dl', {'class': 'tableinnerbox'}))

            # 各部屋に対して、tableに入っているtext情報を取得し、dataリストに格納
            data = []
            for row in rows:
                for dl in row:
                    cols = dl.find_all('dt')
                    for dt in cols:
                        text = dt.find(text=True)
                        data.append(text)
                    cols = dl.find_all('dd')
                    for dd in cols:
                        text = dd.find(text=True)
                        data.append(text)

            # dataリストから、階、賃料、管理費、敷/礼/保証/敷引,償却、間取り、専有面積を順番に取り出す
            index = 0
            for item in data:
                if '販売価格' in item:
                    price.append(data[index + 1])
                    address.append(data[index + 3])
                    location.append(data[index + 5])
                    exclusive_area.append(data[index + 7])
                    balcony.append(data[index + 9])
                    floor_plan.append(data[index + 11])
                    age.append(data[index + 13])
                index += 1

        # プログラムを10秒間停止する（スクレイピングマナー）
        time.sleep(10)

    # 各リストをシリーズ化
    name = Series(name)
    price = Series(price)
    address = Series(address)
    location = Series(location)
    exclusive_area = Series(exclusive_area)
    balcony = Series(balcony)
    floor_plan = Series(floor_plan)
    age = Series(age)
    link = Series(link)

    # 各シリーズをデータフレーム化
    suumo_df = pd.concat(
        [name, price, address, location, exclusive_area, balcony, floor_plan, age, link],
        axis=1)

    # カラム名
    suumo_df.columns = ['マンション名', '販売価格', '住所', '最寄り駅', '専有面積', 'バルコニー', '間取り', '築年数', 'URL']

    # csvファイルとして保存
    suumo_df.to_csv('suumo_estate.csv', encoding='utf-8')
