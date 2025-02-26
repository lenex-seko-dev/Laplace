from selenium import webdriver
import os
import time
import pymysql
import requests
from requests.auth import HTTPDigestAuth
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.select import Select
import warnings
import pandas as pd
import re
import sys
from datetime import datetime
from io import StringIO
from pandas.errors import SettingWithCopyWarning

warnings.simplefilter('ignore', FutureWarning)
warnings.simplefilter('ignore', DeprecationWarning)
warnings.simplefilter('ignore', SettingWithCopyWarning)

def get_ep(choice, targetPath, start, end, prefectures):
    today = datetime.now()
    # データベースに接続
    db = pymysql.connect(
        host='',
        port='',
        user='',
        passwd='',
        db='',
        charset=''
    )

    # カーソルを作成してクエリを実行
    with db.cursor() as cursor:
        sql = """
                SELECT
                    pt_solar.object_no,
                    pt_solar.id,
                    pt_solar.project_name,
                    pt_solar.object_address,
                    pt_solar.system_start_schedule_date,
                    pt_solar_measuring_info.pk_measuring_equip_id,
                    pt_solar_measuring_info.api_key1,
                    pt_solar_measuring_info.api_key2
                FROM pt_solar
                    INNER JOIN pt_solar_process_status ON pt_solar.id = pt_solar_process_status.pt_solar_id
                    INNER JOIN pt_solar_measuring_info ON pt_solar.id = pt_solar_measuring_info.pt_solar_id
                WHERE pt_solar.pk_solar_status_id = 10
                    AND pt_solar.del_flg = 0
                    AND pt_solar_process_status.del_flg = 0
                GROUP BY pt_solar.object_no
            """
        cursor.execute(sql)
        result = cursor.fetchall()
        db.close()

    # 取得したデータのデータフレーム作成
    ep_df = pd.DataFrame(result, columns=[
        "物件番号", 
        "案件データID", 
        "案件名", 
        "住所", 
        "連系予定日", 
        "機器ID", 
        "API1", 
        "API2"
        ])
    ep_df = ep_df[ep_df['機器ID'] == 20]  # 20がラプラス
    ep_df['住所'] = ep_df['住所'].apply(extract_prefecture)
    base_url = "https://ep.solar-mate.jp/admin/solar/basic/edit/"
    # "URL" 列を追加し、指定のURLと "案件データid" を結合した値を持つ列を作成する
    ep_df["EPシステムURL"] = base_url + ep_df["案件データID"].astype(str)
    ep_df['連系予定日'] = pd.to_datetime(ep_df['連系予定日'])

    if choice == 1:
        ep_df = ep_df[(ep_df['連系予定日'] >= start) & (ep_df['連系予定日'] <= end)]
        ep_df['連系予定日'] = ep_df['連系予定日'].dt.strftime('%Y-%m-%d')
        return ep_df
    elif choice == 2:
        # ep_df = ep_df[ep_df['連系予定日'] < today]
        # ep_df['連系予定日'] = ep_df['連系予定日'].dt.strftime('%Y-%m-%d')
        # 複数案件読み取り処理
        excel_df = pd.read_excel(targetPath + "【ラプラス一括監視情報取得】物件番号リスト.xlsx")
        df_list = []
        for i in excel_df.itertuples():
            num = i.物件番号
            df_list.append(num)
        ep_df = ep_df[ep_df['物件番号'].isin(df_list)]
        return ep_df
    elif choice == 3:
        ep_df = ep_df[ep_df['連系予定日'] < today]
        ep_df = ep_df[ep_df['住所'] == prefectures]
        ep_df['連系予定日'] = ep_df['連系予定日'].dt.strftime('%Y-%m-%d')
        return ep_df
    elif choice == 4:
        ep_df = ep_df[ep_df['連系予定日'] < today]
        # 複数案件読み取り処理
        excel_df = pd.read_excel(targetPath + "【ラプラス一括監視情報取得】物件番号リスト.xlsx")
        df_list = []
        for i in excel_df.itertuples():
            num = i.物件番号
            df_list.append(num)
        ep_df = ep_df[ep_df['物件番号'].isin(df_list)]
        ep_df['連系予定日'] = ep_df['連系予定日'].dt.strftime('%Y-%m-%d')
        return ep_df

def get_data_api(ep_df, flag):
    SMARTMETER_TYPE = 'approvedmeter'
    PCS_TYPE = 'pcs'
    today = datetime.now().strftime('%Y%m%d')
    sm_data = []
    pcs_data = []
    print(f' {len(ep_df)}件あります')
    print(' APIでデータを収集中・・・・')
    x = 1
    for i in ep_df.itertuples():
        property_num = i.物件番号
        apiKey1 = i.API1
        apiKey2 = i.API2
        print(f' {x}件目: {property_num}')
        response = requests.get(
            'https://services.energymntr.com/megasolar/' + apiKey1 + '/services/api/download/span.php?&unit=day&groupid=1&from=' + today + '&to=' + today + '&data=measuringdata&format=csv&type=' + SMARTMETER_TYPE,
            auth=HTTPDigestAuth(apiKey1, apiKey2),
        )
        if response.status_code == 200 :
            decoded_str = response.content      
            decoded_str = response.content.decode('shift_jis')        
            data = StringIO(decoded_str)
            # pandasでデータフレームに変換
            sm_df = pd.read_csv(data)  
            sm_df['物件番号'] = property_num
            sm_column = ['物件番号', '発電電力量(kWh)'] 
            sm_df = sm_df[sm_column]
            sm_data.append(sm_df)       

        response = requests.get(
            'https://services.energymntr.com/megasolar/' + apiKey1 + '/services/api/download/span.php?&unit=day&groupid=1&from=' + today + '&to=' + today + '&data=measuringdata&format=csv&type=' + PCS_TYPE,
            auth=HTTPDigestAuth(apiKey1, apiKey2),
        )
        if response.status_code == 200 :
            decoded_str = response.content      
            decoded_str = response.content.decode('shift_jis')        
            data = StringIO(decoded_str)
            # pandasでデータフレームに変換
            pcs_df = pd.read_csv(data)     
            pcs_df['物件番号'] = property_num
            pcs_column = ['物件番号', 'PCS1 故障', 'PCS1 系統異常', 'PCS2 故障', 'PCS2 系統異常'] 
            for col in pcs_column:
                if col not in pcs_df.columns:
                    pcs_df[col] = 'なし'
            pcs_df = pcs_df[pcs_column]
            pcs_data.append(pcs_df)  
        x += 1    
    sm_concat_df = pd.concat(sm_data)
    pcs_concat_df = pd.concat(pcs_data)
    df = pd.merge(sm_concat_df, pcs_concat_df, on='物件番号', how='outer')
    df.fillna('空欄', inplace=True)
    chat_data = []
    _chat_data = []
    exe_list = [0, 'なし']
    exe_list2 = [0, '空欄']
    sejo = '正常'
    ijo = '異常'
    if flag == False:
        df1 = df[df['発電電力量(kWh)'].isin(exe_list2)]
        df2 = df[df['PCS1 故障'] != 0]
        df3 = df[df['PCS1 系統異常'] != 0]
        df4 = df[~df['PCS2 故障'].isin(exe_list)]
        df5 = df[~df['PCS2 系統異常'].isin(exe_list)]
        chat_data.append(df1)
        chat_data.append(df2)
        chat_data.append(df3)
        chat_data.append(df4)
        chat_data.append(df5)
        df_columns = [
            '物件番号',
            '案件名',
            '住所',
            '連系予定日',
            '発電電力量(kWh)',
            'PCS1 故障', 
            'PCS1 系統異常', 
            'PCS2 故障', 
            'PCS2 系統異常',
            'EPシステムURL'
        ]
    elif flag == True:
        df_0 = df[~df['発電電力量(kWh)'].isin(exe_list2)]  # 発電量正常
        df_0 = df_0[df_0['PCS1 故障'] == 0]
        df_0 = df_0[df_0['PCS1 系統異常'] == 0]
        df_0 = df_0[df_0['PCS2 故障'].isin(exe_list)]
        df_0 = df_0[df_0['PCS2 系統異常'].isin(exe_list)]
        df_0['発電状態'] = '正常'

        df_1 = df[df['発電電力量(kWh)'].isin(exe_list2)]  # 発電量0か空欄
        df_2 = df[df['PCS1 故障'] != 0]
        df_3 = df[df['PCS1 系統異常'] != 0]
        df_4 = df[~df['PCS2 故障'].isin(exe_list)]
        df_5 = df[~df['PCS2 系統異常'].isin(exe_list)]
        df_1 = pd.concat([df_1, df_2], ignore_index=True)
        df_1 = pd.concat([df_1, df_3], ignore_index=True)
        df_1 = pd.concat([df_1, df_4], ignore_index=True)
        df_1 = pd.concat([df_1, df_5], ignore_index=True)
        df_1['発電状態'] = '異常の可能性あり'
        _chat_data.append(df_0)
        _chat_data.append(df_1)
        df_columns = [
            '物件番号',
            '発電状態',
            '案件名',
            '住所',
            '連系予定日',
            '発電電力量(kWh)',
            'PCS1 故障', 
            'PCS1 系統異常', 
            'PCS2 故障', 
            'PCS2 系統異常',
            'EPシステムURL'
        ]
    if len(_chat_data) > 0:
        _df = pd.concat(_chat_data)
        if flag == False:
            _df['発電状態'] = '異常の可能性あり'
        chat_data.append(_df)
    df = pd.concat(chat_data)
    df = df.drop_duplicates(subset='物件番号', keep='first')
    df_num_list = []
    for i in df.itertuples():
        num = i.物件番号
        df_num_list.append(num)
    ep_df_2 = ep_df[ep_df['物件番号'].isin(df_num_list)]
    df = pd.merge(ep_df_2, df, on='物件番号', how='outer')
    df = df[df_columns]
    rename_dict = {
        '発電電力量(kWh)': '発電電力量',
        'PCS1 故障': 'PCS1故障',
        'PCS1 系統異常': 'PCS1系統異常',
        'PCS2 故障': 'PCS2故障',
        'PCS2 系統異常': 'PCS2系統異常'
    }

    # データフレームの列をリネーム
    df = df.rename(columns=rename_dict)    
    return df

def chrome_driver(ep_df):
    BrouteData = "デフォルト"
    chrome_options = Options()

    prefs = {'download.prompt_for_download':False,
                'download.directory_upgrade':True,
                'plugins.always_open_pdf_externally':True}
    chrome_options.add_experimental_option('prefs', prefs)
    chrome_options.add_argument('--disable-logging')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
    chrome_options.add_argument('--log-level=3')
    driver = webdriver.Chrome(options=chrome_options)
    # ラプラスウェブサイトログイン
    laplaceWeb = "https://laplaceid.energymntr.com/"
    driver.get(laplaceWeb)
    driver.find_element('xpath', "/html/body/div/div/div/div[2]/div/form/div/div[1]/input").send_keys("")
    driver.find_element('xpath', "/html/body/div/div/div/div[2]/div/form/div/div[2]/input").send_keys("")
    driver.find_element('xpath', "/html/body/div/div/div/div[2]/div/form/div/div[4]/button").click()

    # 発電所一覧へ移動
    WebDriverWait(driver, 30).until(EC.visibility_of_element_located((By. XPATH, '//*[@id="root"]/div/div/div[1]/div[1]/header/div/div/div[2]/div/div/a[2]')))
    driver.find_element('xpath', "/html/body/div/div/div/div[1]/div[1]/header/div/div/div[2]/div/div/a[2]").click()

    WebDriverWait(driver, 30).until(EC.visibility_of_element_located((By. XPATH, '//*[@id="root"]/div/div/div[1]/div[1]/div/div/div[1]/div[4]/div/a')))
    driver.find_element('xpath', "/html/body/div/div/div/div[1]/div[1]/div/div/div[1]/div[4]/div/a").click()

    df_list = []
    count = 0
    print(f' {len(ep_df)}件あります')
    for i in ep_df.itertuples():
        df_data = {}
        property_num = i.物件番号
        property_num = property_num.strip()
        task = i.案件名
        schedule = i.連系予定日
        ep_url = i.EPシステムURL
        # Target 商品番号の検索と移動
        WebDriverWait(driver, 30).until(EC.visibility_of_element_located((By. XPATH, '//*[@id="root"]/div/div/div[1]/div[1]/div/div/div[1]/div[3]/button')))

        select_path =  driver.find_element("xpath", '//*[@id="root"]/div/div/div[1]/div[1]/div/div/div[1]/div[2]/div/select')
        select_company = Select(select_path)
        select_company.select_by_visible_text("レネックスみらい合同会社")

        # 1回目以降はclear処理必要
        if count > 0 :
            driver.find_element('xpath', '//*[@id="root"]/div/div/div[1]/div[1]/div/div/div[1]/div[4]/input').clear()

        driver.find_element('xpath', '//*[@id="root"]/div/div/div[1]/div[1]/div/div/div[1]/div[4]/input').send_keys(property_num)
        driver.find_element('xpath', '//*[@id="root"]/div/div/div[1]/div[1]/div/div/div[1]/div[4]/button').click()

        time.sleep(7)

        try :
            WebDriverWait(driver, 30).until(EC.visibility_of_element_located((By. XPATH, '//*[@id="root"]/div/div/div[1]/div[1]/div/div/div[1]/table/tbody/tr/td[1]/a')))
        except Exception :
            continue

        # Bルート案件か否かを判断するためのデータパターン(selected Html Element)を取得
        select = Select(driver.find_element('xpath', '//*[@id="root"]/div/div/div[1]/div[1]/div/div/div[1]/table/tbody/tr/td[4]/div/select'))
        selectedDataPattern = select.first_selected_option.text

        # その案件に移動
        driver.find_element('xpath', "/html/body/div/div/div/div[1]/div[1]/div/div/div[1]/table/tbody/tr/td[1]/a").click()
        time.sleep(2)
        if selectedDataPattern == BrouteData :
            # Bルートの有無によって画面要素が変わるのでtry処理
            try :
                laplace_url = driver.current_url
                # 電力メーター（計量値） - 日付部分
                date = driver.find_element('xpath', '//*[@id="energyMeterWidgetArea"]/div[2]').text
                date_str = date.split(' ')[0]
                date_obj = datetime.strptime(date_str, '%m/%d')
                today = datetime.now()
                today = today.strftime('%m/%d')
                today = datetime.strptime(today, '%m/%d')
                if date_obj < today:
                    print(f' {property_num}: 日付が過去です → {date_str}')
                    df_data['物件番号'] = [property_num]
                    df_data['案件名'] = [task]
                    df_data['連系予定日'] = [schedule]
                    df_data['EPシステムURL'] = [ep_url]
                    df_data['ラプラスURL'] = [laplace_url]
                    df_data['bルート登録'] = ['登録なし']
                else :
                    print(f' {property_num}: 正常')
            except Exception as e :
                print(f"日付取得なし・日付がないので対象  {e}")
                df_data['物件番号'] = [property_num]
                df_data['案件名'] = [task]
                df_data['連系予定日'] = [schedule]
                df_data['EPシステムURL'] = [ep_url]
                df_data['ラプラスURL'] = [laplace_url]
                df_data['bルート登録'] = ['登録なし']
        else:
            print(f' {property_num}: 項目がbルートありじゃない(デフォルトではない)')
        add_df = pd.DataFrame(df_data)
        df_list.append(add_df)
        count += 1
        driver.back()
        time.sleep(5)
    df = pd.concat(df_list)
    return df

# 住所列から都道府県の部分だけを抽出する
def extract_prefecture(address):
    # 都道府県名を取り出す正規表現パターン
    pattern = '東京都|北海道|(京都|大阪)府|.{2,3}県'
    match = re.match(pattern, address)
    if match:
        return match.group()
    else:
        return ''

def main():
    print(" \n【ラプラス異常データ取得プログラム】")
    print(' 1.連系予定日を基準に期間を決める')
    print(' 2.【ラプラス一括監視情報取得】物件番号リスト.xlsxにある案件を確認する')
    print(' 3.都道府県ごとに確認する')
    print(' 4.Bルート登録有無を確認する')
    print(' 0.プログラムを終了する')
    choice = int(input(' 数字を入力して下さい(半角): '))
    downloadPath = 'C:\\python\\download\\'
    targetPath = 'C:\\python\\target\\'
    flag = False  # 2の正常確認か異常確認かを判断する用 falseが異常
    prefectures = ''
    start = ''
    end = ''
    today_str = datetime.now().strftime('%Y%m%d%H%M%S')
    # create local directory
    if not os.path.exists(downloadPath) :
        os.makedirs(downloadPath)
    if not os.path.exists(targetPath) :
        os.makedirs(targetPath)

    if choice == 1:
        start = input(' いつからの連系予定日ですか(半角)(例: 2024/01/01): ')
        start = datetime.strptime(start, '%Y/%m/%d')
        end = input(' いつまでの連系予定日ですか(半角)(例: 2024/01/01): ')
        end = datetime.strptime(end, '%Y/%m/%d')
        ep_df = get_ep(choice, targetPath, start, end, prefectures)
        start = start.strftime('%Y%m%d')
        end = end.strftime('%Y%m%d')
        df = get_data_api(ep_df, flag)
        if len(df) > 0:
            path = downloadPath + f'【連系予定日】ラプラス異常データ_{start}-{end}_{today_str}.xlsx'
            df.to_excel(path, index=False)
        else:
            print(' 対象案件なしのため、ファイル作成なし')
            main()

    elif choice == 2:
        print(" ※ 注意事項1: 実行前に必ず「C:\\python\\target」 ディレクトリに更新対象物件番号リストを入れること。")
        print(" ※ 注意事項2: 更新対象の物件番号リストファイル名はかならず「【ラプラス一括監視情報取得】物件番号リスト.xlsx」であること。")
        print(" ※ 注意事項3: 更新対象のExcelファイルに暗号化がかかっているとエラーになるので注意すること。")
        input(' 準備ができたらEnterを押す')
        print(' \n 1.正常確認')
        print(' 2.異常確認')
        flag_choice = int(input(' 数字を入力して下さい(半角): '))
        if flag_choice == 1:
            flag = True
            path = downloadPath + f'【リスト】ラプラス正常確認データ_{today_str}.xlsx'
        elif flag_choice == 2:
            path = downloadPath + f'【リスト】ラプラス異常データ_{today_str}.xlsx'
        ep_df = get_ep(choice, targetPath, start, end, prefectures)
        df = get_data_api(ep_df, flag)
        if len(df) > 0:
            df.to_excel(path, index=False)
        else:
            print(' 対象案件なしのため、ファイル作成なし')
            main()

    elif choice == 3:
        prefectures = input(' 都道府県名を入力してください(例: 北海道、東京都、千葉県、大阪府): ')
        ep_df = get_ep(choice, targetPath, start, end, prefectures)
        df = get_data_api(ep_df, flag)
        if len(df) > 0:
            path = downloadPath + f'【{prefectures}】ラプラス異常データ_{today_str}.xlsx'
            df.to_excel(path, index=False)
        else:
            print(' 対象案件なしのため、ファイル作成なし')
            main()

    elif choice == 4:
        print(" ※ 注意事項1: 実行前に必ず「C:\\python\\target」 ディレクトリに更新対象物件番号リストを入れること。")
        print(" ※ 注意事項2: 更新対象の物件番号リストファイル名はかならず「【ラプラス一括監視情報取得】物件番号リスト.xlsx」であること。")
        print(" ※ 注意事項3: 更新対象のExcelファイルに暗号化がかかっているとエラーになるので注意すること。")
        input(' 準備ができたらEnterを押す')
        ep_df = get_ep(choice, targetPath, start, end, prefectures)
        df = chrome_driver(ep_df)
        if len(df) > 0:
            path = downloadPath + f'【Bルート登録なし】ラプラスデータ_{today_str}.xlsx'
            df.to_excel(path, index=False)
        else:
            print(' 対象案件なしのため、ファイル作成なし')
            main()

    else:
        print(' プログラム終了します')
        sys.exit()
    print(f'作成完了\n作成したファイルは{downloadPath}にあります')
    main()

if __name__ == '__main__':
     main()

