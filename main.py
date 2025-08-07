import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import asyncio
import time

#設定ファイル読込
with open('config.json','r',encoding='utf-8') as f:
    config = json.load(f)

SPREADSHEET_NAME = str(config['spreadsheet_name'])

# スプレッドシートの認証と取得
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('your-service-account.json', scope)
client = gspread.authorize(creds)

# スプレッドシートとシートの取得
spreadsheet = client.open(SPREADSHEET_NAME)
log_sheet = spreadsheet.worksheet("log")
manage_sheet = spreadsheet.worksheet("管理")
settings_sheet = spreadsheet.worksheet("設定")

# ===== コア処理：ユーザー同期 =====
async def sync_users():
    print("ユーザー同期を開始します...")

    # log_sheetのデータを辞書型で取得
    log_data = log_sheet.get_all_records()
    
    if not log_data:  # log_sheetが空の場合、処理を終了
        print("ログシートにデータがありません。処理を終了します。")
        return

    # manage_sheetのデータを取得
    try:
        manage_data = manage_sheet.get_all_records()
        print(f"{manage_data}")
        if manage_data:
            manage_ids = [str(row["ユーザーID"]) for row in manage_data]
        else:
            print("管理シートにデータがありませんが、処理を続行します。")
            manage_ids = []
    except Exception as e:
        print(f"管理シートのデータが空です: {e}")
        manage_ids = []

    for row in log_data:
        user_id = str(row["ユーザID"])
        username = row["ユーザー名"]
        
        if user_id not in manage_ids:
            # ユーザーが管理シートにいない場合は追加
            username = row["ユーザー名"]
            # スプレッドシートに記入:関数式をセルに設定
            new_row = [user_id, username, "", "", "", "", "", "", ""] # takat 追記
            manage_sheet.append_row(new_row)
            print(f"管理シートに追加: {user_id}")

            # 数式をセルに設定 (行番号は追加した行のインデックスを使用)
            row_index = len(manage_sheet.get_all_values())  # 新しく追加した行のインデックス
            print(f"書き込み行: {row_index}")

            cell_list = manage_sheet.range(f"C{row_index}:F{row_index}")

            manage_sheet.update_cell(row_index, 3, f'=COUNTIFS(log!$A:$A,A{row_index},log!$D:$D,"ログイン")')  # takat記載 ログイン数
            manage_sheet.update_cell(row_index, 4, f'=COUNTIFS(log!$A:$A,A{row_index},log!$D:$D,"募集作成")')  # takat記載 募集数
            manage_sheet.update_cell(row_index, 5, f'=SUMIFS(log!$G:$G,log!$A:$A,A{row_index},log!$D:$D,"VC")')  # takat記載 VC接続時間
            # takat記載 6 ボーナス
            manage_sheet.update_cell(row_index, 7, f"=INT(C{row_index}*'設定'!B2+D{row_index}*'設定'!B3+E{row_index}*('設定'!B4/3600)+F{row_index})")  # takat記載 獲得ポイント # 計算式
            manage_sheet.update_cell(row_index, 8, f"=SUMPRODUCT(IFERROR(VLOOKUP(FILTER(log!H:H, log!A:A=A{row_index}, log!D:D=\"消費\"), '交換品'!B:C, 2, FALSE), 0))") # takat記載 消費ポイント
            manage_sheet.update_cell(row_index, 9, f'=INT(G{row_index}-H{row_index})')  # takat記載 所持ポイント

            time.sleep(5)

            # 管理シートに追加後、manage_ids を再取得して更新
            manage_data = manage_sheet.get_all_records()  # 再度データを取得
            
            manage_ids = [str(row["ユーザーID"]) for row in manage_data]  # 最新のユーザIDリストを更新
        else:
            print(f"ユーザー {user_id} は管理シートに既に存在します。")

# メイン関数
def main():
    #asyncio.run(sync_users())             #これで実行すると、数式が文字列として書き込まれる
    loop = asyncio.get_event_loop()
    loop.run_until_complete(sync_users())  # 非同期関数を実行

if __name__ == "__main__":
    main()