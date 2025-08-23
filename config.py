# config.py

import os

# 各シートのスプレッドシートID は環境変数で上書き可能（なければデフォルト）
_MAIN_SHEET_ID = os.getenv('MAIN_SHEET_ID', '1ICVcByL2iEdzR4enIclntlTnHLG3Eze2e8-iH8wuT6Y')

SHEETS = {
    "入札書": {
        "spreadsheet_id": _MAIN_SHEET_ID,
        "range_prefix": "'入札書'!",
        "gid": os.getenv('BID_GID', "0")
    },
    "見積書": {
        "spreadsheet_id": _MAIN_SHEET_ID,
        "range_prefix": "'見積書'!",
        "gid": os.getenv('REF_GID', "885700754")
    }
}

# デフォルト設定（sheetパラメータ未指定時など）
DEFAULT_SHEET = {
    "spreadsheet_id": "",
    "range_prefix": "'シート1'!",
    "gid": "0"
}

# ── テンプレート（成分表）設定 ────────────────────────────────
# 入力された商品CDをこのスプシの「成分表」シートの A32 から書き込む
#FMT更新版
TEMPLATE_SPREADSHEET_ID = os.getenv('TEMPLATE_SPREADSHEET_ID', "1rzgCr2p1uIFiEhy_CxlGOYBPCLyhyXfy")
TEMPLATE_SHEET_ID       = int(os.getenv('TEMPLATE_SHEET_ID', '1557733602'))
START_ROW               = int(os.getenv('START_ROW', '32'))
