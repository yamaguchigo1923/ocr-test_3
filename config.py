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
# 入力された商品CDをこのスプシの「成分表」シートの A34 から書き込む
TEMPLATE_SPREADSHEET_ID = os.getenv('TEMPLATE_SPREADSHEET_ID', "1RcTzLKQuG8ZToa4lOU5pv4VVYVSbl62lpsCgd30Ac_U")
TEMPLATE_SHEET_ID       = int(os.getenv('TEMPLATE_SHEET_ID', '1643122230'))
START_ROW               = int(os.getenv('START_ROW', '34'))
