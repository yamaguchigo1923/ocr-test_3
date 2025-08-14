# config.py

# 各シートに対応するスプレッドシートID、A1範囲プレフィックス、シートID(gid)をまとめる
SHEETS = {
    "入札書": {
        "spreadsheet_id": "1ICVcByL2iEdzR4enIclntlTnHLG3Eze2e8-iH8wuT6Y",
        "range_prefix": "'入札書'!",
        "gid": "0"  # 入札書シートの gid
    },
    "見積書": {
        "spreadsheet_id": "1ICVcByL2iEdzR4enIclntlTnHLG3Eze2e8-iH8wuT6Y",
        "range_prefix": "'見積書'!",
        "gid": "885700754"  # 見積書シートの gid
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
TEMPLATE_SPREADSHEET_ID = "1RcTzLKQuG8ZToa4lOU5pv4VVYVSbl62lpsCgd30Ac_U"
TEMPLATE_SHEET_ID       = 1643122230
START_ROW               = 34
