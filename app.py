# app.py  （ocr-test_3 用・“作業用コピーで一括計算”安定化＆高速化・B34~K 取得に修正）

try:
    from flask import Flask, request, send_from_directory, Response
    from googleapiclient.errors import HttpError
    from azure.core.credentials import AzureKeyCredential
    from azure.ai.documentintelligence import DocumentIntelligenceClient
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from google.cloud import storage
    import pandas as pd
    import openpyxl  # noqa: F401
    from datetime import datetime, timezone, timedelta
    import os, traceback, io, time, json, uuid, random, base64
    import config
    print("[BOOT] libs ok")
except ImportError as e:
    print(f"[BOOT][IMPORT-ERROR] {e}")
    raise e

app = Flask(__name__)
print("[BOOT] Flask initialized")

# アプリバージョン（デプロイ反映確認用に手動更新）
APP_VERSION = "2025-08-14a"
print(f"[BOOT] APP_VERSION={APP_VERSION}")

# --- ENV ---
AZURE_ENDPOINT  = os.environ.get("AZURE_ENDPOINT", "")
AZURE_KEY       = os.environ.get("AZURE_KEY", "")
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "")
DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID", "")

# チャンク/ポーリングの調整（必要に応じて Cloud Run の --set-env-vars で上書き）
CHUNK_SIZE     = int(os.environ.get('BATCH_CHUNK_SIZE', '40'))     # 一度に投げる商品CD件数
POLL_MAX_WAIT  = float(os.environ.get('POLL_MAX_WAIT', '60'))      # 1チャンクの最大待ち秒（ハード上限）
POLL_MIN_READY = float(os.environ.get('POLL_MIN_READY', '0.95'))   # “準備完了行”率で判定（95%既定）
READY_COL_IDX  = int(os.environ.get('READY_COL_IDX', '0'))         # 取得範囲(B:K)に対する準備判定列の相対index（0=B列）

## --- Credentials & Google API Clients Initialization (order fixed) ---
SERVICE_ACCOUNT_FILE = 'service-account.json'

# 1) Reconstruct service-account.json from Base64 env (if provided)
_sa_b64 = os.environ.get("SERVICE_ACCOUNT_JSON_B64")
if _sa_b64 and not os.path.exists(SERVICE_ACCOUNT_FILE):
    try:
        with open(SERVICE_ACCOUNT_FILE, 'wb') as f:
            f.write(base64.b64decode(_sa_b64))
        print('[BOOT] service-account.json written from SERVICE_ACCOUNT_JSON_B64')
    except Exception as _e:
        print(f'[BOOT][WARN] failed to write service-account.json: {_e}')

# 2) Load credentials
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
creds = None
if os.path.exists(SERVICE_ACCOUNT_FILE):
    try:
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES
        )
        print(f"[BOOT] credentials loaded project={getattr(creds, 'project_id', 'UNKNOWN')}")
    except Exception as _e:
        print(f"[BOOT][FATAL] failed to load credentials: {_e}")
else:
    print('[BOOT][FATAL] service-account.json not found (and no Base64 provided)')

if not creds:
    raise RuntimeError('Google service account credentials not available')

# 3) Initialize clients WITH credentials (storage first needs creds before bucket usage)
storage_client = storage.Client(credentials=creds, project=creds.project_id)
bucket = storage_client.bucket(GCS_BUCKET_NAME) if GCS_BUCKET_NAME else None
if not GCS_BUCKET_NAME:
    print('[BOOT][WARN] GCS_BUCKET_NAME empty; GCS features disabled (local fallback)')

sheets_service = build('sheets', 'v4', credentials=creds)
drive_service  = build('drive',  'v3', credentials=creds)

client = DocumentIntelligenceClient(AZURE_ENDPOINT, AzureKeyCredential(AZURE_KEY))

# =========================================================
# 商品カタログ取得（テンプレート『商品』シートを 1 回読み込んで辞書化）
#   - Range: '商品'!A10:H30000 （A=商品CD, B=メーカー, C=商品名, D=規格, H=備考想定）
#   - 先頭・末尾の空白を除去し、商品CDは先頭ゼロを削除した形でもマップ
# =========================================================
def load_product_catalog(spreadsheet_id, yield_event):
    try:
        rng = "'商品'!A10:H30000"
        res = yield from execute_with_backoff(
            sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id, range=rng, valueRenderOption='FORMATTED_VALUE'
            ),
            yield_event, label="values.get[catalog]"
        )
        values = res.get('values', [])
        catalog = {}
        for row in values:
            if not row:
                continue
            code_raw = (row[0] if len(row) > 0 else '').strip()
            if not code_raw:
                continue
            code_norm = code_raw.lstrip('0') or '0'
            catalog[code_raw] = row
            catalog[code_norm] = row
        return catalog
    except Exception as e:
        yield (yield_event("dbg", f"[CATALOG][ERROR] {e}"))
        return {}

# =========================================================
# 共通：指数バックオフ＋ジッター
# =========================================================
def _status_of_http_error(e: HttpError):
    return getattr(e, "status_code", None) or getattr(getattr(e, "resp", None), "status", None)

def execute_with_backoff(api_call, yield_event, *, max_retries=8, base_delay=0.6, label=""):
    retry_statuses = {408, 429, 500, 502, 503, 504}
    delay = base_delay
    for i in range(max_retries):
        try:
            res = api_call.execute()
            yield (yield_event("dbg", f"[OK][{label}] try={i}"))
            return res
        except HttpError as e:
            st = _status_of_http_error(e)
            if st in retry_statuses and i < max_retries - 1:
                yield (yield_event("dbg", f"[RETRY][{label}] HttpError {st}; sleep {round(delay,2)}s"))
                time.sleep(delay + random.uniform(0, delay * 0.3))
                delay = min(delay * 2, 30)
                continue
            yield (yield_event("dbg", f"[ERROR][{label}] HttpError {st}: {e}"))
            raise
        except Exception as e:
            if i < max_retries - 1:
                yield (yield_event("dbg", f"[RETRY][{label}] {e.__class__.__name__}; sleep {round(delay,2)}s"))
                time.sleep(delay + random.uniform(0, delay * 0.3))
                delay = min(delay * 2, 30)
                continue
            yield (yield_event("dbg", f"[ERROR][{label}] {e}"))
            raise

def analyze_with_backoff(content, yield_event, *, attempts=3, initial_delay=0.7):
    delay = initial_delay
    for i in range(attempts):
        try:
            poller = client.begin_analyze_document('prebuilt-layout', content)
            res = poller.result()
            yield (yield_event("dbg", f"[OCR] ok try={i}"))
            return res
        except Exception as e:
            if i < attempts - 1:
                yield (yield_event("dbg", f"[OCR][RETRY] {e.__class__.__name__}; sleep {round(delay,2)}s"))
                time.sleep(delay + random.uniform(0, delay * 0.3))
                delay = min(delay * 2, 8)
                continue
            yield (yield_event("dbg", f"[OCR][ERROR] {e}"))
            raise

# =========================================================
# 入札書向け後処理
# =========================================================
def process_bid_tables(merged_rows, yield_event):
    if not merged_rows: return []
    header = list(merged_rows[0])
    # テンプレの期待：後処理で「成分表」「見本」を追加
    if "成分表" not in header: header.append("成分表")
    if "見本"   not in header: header.append("見本")
    # 優先して使いたい列名の候補（OCRゆらぎを吸収）
    prefer_patterns = {"銘柄·条件", "銘柄・条件", "銘柄条件"}
    judge_idx = None
    for i, h in enumerate(merged_rows[0]):
        if not h: continue
        norm = str(h).replace(" ", "").replace("　", "")
        norm_mid = norm.replace("・", "·")
        norm_cmp = norm_mid.replace("·", "")
        if norm in prefer_patterns or norm_mid in prefer_patterns or norm_cmp == "銘柄条件":
            judge_idx = i
            break
    if judge_idx is None:
        # フォールバック（既存ロジック）
        target = set("銘柄条件提出見本備考")
        best, idx_f = -1, 0
        for i, h in enumerate(merged_rows[0]):
            if h:
                score = sum(1 for c in set(str(h)) if c in target)
                if score > best:
                    best, idx_f = score, i
        judge_idx = idx_f
        yield (yield_event("dbg", f"[POST] header={header} judge(FALLBACK) idx={judge_idx} name={merged_rows[0][judge_idx]}"))
    else:
        yield (yield_event("dbg", f"[POST] header={header} judge(FIXED) idx={judge_idx} name={merged_rows[0][judge_idx]}"))

    out = [header]
    match_seibun = set("成分表提出")
    hits = 0
    for r in merged_rows[1:]:
        row = list(r)
        while len(row) < len(header): row.append("")
        val = (row[judge_idx] or "")
        cnt = sum(1 for c in set(val) if c in match_seibun)
        # 成分表列: 条件不成立でも明示的に "-" を入れる
        row[-2] = "○" if cnt >= 2 else "-"
        # 見本列: 見本文字列含む場合 3、なければ - （仕様通り）
        row[-1] = "3" if "見本" in val else "-"
        if row[-2] == "○": hits += 1
        out.append(row)
    yield (yield_event("dbg", f"[POST] 成分表=○ hits={hits}/{len(out)-1}"))
    return out

# =========================================================
# SSEユーティリティ
# =========================================================
def make_yielders():
    last_hb = {"t": time.time()}
    def yield_event(t, data):
        payload = json.dumps({"event": t, "data": data}, ensure_ascii=False)
        return (f"data: {payload}\n\n").encode("utf-8")
    def heartbeat():
        now = time.time()
        if now - last_hb["t"] >= 5:
            last_hb["t"] = now
            return b": hb\n\n"
        return None
    return yield_event, heartbeat

# =========================================================
# Sheets/Drive ヘルパ
# =========================================================
def create_template_work_copy(original_id, title_suffix, yield_event):
    name = f"WORK_{title_suffix}"
    body = {"name": name, "parents": [DRIVE_FOLDER_ID]} if DRIVE_FOLDER_ID else {"name": name}
    res = yield from execute_with_backoff(
        drive_service.files().copy(
            fileId=original_id, body=body, supportsAllDrives=True, fields='id'
        ),
        yield_event, label="files.copy"
    )
    return res

def delete_file(file_id, yield_event):
    try:
        _ = yield from execute_with_backoff(
            drive_service.files().delete(fileId=file_id, supportsAllDrives=True),
            yield_event, label="files.delete"
        )
    except HttpError as e:
        st = _status_of_http_error(e)
        yield (yield_event("dbg", f"[WARN] files.delete {file_id} HttpError {st}: {e}"))
    except Exception as e:
        yield (yield_event("dbg", f"[WARN] files.delete {file_id} {e}"))

def batch_get_values(spreadsheet_id, ranges, yield_event, label="values.batchGet"):
    res = yield from execute_with_backoff(
        sheets_service.spreadsheets().values().batchGet(
            spreadsheetId=spreadsheet_id, ranges=ranges, valueRenderOption='FORMATTED_VALUE'
        ),
        yield_event, label=label
    )
    return res

def batch_update_values(spreadsheet_id, data, yield_event, label="values.batchUpdate"):
    res = yield from execute_with_backoff(
        sheets_service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id, body={'valueInputOption': 'USER_ENTERED', 'data': data}
        ),
        yield_event, label=label
    )
    return res

def clear_values(spreadsheet_id, a1_range, yield_event, label="values.clear"):
    res = yield from execute_with_backoff(
        sheets_service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range=a1_range),
        yield_event, label=label
    )
    return res

# --- 列幅ユーティリティ ---
def _col_width(start_col_char: str, end_col_char: str) -> int:
    return ord(end_col_char.upper()) - ord(start_col_char.upper()) + 1

# =========================================================
# ポーリング（B:K で「メーカー名が入った行」を準備完了として判定）
#   - B列（相対index 0）を既定の「準備完了フラグ」とする
#   - 行末の完全空行トリムに影響されない
#   - “準備完了行 / 期待行数 >= POLL_MIN_READY” で終了
# =========================================================
def poll_until_ready(spreadsheet_id, start_row, n_rows, end_col_char, yield_event):
    # 取得は **B:K**（要求仕様：A=商品CD、B~Kが計算結果）
    start_col_char = 'B'
    rng = [f"'成分表'!{start_col_char}{start_row}:{end_col_char}{start_row + n_rows - 1}"]
    colw_expected = _col_width(start_col_char, end_col_char)

    t0 = time.time()
    delay = 0.20
    attempt = 0
    last_ready = -1
    stable = 0
    MIN_STABLE = 3

    while True:
        attempt += 1
        res = yield from batch_get_values(spreadsheet_id, rng, yield_event, label="values.batchGet[poll]")
        values = (res.get('valueRanges') or [{}])[0].get('values', [])

        # 行数が短い場合があるためパディング（列数はB:Kの想定幅で合わせる）
        if len(values) < n_rows:
            values += [[] for _ in range(n_rows - len(values))]
        values = [ (row + [""] * (colw_expected - len(row)))[:colw_expected] for row in values ]

        # 準備完了（B列＝index 0 が非空）の行数
        ready_rows = sum(1 for r in values if str(r[READY_COL_IDX]).strip() != "")
        ready_ratio = ready_rows / max(1, n_rows)

        yield (yield_event("dbg", f"[POLL] ready={ready_rows}/{n_rows} ({ready_ratio:.2f}) attempt={attempt} delay={round(delay,2)}s"))

        # 準備完了率で判定（既定 95%）
        if ready_ratio >= POLL_MIN_READY:
            return values

        # 進捗が止まっているなら早期収束（部分結果返却）
        if ready_rows == last_ready:
            stable += 1
        else:
            stable = 0
            last_ready = ready_rows
        if stable >= MIN_STABLE and ready_ratio >= 0.5:
            yield (yield_event("dbg", "[POLL][EARLY-STOP] stable; return partial"))
            return values

        if time.time() - t0 > POLL_MAX_WAIT:
            yield (yield_event("dbg", "[POLL][TIMEOUT] return partial"))
            return values

        time.sleep(delay + random.uniform(0, delay * 0.3))
        delay = min(delay * 2, 5)

# =========================================================
# 「作業用コピーで一括計算」：A列に商品CDを書き込み→B:Kを取得（テンプレ仕様に厳密一致）
# =========================================================
def compute_all_in_one_copy(work_id, selections, start_row, end_col_char, yield_event):
    N = len(selections)
    values_all = []
    idx = 0
    while idx < N:
        batch = selections[idx: idx + CHUNK_SIZE]
        cds = [[cd] for (_, cd) in batch]

        yield (yield_event("dbg", f"[BATCH] write {len(batch)} rows idx={idx}"))
        _ = yield from batch_update_values(
            work_id,
            [{'range': f"'成分表'!A{start_row}", 'values': cds}],
            yield_event, label="values.batchUpdate[A-all]"
        )

        # ★ B:K（= メーカー名/商品名/規格/...）を取得して準備完了を判定
        vals = yield from poll_until_ready(work_id, start_row, len(batch), end_col_char, yield_event)

        # 可観測性（どれだけ埋まったか）
        filled_rows = sum(1 for r in vals if any(str(c).strip() != "" for c in r))
        yield (yield_event("dbg", f"[BATCH] filled_rows(B:K)={filled_rows}/{len(batch)}"))

        values_all.extend(vals)

        # 使い終えたA列をクリア（少し余裕を持って）
        _ = yield from clear_values(
            work_id, f"'成分表'!A{start_row}:A{start_row + len(batch) + 10}",
            yield_event, label="values.clear[A-all]"
        )
        idx += len(batch)
    return values_all

# =========================================================
# Routes
# =========================================================
@app.route("/")
def index():
    return send_from_directory("static", "index.html")

@app.route("/sheet1")
def sheet1():
    return send_from_directory("static", "index_sheet1.html")

# --- Step1 ---
@app.route("/analyze_and_calculate", methods=["POST"])
def analyze_and_calculate():
    task_id = uuid.uuid4().hex[:8]
    base_path = f"tmp/{task_id}"

    ref_file = request.files.get('refSheetFile')
    ref_file_path = ""
    if ref_file:
        ref_file_path = f"{base_path}/ref/{ref_file.filename}"
        if bucket:
            try:
                bucket.blob(ref_file_path).upload_from_file(ref_file)
            except Exception as _e:
                print(f"[GCS][WARN] upload ref failed fallback to local: {_e}")
                _local_path = f"/tmp_fallback/{ref_file_path}"
                os.makedirs(os.path.dirname(_local_path), exist_ok=True)
                ref_file.stream.seek(0)
                ref_file.save(_local_path)
        else:
            _local_path = f"/tmp_fallback/{ref_file_path}"
            os.makedirs(os.path.dirname(_local_path), exist_ok=True)
            ref_file.save(_local_path)

    ocr_files = request.files.getlist('file')
    ocr_paths = []
    for f in ocr_files:
        p = f"{base_path}/ocr/{f.filename}"
        if bucket:
            try:
                bucket.blob(p).upload_from_file(f)
            except Exception as _e:
                print(f"[GCS][WARN] upload ocr failed fallback to local: {_e}")
                _local_path = f"/tmp_fallback/{p}"
                os.makedirs(os.path.dirname(_local_path), exist_ok=True)
                f.stream.seek(0)
                f.save(_local_path)
        else:
            _local_path = f"/tmp_fallback/{p}"
            os.makedirs(os.path.dirname(_local_path), exist_ok=True)
            f.save(_local_path)
        ocr_paths.append(p)

    sheet_name = request.form.get('sheet')

    def generate():
        yield_event, heartbeat = make_yielders()
        try:
            # 1) 参照ファイル
            ref_table = []
            if ref_file_path:
                yield (yield_event("dbg", f"[STEP1] read ref {ref_file_path}"))
                if bucket:
                    try:
                        buf = io.BytesIO(bucket.blob(ref_file_path).download_as_bytes())
                    except Exception as _e:
                        yield (yield_event("dbg", f"[GCS][WARN] download ref fallback local: {_e}"))
                        _lp = f"/tmp_fallback/{ref_file_path}"
                        buf = open(_lp, 'rb')
                else:
                    _lp = f"/tmp_fallback/{ref_file_path}"
                    buf = open(_lp, 'rb')
                fn = ref_file_path.lower()
                if fn.endswith(('.xlsx', '.xls')): df = pd.read_excel(buf, header=0, dtype=str)
                elif fn.endswith('.csv'):          df = pd.read_csv(buf, header=0, dtype=str)
                else:                               df = pd.DataFrame()
                if not df.empty:
                    df = df[df.iloc[:,0].notna() & df.iloc[:,0].astype(str).str.strip().astype(bool)].reset_index(drop=True).fillna("").astype(str)
                    ref_table.append(df.columns.tolist()); ref_table.extend(df.values.tolist())
                yield (yield_event("dbg", f"[REF] header={ref_table[0] if ref_table else []} rows={len(ref_table)-1 if ref_table else 0}"))
                yield (yield_event("ref_table", ref_table))

            # 2) OCR
            merged_rows = []
            yield (yield_event("dbg", f"[STEP1] OCR start n_files={len(ocr_paths)}"))
            for i, p in enumerate(ocr_paths):
                hb = heartbeat()
                if hb: yield hb
                if bucket:
                    try:
                        content = bucket.blob(p).download_as_bytes()
                    except Exception as _e:
                        yield (yield_event("dbg", f"[GCS][WARN] download ocr fallback local: {_e}"))
                        _lp = f"/tmp_fallback/{p}"
                        with open(_lp, 'rb') as _f:
                            content = _f.read()
                else:
                    _lp = f"/tmp_fallback/{p}"
                    with open(_lp, 'rb') as _f:
                        content = _f.read()
                result = yield from analyze_with_backoff(content, yield_event)
                t = result.tables[0] if result.tables else None
                if t:
                    cur = [[""] * t.column_count for _ in range(t.row_count)]
                    for cell in t.cells:
                        cur[cell.row_index][cell.column_index] = (cell.content or "").replace("\n", " ")
                    merged_rows.extend(cur if i == 0 else cur[1:])
            if sheet_name == "入札書":
                merged_rows = yield from process_bid_tables(merged_rows, yield_event)
            yield (yield_event("dbg", f"[OCR] rows={len(merged_rows)} cols={(len(merged_rows[0]) if merged_rows else 0)}"))
            yield (yield_event("ocr_table", merged_rows))

            # 3) OCR結果を入札書/見積書スプシへ（参考保存）
            conf = config.SHEETS.get(sheet_name, config.DEFAULT_SHEET)
            gid = conf.get('gid', '0')
            sheet_url = f"https://docs.google.com/spreadsheets/d/{conf['spreadsheet_id']}/edit#gid={gid}"
            _ = yield from execute_with_backoff(
                sheets_service.spreadsheets().values().clear(
                    spreadsheetId=conf['spreadsheet_id'], range=f"{conf['range_prefix']}A1:ZZ"
                ), yield_event, label="values.clear(main)"
            )
            if merged_rows:
                _ = yield from execute_with_backoff(
                    sheets_service.spreadsheets().values().update(
                        spreadsheetId=conf['spreadsheet_id'],
                        range=f"{conf['range_prefix']}A1",
                        valueInputOption='USER_ENTERED',
                        body={'values': merged_rows}
                    ), yield_event, label="values.update(main)"
                )
            yield (yield_event("final_url", {"name":"OCR結果のスプレッドシート","url":sheet_url}))

            # 4) selections 抽出（ref×ocr）
            selections = []
            if ref_table and merged_rows:
                header, ref_header = merged_rows[0], ref_table[0]
                yield (yield_event("dbg", f"[SEL] ocr_header={header}"))
                yield (yield_event("dbg", f"[SEL] ref_header={ref_header}"))
                if '成分表' in header and '見本' in header and '商品CD' in ref_header and 'メーカー' in ref_header:
                    seibun_idx = header.index('成分表')
                    mihon_idx  = header.index('見本')
                    cd_idx     = ref_header.index('商品CD')
                    maker_idx  = ref_header.index('メーカー')
                    hits = 0
                    for ridx, row in enumerate(merged_rows[1:], start=1):
                        if ridx >= len(ref_table):
                            continue
                        seibun_flag = (len(row) > seibun_idx and row[seibun_idx] == '○')
                        mihon_flag  = (len(row) > mihon_idx and row[mihon_idx] in ('3', '○'))
                        if seibun_flag or mihon_flag:
                            cd = (ref_table[ridx][cd_idx]).lstrip('0') or '0'
                            maker = ref_table[ridx][maker_idx]
                            selections.append((maker, cd, '○' if seibun_flag else '-', '3' if mihon_flag else '-'))
                            hits += 1
                    yield (yield_event("dbg", f"[SEL] 成分表/見本 hits={hits} selections={len(selections)}"))
                else:
                    yield (yield_event("dbg", "[SEL][WARN] 必要ヘッダが見つからない"))
            yield (yield_event("dbg", f"[SEL] preview_first10={selections[:10]}"))
            yield (yield_event("selections", selections[:10]))

            # 5) 商品シートから直接参照しローカルで B:K 相当を構築
            all_maker_data = {}
            maker_cds = {}
            flags_list = []  # [[maker_key, cd, seibun_flag, mihon_flag], ...]
            if selections:
                catalog = yield from load_product_catalog(config.TEMPLATE_SPREADSHEET_ID, yield_event)
                yield (yield_event("dbg", f"[CATALOG] size={len(catalog)}"))
                for maker, cd, seibun_flag, mihon_flag in selections:
                    maker_key = maker or "メーカー名なし"
                    maker_cds.setdefault(maker_key, []).append(cd)
                    flags_list.append([maker_key, cd, seibun_flag, mihon_flag])
                # values: [メーカー, 商品名, 規格, 備考] （元仕様）
                for maker_key, cds in maker_cds.items():
                    rows = []
                    miss = 0
                    for cd in cds:
                        row = catalog.get(cd) or catalog.get(cd.lstrip('0') or '0')
                        if row:
                            maker_val = (row[1] if len(row) > 1 else '') or maker_key
                            product_name = (row[2] if len(row) > 2 else '')
                            spec = (row[3] if len(row) > 3 else '')
                            note = (row[7] if len(row) > 7 else '')
                            rows.append([maker_val, product_name, spec, note])
                        else:
                            miss += 1
                            rows.append([maker_key, '', '', f'NOT_FOUND:{cd}'])
                    if miss:
                        yield (yield_event("dbg", f"[CATALOG][MISS] maker={maker_key} missing={miss}/{len(cds)}"))
                    all_maker_data[maker_key] = rows
                yield (yield_event("dbg", f"[LOCAL_LOOKUP] makers={len(all_maker_data)}"))

            yield (yield_event("dbg", f"[STEP1] makers={len(all_maker_data)}"))
            yield (yield_event("calculation_complete", {"maker_data": all_maker_data, "maker_cds": maker_cds, "flags": flags_list}))
            yield (yield_event("dbg", "[STEP1] done"))

        except Exception as e:
            yield (yield_event("dbg", f"[FATAL][STEP1] {e}"))
            traceback.print_exc()
        finally:
            if bucket:
                try:
                    for blob in bucket.list_blobs(prefix=f"tmp/{task_id}/"):
                        blob.delete()
                    yield (yield_event("dbg", "[CLEANUP] tmp deleted"))
                except Exception as ce:
                    yield (yield_event("dbg", f"[CLEANUP][WARN] {ce}"))
            else:
                # Local fallback cleanup (best-effort)
                try:
                    _base = f"/tmp_fallback/tmp/{task_id}"
                    if os.path.exists(_base):
                        import shutil
                        shutil.rmtree(_base, ignore_errors=True)
                        yield (yield_event("dbg", "[CLEANUP] local tmp deleted"))
                except Exception as ce:
                    yield (yield_event("dbg", f"[CLEANUP][WARN] local {ce}"))
            yield (yield_event("done", "ステップ1完了"))

    return Response(generate(), mimetype='text/event-stream; charset=utf-8')

# --- Step2 ---
@app.route("/create_spreadsheet", methods=["POST"])
def create_spreadsheet():
    data = request.get_json()
    all_maker_data = data.get('maker_data', {})
    maker_cds      = data.get('maker_cds', {})
    flags_list     = data.get('flags', [])

    # flags を (maker, cd) -> (成分表フラグ, 見本フラグ) に整理
    flags_map = {}
    for item in flags_list:
        try:
            mk, cd, s_flag, m_flag = item
            flags_map[(mk, cd)] = (s_flag, m_flag)
        except Exception:
            continue

    # シート名サニタイズ
    def sanitize_title(title: str, existing: set):
        base = ''.join(c for c in title.strip()[:80] if c not in ':\\/?*[]') or '無名'
        t = base
        suffix = 1
        while t in existing:
            suffix += 1
            t = f"{base}_{suffix}"
            if len(t) > 90:
                t = t[:90]
        existing.add(t)
        return t

    def generate():
        yield_event, heartbeat = make_yielders()
        try:
            ts = (datetime.now(timezone.utc) + timedelta(hours=9)).strftime('%Y%m%d_%H%M%S')
            out_name = f"成分表出力_{ts}"
            yield (yield_event("dbg", f"[STEP2] create {out_name}"))

            # --- Diagnostic (no functional change) ---
            if os.environ.get('DRIVE_DIAG', '1') == '1':
                try:
                    sa_email = getattr(creds, 'service_account_email', getattr(creds, 'service_account_email', 'UNKNOWN'))
                except Exception:
                    sa_email = 'UNKNOWN'
                try:
                    about = drive_service.about().get(fields='user,storageQuota').execute()
                    quota = about.get('storageQuota', {})
                    usage = quota.get('usage')
                    limit = quota.get('limit')
                    yield (yield_event("dbg", f"[STEP2][DIAG] sa_email={sa_email} usage={usage} limit={limit}"))
                except Exception as de:
                    yield (yield_event("dbg", f"[STEP2][DIAG][WARN] about.get failed {de}"))
                try:
                    # Count existing output spreadsheets (not trashed)
                    q = "name contains '成分表出力_' and mimeType='application/vnd.google-apps.spreadsheet' and trashed=false"
                    res_list = drive_service.files().list(q=q, pageSize=10, fields='files(id,name)', supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
                    files_list = res_list.get('files', [])
                    yield (yield_event("dbg", f"[STEP2][DIAG] recent_outputs={len(files_list)} sample={[f.get('name') for f in files_list][:3]}"))
                except Exception as le:
                    yield (yield_event("dbg", f"[STEP2][DIAG][WARN] list failed {le}"))

            new_ss = yield from execute_with_backoff(
                drive_service.files().create(
                    body={'name': out_name, 'mimeType': 'application/vnd.google-apps.spreadsheet',
                          'parents': [DRIVE_FOLDER_ID] if DRIVE_FOLDER_ID else []},
                    supportsAllDrives=True
                ), yield_event, label="files.create"
            )
            out_id = (new_ss or {}).get('id')
            yield (yield_event("dbg", f"[STEP2] new_id={out_id}"))
            out_url = f"https://docs.google.com/spreadsheets/d/{out_id}/edit"
            yield (yield_event("final_url", {"name":"メーカーごとの各種依頼書スプレッドシート","url":out_url}))

            # 既定シートを削除（あれば）
            try:
                meta = yield from execute_with_backoff(
                    sheets_service.spreadsheets().get(spreadsheetId=out_id, fields='sheets.properties'),
                    yield_event, label="spreadsheets.get"
                )
                first_id = meta['sheets'][0]['properties']['sheetId']
                _ = yield from execute_with_backoff(
                    sheets_service.spreadsheets().batchUpdate(
                        spreadsheetId=out_id, body={'requests':[{'deleteSheet': {'sheetId': first_id}}]}
                    ),
                    yield_event, label="spreadsheets.batchUpdate[delFirst]"
                )
            except Exception:
                pass

            template_id       = config.TEMPLATE_SPREADSHEET_ID
            template_sheet_id = config.TEMPLATE_SHEET_ID
            start_row         = config.START_ROW

            total = len(all_maker_data)
            existing_titles = set()
            for i, (maker, values_bk) in enumerate(all_maker_data.items(), start=1):
                hb = heartbeat()
                if hb: yield hb
                yield (yield_event("dbg", f"[MK-SHEET] {i}/{total} maker={maker} rows={len(values_bk)} cds={len(maker_cds.get(maker,[]))}"))

                copied = yield from execute_with_backoff(
                    sheets_service.spreadsheets().sheets().copyTo(
                        spreadsheetId=template_id, sheetId=template_sheet_id, body={'destinationSpreadsheetId': out_id}
                    ),
                    yield_event, label="sheets.copyTo"
                )
                new_sheet_id = (copied or {}).get('sheetId')
                safe_title = sanitize_title(maker or 'メーカー名なし', existing_titles)
                _ = yield from execute_with_backoff(
                    sheets_service.spreadsheets().batchUpdate(
                        spreadsheetId=out_id,
                        body={'requests': [{'updateSheetProperties': {'properties': {'sheetId': new_sheet_id, 'title': safe_title}, 'fields': 'title'}}]}
                    ),
                    yield_event, label="spreadsheets.batchUpdate[rename]"
                )
                cds = maker_cds.get(maker, [])
                # 依頼内容にサンプル(=見本フラグ)が含まれていれば担当=営業担当, なければ 大出 を E22:G22 に設定
                try:
                    mihon_exists = any((flags_map.get((maker, cd)) or ('',''))[1] in ('3','○') for cd in cds)
                    tantou_val = '営業担当' if mihon_exists else '大出'
                    _ = yield from execute_with_backoff(
                        sheets_service.spreadsheets().values().update(
                            spreadsheetId=out_id,
                            range=f"'{safe_title}'!E22:G22",
                            valueInputOption='USER_ENTERED',
                            body={'values': [[tantou_val, tantou_val, tantou_val]]}
                        ),
                        yield_event, label="values.update[担当]"
                    )
                except Exception as _e:
                    yield (yield_event("dbg", f"[STEP2][担当][WARN] {_e}"))
                if cds and values_bk:
                    # ---- ヘッダ検査と列挿入（規格と備考の間に 成分表 / 見本 が無ければ追加） ----
                    def col_letter(idx0: int) -> str:
                        s = ""
                        n = idx0 + 1
                        while n:
                            n, r = divmod(n - 1, 26)
                            s = chr(65 + r) + s
                        return s
                    header_row = config.START_ROW - 1
                    # A～Z まで読み込み（必要に応じ拡張可）
                    rng_header = f"'{safe_title}'!A{header_row}:Z{header_row}"
                    try:
                        res_header = yield from execute_with_backoff(
                            sheets_service.spreadsheets().values().get(
                                spreadsheetId=out_id, range=rng_header, valueRenderOption='FORMATTED_VALUE'
                            ), yield_event, label="values.get[export-header]"
                        )
                        header_vals = (res_header.get('values') or [[]])[0]
                    except Exception:
                        header_vals = []
                    # インデックス探索
                    def find_idx(keyword: str):
                        for j, v in enumerate(header_vals):
                            if str(v).strip() == keyword:
                                return j
                        return None
                    spec_idx = find_idx('規格')
                    seibun_idx = find_idx('成分表')
                    mihon_idx = find_idx('見本') or find_idx('サンプル')
                    # 備考列（複数想定、最初の備考位置）
                    biko_indices = [j for j, v in enumerate(header_vals) if str(v).startswith('備考')]
                    first_biko_idx = biko_indices[0] if biko_indices else None
                    # 成分表/見本列が存在しない && 規格と備考がある → 挿入
                    if spec_idx is not None and first_biko_idx is not None and (seibun_idx is None and mihon_idx is None or seibun_idx is None or mihon_idx is None):
                        insert_pos = spec_idx + 1  # 規格の直後
                        need_cols = 0
                        if seibun_idx is None: need_cols += 1
                        if mihon_idx is None: need_cols += 1
                        if need_cols:
                            _ = yield from execute_with_backoff(
                                sheets_service.spreadsheets().batchUpdate(
                                    spreadsheetId=out_id,
                                    body={'requests':[{'insertDimension': {'range': {'sheetId': new_sheet_id, 'dimension': 'COLUMNS', 'startIndex': insert_pos, 'endIndex': insert_pos + need_cols}, 'inheritFromBefore': True}}]}
                                ), yield_event, label="spreadsheets.batchUpdate[insertCols]"
                            )
                            # ヘッダ書き込み
                            headers_to_write = []
                            write_range = f"'{safe_title}'!{col_letter(insert_pos)}{header_row}:{col_letter(insert_pos+need_cols-1)}{header_row}"
                            for _i in range(need_cols):
                                if seibun_idx is None:
                                    headers_to_write.append(['成分表'])
                                    seibun_idx = insert_pos
                                    insert_pos += 1
                                elif mihon_idx is None:
                                    headers_to_write.append(['見本'])
                                    mihon_idx = insert_pos
                                    insert_pos += 1
                            _ = yield from execute_with_backoff(
                                sheets_service.spreadsheets().values().update(
                                    spreadsheetId=out_id, range=write_range, valueInputOption='USER_ENTERED', body={'values': list(map(list, zip(*headers_to_write)))}
                                ), yield_event, label="values.update[insertHeaders]"
                            )
                            # ヘッダ再取得（後続の letter 計算統一のため）
                            try:
                                res_header = yield from execute_with_backoff(
                                    sheets_service.spreadsheets().values().get(
                                        spreadsheetId=out_id, range=rng_header, valueRenderOption='FORMATTED_VALUE'
                                    ), yield_event, label="values.get[export-header2]"
                                )
                                header_vals = (res_header.get('values') or [[]])[0]
                            except Exception:
                                pass
                    # 再計算
                    spec_idx = find_idx('規格') if spec_idx is None else spec_idx
                    seibun_idx = find_idx('成分表') if seibun_idx is None else seibun_idx
                    mihon_idx = find_idx('見本') or find_idx('サンプル') if mihon_idx is None else mihon_idx
                    # 備考開始位置（最初の備考）
                    first_biko_idx = ([j for j, v in enumerate(header_vals) if str(v).startswith('備考')] or [None])[0]
                    # データ書き込み準備
                    n = min(len(cds), len(values_bk))
                    cds = cds[:n]
                    values_bk = values_bk[:n]
                    makers_col = []
                    product_colD = []
                    spec_col = []
                    seibun_col = []
                    mihon_col = []
                    note_col = []
                    for i in range(n):
                        row = values_bk[i] or []
                        # row 構成: [メーカー, 商品名, 規格, 備考] （成分表/見本は flags_map から取得）
                        makers_col.append([row[0] if len(row) > 0 else ""])
                        product_colD.append([row[1] if len(row) > 1 else ""])
                        spec_col.append([row[2] if len(row) > 2 else ""])
                        note_val = row[3] if len(row) > 3 else ""
                        note_col.append([note_val])
                        s_flag, m_flag = flags_map.get((maker, cds[i]), ("", ""))
                        seibun_col.append([s_flag])
                        mihon_col.append([m_flag])
                    empty_col = [[""] for _ in range(n)]
                    # 既定列レター（固定部）
                    A = 'A'; C='C'; D='D'; E='E'; F='F'; G='G'; H='H'
                    # 規格は既定想定 H（テンプレが違う場合は spec_idx で上書き）
                    if spec_idx is not None:
                        H = col_letter(spec_idx)
                    if seibun_idx is not None:
                        I = col_letter(seibun_idx)
                    else:
                        I = col_letter((spec_idx or 7)+1)
                    if mihon_idx is not None:
                        J = col_letter(mihon_idx)
                    else:
                        J = col_letter((spec_idx or 7)+2)
                    # 備考は first_biko_idx 位置（無い場合は J の次）
                    if first_biko_idx is not None:
                        K = col_letter(first_biko_idx)
                        L = col_letter(first_biko_idx + 1) if first_biko_idx + 1 < 26 else col_letter(first_biko_idx + 1)
                    else:
                        K = col_letter(ord(J)-65 + 1)
                        L = col_letter(ord(J)-65 + 2)
                    data_updates = [
                        {'range': f"'{safe_title}'!{A}{start_row}", 'values': [[cd] for cd, *_ in cds]},
                        {'range': f"'{safe_title}'!{C}{start_row}", 'values': makers_col},
                        {'range': f"'{safe_title}'!{D}{start_row}", 'values': product_colD},
                        {'range': f"'{safe_title}'!{E}{start_row}", 'values': empty_col},
                        {'range': f"'{safe_title}'!{F}{start_row}", 'values': empty_col},
                        {'range': f"'{safe_title}'!{G}{start_row}", 'values': empty_col},
                        {'range': f"'{safe_title}'!{H}{start_row}", 'values': spec_col},
                        {'range': f"'{safe_title}'!{I}{start_row}", 'values': seibun_col},
                        {'range': f"'{safe_title}'!{J}{start_row}", 'values': mihon_col},
                        {'range': f"'{safe_title}'!{K}{start_row}", 'values': note_col},
                        # 2つ目備考は空（仕様未確定）
                        {'range': f"'{safe_title}'!{L}{start_row}", 'values': empty_col},
                    ]
                    _ = yield from batch_update_values(out_id, data_updates, yield_event, label="values.batchUpdate[export]")
                # 過剰な固定sleepは不要。APIクォータ保護のため最小バックオフ（行数多い場合のみ軽い休止）
                if len(values_bk) > 200:
                    time.sleep(0.1)

            yield (yield_event("dbg", "[STEP2] done"))
        except Exception as e:
            yield (yield_event("dbg", f"[FATAL][STEP2] {e}"))
            traceback.print_exc()
        finally:
            yield (yield_event("done", "ステップ2完了"))

    return Response(generate(), mimetype='text/event-stream; charset=utf-8')

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
