from google.oauth2 import service_account
from googleapiclient.discovery import build

# サービスアカウント鍵ファイルとスコープ
SERVICE_ACCOUNT_FILE = 'service-account.json'
SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']

creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)
drive = build('drive', 'v3', credentials=creds)

# storageQuota フィールドのみ取得
about = drive.about().get(fields='storageQuota').execute()
print(about)
