# ベースイメージとして、軽量なPython 3.10-slimを使用します
FROM python:3.10-slim

# コンテナ内での作業ディレクトリを設定します
WORKDIR /app

# 必要なライブラリをインストールします
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# アプリケーションのソースコードをコピーします
COPY . .

# Cloud Run が自動で設定するポートを環境変数で受け取る
ENV PORT 8080

# コンテナでリッスンするポートを開放
EXPOSE 8080

# shell 形式で起動し、$PORT を展開し、タイムアウトを300秒に設定します
CMD ["sh", "-c", "gunicorn --bind 0.0.0.0:${PORT} --workers 1 --timeout 900 app:app"]
