FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# NODE_NAME 由 compose 注入；数据库放挂载卷里，便于查看与保留。
ENV BACKEND_DB=/data/writes.db
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
