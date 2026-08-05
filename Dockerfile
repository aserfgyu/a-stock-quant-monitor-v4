FROM python:3.11-slim

WORKDIR /app

# 安装系统依赖
RUN apt-get update && apt-get install -y --no-install-recommends     gcc     g++     && rm -rf /var/lib/apt/lists/*

# 复制依赖并安装
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制代码
COPY backend/ ./backend/

WORKDIR /app/backend

EXPOSE 10000

CMD ["python", "app.py"]
