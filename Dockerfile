FROM python:3.12-slim

# 设置工作目录
WORKDIR /app

# 设置时区为亚洲/上海
RUN apt-get update && \
    apt-get install -y --no-install-recommends tzdata && \
    ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
    echo "Asia/Shanghai" > /etc/timezone && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 复制依赖文件并安装
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制源代码
COPY . .

# 创建必要的目录
RUN mkdir -p /app/logs
RUN mkdir -p /app/config

# 检查配置文件，如果不存在则复制示例配置
RUN if [ ! -f /app/config/config.ini ]; then \
    cp -n /app/config/config.ini.example /app/config/config.ini || echo "No example config found"; \
fi

CMD ["python", "src/main.py"]