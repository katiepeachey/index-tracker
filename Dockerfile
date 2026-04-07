FROM python:3.11-slim

# Install Playwright's system dependencies as root (required for headless Chromium)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 \
    libdbus-1-3 libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 \
    libxrandr2 libgbm1 libasound2 libpango-1.0-0 libcairo2 \
    libx11-6 libx11-xcb1 libxcb1 libxext6 libxcursor1 libxi6 \
    libxtst6 libxss1 libglib2.0-0 fonts-liberation wget ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Chromium browser (system deps already present above)
RUN playwright install chromium

COPY . .

# Render sets PORT automatically; default to 10000
CMD gunicorn app:app --bind 0.0.0.0:${PORT:-10000} --workers 1 --timeout 300
