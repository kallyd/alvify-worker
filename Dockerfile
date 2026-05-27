FROM python:3.11-slim

WORKDIR /app

# System deps for Playwright
RUN apt-get update && apt-get install -y \
    wget curl libglib2.0-0 libnss3 libnspr4 libatk1.0-0 \
    libatk-bridge2.0-0 libcups2 libdrm2 libdbus-1-3 libxcb1 \
    libxkbcommon0 libx11-6 libxcomposite1 libxdamage1 libxext6 \
    libxfixes3 libxrandr2 libgbm1 libpango-1.0-0 libcairo2 \
    libasound2 libxshmfence1 --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY worker/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Install Playwright browsers
RUN playwright install chromium

# Copy backend core (scraper, browser_pool) into path
COPY backend/app/core /app/app/core
COPY backend/app/models /app/app/models

# Copy the worker entry point
COPY worker/main.py /app/main.py

ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]
