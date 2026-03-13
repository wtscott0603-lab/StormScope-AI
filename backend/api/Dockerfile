FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY backend/api/requirements.txt /tmp/requirements.txt
COPY scripts/download_geo.sh /tmp/download_geo.sh
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY backend /app/backend
RUN chmod +x /tmp/download_geo.sh && /tmp/download_geo.sh /app/backend/api/static/geo

CMD ["uvicorn", "backend.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
