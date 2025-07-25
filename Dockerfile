FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8080 \
    GOOGLE_CLOUD_PROJECT=dm-data-aiml

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg file build-essential gcc libffi-dev libssl-dev curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["gunicorn", "--timeout", "0", "--workers=4", "--threads=4", "--bind", ":8080", "main:app"]