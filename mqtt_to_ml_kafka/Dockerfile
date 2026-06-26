FROM python:3.12-slim-bullseye

WORKDIR /app

RUN apt-get update && apt-get install -y nano && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /app/log

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1

CMD ["python", "mqtt_to_ml_kafka.py"]