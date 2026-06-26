FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pred_logger.py .

RUN mkdir -p /data/predictions

ENV PYTHONUNBUFFERED=1

CMD ["python", "pred_logger.py"]