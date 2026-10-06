FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY scripts ./scripts

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src \
    PYTHONDONTWRITEBYTECODE=1

CMD ["python", "scripts/generate_seeds.py"]
