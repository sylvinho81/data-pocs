FROM python:3.12-slim-bookworm

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config/ config/
COPY seeds/ seeds/
COPY src/ src/
COPY scripts/ scripts/

ENV PYTHONPATH=/app/src

CMD ["python", "scripts/run_demo.py"]
