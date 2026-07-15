FROM python:3.12-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir --timeout 120 -e ".[dev]"
