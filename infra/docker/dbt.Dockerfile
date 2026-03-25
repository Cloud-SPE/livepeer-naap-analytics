# syntax=docker/dockerfile:1

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN pip install --no-cache-dir dbt-core dbt-clickhouse

WORKDIR /app

COPY warehouse /app/warehouse

WORKDIR /app/warehouse

ENTRYPOINT ["dbt"]
