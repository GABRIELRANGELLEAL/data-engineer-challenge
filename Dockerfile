# syntax=docker/dockerfile:1.7
FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY . .

RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system -e ".[dev]"
