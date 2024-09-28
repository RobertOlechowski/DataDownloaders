FROM python:3.12-slim

ARG DEBIAN_FRONTEND=noninteractive
ARG BUILD_TIME=undefined
ARG BUILD_VERSION=undefined

ENV RR_BUILD_TIME=$BUILD_TIME
ENV RR_BUILD_VERSION=$BUILD_VERSION
ENV PYTHONPATH=/app

LABEL maintainer="Robert Olechowski <robertolechowski@gmail.com>"
LABEL version=$BUILD_VERSION
LABEL license="Apache License 2.0"

LABEL org.opencontainers.image.title="ETH to DB"
LABEL org.opencontainers.image.description="Tool to collect data from ETH node and insert it to object storage"
LABEL org.opencontainers.image.authors="Robert Olechowski <robertolechowski@gmail.com>"
LABEL org.opencontainers.image.version=$BUILD_VERSION
LABEL org.opencontainers.image.source="https://github.com/RobertOlechowski/DataDownloaders"


WORKDIR /app

COPY ../requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY .. .

CMD ["python3", "main.py"]
