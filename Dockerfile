# Dockerfile for use with Synology NAS
FROM python:3
RUN mkdir -p /palm
WORKDIR /palm
COPY . .
RUN pip install requests
