ARG N8N_VERSION=latest
FROM n8nio/n8n:${N8N_VERSION}

USER root
RUN apk add --no-cache python3 py3-pip sqlite bash curl ca-certificates \
 && python3 -m pip install --no-cache-dir --break-system-packages requests Pillow boto3

RUN mkdir -p /opt/overwatch /data/media /data/audit /data/backups /data/config
COPY scripts/ /opt/overwatch/scripts/
COPY config/ /opt/overwatch/config/
COPY requirements.txt /opt/overwatch/requirements.txt
RUN chmod +x /opt/overwatch/scripts/*.py

ENV OVERWATCH_ROOT=/data
ENV OVERWATCH_CONFIG=/opt/overwatch/config
USER node
