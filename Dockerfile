# Local test image only - the Pi runs bare-metal via setup.sh/systemd.
FROM python:3.13-slim

WORKDIR /app

COPY ingest/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingest/ ingest/
COPY tools/ tools/
COPY platform/ platform/
