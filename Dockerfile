FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Non-root, with the same uid/gid as the Airflow image (50000:0) so both can
# write the shared `models` volume, which inherits /app/models' ownership.
RUN useradd --uid 50000 --gid 0 --create-home app \
    && mkdir -p /app/models && chown app:0 /app/models
USER app

COPY src ./src
COPY scripts ./scripts
