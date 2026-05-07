# ---- build stage ----
FROM python:3.11-slim AS builder

WORKDIR /build

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ---- runtime stage ----
FROM python:3.11-slim AS runtime

LABEL org.opencontainers.image.title="yelp-pipeline"
LABEL org.opencontainers.image.description="Automated JSONL splitter and Snowflake loader"
LABEL org.opencontainers.image.source="https://github.com/atharvadevne123/Automated-data-preprocessing-udf-sql-pipeline"

WORKDIR /app

COPY --from=builder /install /usr/local
COPY . .

RUN mkdir -p /app/data /app/output

ENV LOG_LEVEL=INFO

ENTRYPOINT ["python", "split_files.py"]
CMD ["--help"]
