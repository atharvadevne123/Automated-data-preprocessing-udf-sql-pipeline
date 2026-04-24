# Automated Data Preprocessing & SQL-Based UDF Integration

[![CI](https://github.com/atharvadevne123/Automated-data-preprocessing-udf-sql-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/atharvadevne123/Automated-data-preprocessing-udf-sql-pipeline/actions/workflows/ci.yml)

An end-to-end data analytics project built around the Yelp Open Dataset, performing sentiment analysis and general data analysis using Python, Snowflake SQL UDFs, and AWS S3.

---

## Features

- **Large-file splitter** — split 5 GB+ newline-delimited JSON into N chunks via CLI, with full error handling and a fix for the off-by-one remainder bug
- **Snowflake Python UDF** — `analyze_sentiment()` using TextBlob; returns Positive / Neutral / Negative
- **Flattened analytical tables** — `tbl_yelp_reviews` and `tbl_yelp_businesses` ready for SQL analytics
- **Automated tests** — 8-test pytest suite covering edge cases (remainder lines, empty file, invalid args, JSON integrity)
- **GitHub Actions CI** — lint (ruff) + test on every push/PR
- **Secure credential handling** — no credentials in code; `.env.example` provided

---

## Project Structure

```
├── split_files.py              # CLI tool: split large JSON files
├── UDF and tables.sql          # Snowflake UDFs and table DDL
├── tests/
│   └── test_split_files.py     # pytest suite (8 tests)
├── .github/workflows/ci.yml    # GitHub Actions CI
├── .env.example                # Template for environment variables
├── requirements.txt            # Python dependencies
└── README.md
```

---

## Setup

### 1. Clone

```bash
git clone https://github.com/atharvadevne123/Automated-data-preprocessing-udf-sql-pipeline.git
cd Automated-data-preprocessing-udf-sql-pipeline
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure environment variables

```bash
cp .env.example .env
# Edit .env with your Snowflake and AWS credentials
```

### 4. Split a large JSON file

```bash
# Basic usage (defaults: 10 output files, prefix split_file_)
python split_files.py yelp_academic_dataset_review.json

# Custom options
python split_files.py yelp_academic_dataset_review.json \
    --num-files 20 \
    --output-prefix chunks/review_chunk_
```

### 5. Snowflake SQL setup

Open a Snowflake SQL worksheet and execute `UDF and tables.sql`.  
Replace the `$AWS_KEY_ID` / `$AWS_SECRET_KEY` placeholders with your actual values, or configure a [Snowflake storage integration](https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration) to avoid inline credentials.

---

## Running Tests

```bash
pytest -v --tb=short
```

Expected output: **8 passed**.

---

## Technologies

| Tool | Purpose |
|------|---------|
| Python 3.x | File splitting CLI |
| Snowflake SQL | Data warehouse, UDF runtime |
| Amazon S3 | Raw data staging |
| TextBlob | Sentiment analysis |
| pytest | Unit testing |
| ruff | Linting |
| GitHub Actions | CI/CD |

---

## Dataset

Download from the official [Yelp Open Dataset](https://business.yelp.com/data/resources/open-dataset/) page.

---

## Example Analyses

- Top 10 users by restaurant review count
- Most popular business categories
- Top 3 recent reviews per business
- Sentiment distribution across cities
- % of 5-star reviews per business
- Month-wise review trends
- Top 10 businesses by positive sentiment

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier |
| `SNOWFLAKE_USER` | Snowflake username |
| `SNOWFLAKE_PASSWORD` | Snowflake password |
| `SNOWFLAKE_WAREHOUSE` | Compute warehouse name |
| `SNOWFLAKE_DATABASE` | Target database |
| `SNOWFLAKE_SCHEMA` | Target schema (default: PUBLIC) |
| `AWS_KEY_ID` | AWS access key for S3 |
| `AWS_SECRET_KEY` | AWS secret key for S3 |
| `S3_BUCKET` | S3 bucket name |
| `S3_PREFIX` | S3 key prefix (default: yelp/) |

---

## Author

**Atharva Devne**
