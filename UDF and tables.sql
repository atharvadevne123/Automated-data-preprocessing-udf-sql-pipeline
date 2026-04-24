-- ============================================================
-- Yelp Data Pipeline — UDFs and Tables
-- Credentials are loaded from environment variables.
-- Set AWS_KEY_ID and AWS_SECRET_KEY before running COPY INTO.
-- ============================================================

-- Sentiment-analysis UDF (Snowflake Python UDF, runtime 3.8)
CREATE OR REPLACE FUNCTION analyze_sentiment(text STRING)
    RETURNS STRING
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.8'
    PACKAGES = ('textblob')
    HANDLER = 'sentiment_analyzer'
AS $$
from textblob import TextBlob

def sentiment_analyzer(text):
    analysis = TextBlob(text)
    if analysis.sentiment.polarity > 0:
        return 'Positive'
    elif analysis.sentiment.polarity == 0:
        return 'Neutral'
    else:
        return 'Negative'
$$;


-- ============================================================
-- Reviews
-- ============================================================

-- Stage table: raw variant JSON
CREATE OR REPLACE TABLE yelp_reviews (review_text VARIANT);

-- Load from S3 — replace $AWS_KEY_ID / $AWS_SECRET_KEY with actual values
-- or use a Snowflake storage integration to avoid inline credentials.
COPY INTO yelp_reviews
FROM 's3://namastesql/yelp/'
CREDENTIALS = (
    AWS_KEY_ID     = '$AWS_KEY_ID'
    AWS_SECRET_KEY = '$AWS_SECRET_KEY'
)
FILE_FORMAT = (TYPE = JSON);

-- Flattened reviews table with sentiment
CREATE OR REPLACE TABLE tbl_yelp_reviews AS
SELECT
    review_text:business_id::STRING  AS business_id,
    review_text:date::DATE           AS review_date,
    review_text:user_id::STRING      AS user_id,
    review_text:stars::NUMBER        AS review_stars,
    review_text:text::STRING         AS review_text,
    analyze_sentiment(review_text:text::STRING) AS sentiments
FROM yelp_reviews;


-- ============================================================
-- Businesses
-- ============================================================

-- Stage table: raw variant JSON
CREATE OR REPLACE TABLE yelp_businesses (business_text VARIANT);

COPY INTO yelp_businesses
FROM 's3://namastesql/yelp/yelp_academic_dataset_business.json'
CREDENTIALS = (
    AWS_KEY_ID     = '$AWS_KEY_ID'
    AWS_SECRET_KEY = '$AWS_SECRET_KEY'
)
FILE_FORMAT = (TYPE = JSON);

-- Flattened businesses table
CREATE OR REPLACE TABLE tbl_yelp_businesses AS
SELECT
    business_text:business_id::STRING AS business_id,
    business_text:name::STRING        AS name,
    business_text:city::STRING        AS city,
    business_text:state::STRING       AS state,
    business_text:review_count::INT   AS review_count,
    business_text:stars::NUMBER       AS stars,
    business_text:categories::STRING  AS categories
FROM yelp_businesses
LIMIT 100;
