# Automated-data-preprocessing-udf-sql-pipeline

📊 Automated Data Preprocessing and SQL-Based UDF Integration for Scalable Data Pipelines

📘 Project Description

This is an end-to-end data analytics project built around the Yelp Open Dataset, performing both sentiment analysis and general data analysis. The goal is to demonstrate how to build a scalable data pipeline using real-world JSON data, cloud storage, Python-based file preprocessing, Snowflake as a data warehouse, and advanced SQL for analytics.

This project also includes a reusable data preprocessing and UDF integration pipeline that can be extended to other datasets and platforms.

⸻

## 🔄 Project Workflow Summary

- Download and preprocess the Yelp JSON dataset (5GB+ with 7M+ reviews)
- Use Python to split large files into manageable chunks
- Upload chunks to AWS S3 and ingest into Snowflake using SQL `COPY INTO`
- Flatten JSON into relational tables using Snowflake SQL
- Perform sentiment analysis via Python UDF integrated within Snowflake
- Execute SQL-based analytical queries to derive business insights

⸻

## 📂 Project Structure


````plaintext
├── split_files.py            # Python script for splitting large files into manageable chunks
├── UDF and tables.sql        # SQL script for creating UDFs and required tables
├── yelp_data_pipeline.drawio # Editable system architecture diagram
├── yelp_data_pipeline.png    # Visual overview of the end-to-end pipeline
├── README.md                 # Project overview and usage instructions
````

⸻

## 🧰 Technologies Used

- **Python 3.x**
- **Snowflake SQL**
- **Amazon S3**
- **TextBlob** (Python library for sentiment analysis)
- **Draw.io** (for architecture diagram)
- **Pandas** (optional, for file manipulation)
⸻

🛠️ Setup Instructions

1. Clone the Repository
```
git clone https://github.com/atharvadevne123/Automated-data-preprocessing-udf-sql-pipeline.git
cd automated-data-pipeline-udf
```
2. Python Environment Setup

Ensure Python 3.x is installed. You can install dependencies using:
```
pip install -r requirements.txt
```
If requirements.txt is not present, install manually:
```
pip install pandas textblob
```
3. Run the File Splitter
```
python split_files.py
```
Make sure to update split_files.py with the correct path to your large input file.

4. SQL Setup

Connect to Snowflake and execute the provided SQL file:
```
-- Inside Snowflake SQL worksheet
\i 'UDF and tables.sql'
```
This creates necessary tables and registers UDFs for sentiment classification.

⸻

## 🧾 Dataset

You can download the dataset used in this project from Yelp’s official page:

🔗 [Yelp Open Dataset](https://business.yelp.com/data/resources/open-dataset/)

⸻

## 💡 Example Analysis Performed

- Top 10 users with most restaurant reviews
- Most popular business categories
- Top 3 recent reviews per business
- Sentiment distribution across cities
- % of 5-star reviews per business
- Month-wise review trends
- Top 10 businesses by positive sentiment

  
⸻

## 📌 Notes

- Ensure that your Snowflake account has access to create UDFs.
- Modify script paths and SQL table references as per your environment.
- You can optionally bypass AWS S3 and directly upload files into Snowflake.

  
⸻

## ✅ Future Improvements

- Add unit tests for Python logic
- Include support for Google Cloud / Azure Storage
- Automate UDF deployment with CI/CD
- Explore BERT-based sentiment scoring

⸻

## 👨‍💻 Author

**Atharva Devne**  

