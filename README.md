# Twitter Airflow Data Engineering Project

## Project Overview

This project demonstrates how to set up an ETL (Extract, Transform, Load) process using Apache Airflow to extract tweets from Twitter, transform the data, and load it into a storage system. The project utilizes various AWS services and Python libraries to automate and streamline the data engineering workflow.

## Airflow DAG Setup

The DAG runs the ETL process daily. It uses the PythonOperator to call the `run_twitter_etl` function.

## Twitter ETL Function (`run_twitter_etl`)

- Authenticates to Twitter using Tweepy.
- Fetches up to 200 tweets from users timeline.
- Refines the tweet data by selecting specific fields.
- Saves the refined data as a CSV file.

## Services Used
- **Apache Airflow** for workflow automation and scheduling.
- **Tweepy** for accessing the Twitter API.
- **Pandas** for data manipulation and transformation.
- **AWS S3** for storage.
- **AWS IAM** for secure access management.

## Project Goals
- **Data Ingestion** — Build a mechanism to ingest data from the Twitter API.
- **ETL System** — Transform raw tweet data into a structured format.
- **Data Storage** — Store the transformed data in a reliable storage system.
- **Scalability** — Ensure the system can scale as data volume increases.
- **Automation** — Automate the entire ETL process using Airflow.
- **Reporting** — Build a dashboard to visualize the data and derive insights.


![Twitter Airflow Data Engineering Project](https://github.com/dengineer2104/Twitter_ETL/blob/main/Airflow_Twitter.jpg)

