# GHC 2022 9/23: Demystifying Data Engineering

## Goals
The goal of this workshop is to demystify data engineering and demonstrate some of the important concepts and principles of data engineering via a toy data pipeline.

Most data engineering workflows make use of cloud computing services and
cloud data warehouses such as AWS, GCP, Azure, and Snowflake, but this demo
won't touch on those, since it's set up to run locally.

This repo contains a pipeline that scrapes the ticker price of a company from Yahoo Finance, recent articles about the company from Bing News, along with information about the company from Wikipedia, and generate a simple report.

## Setup

1. clone the repo
2. change paths in airflow/airflow.cfg to where ever you've chosen to clone the repo
3. activate the venv
4. run airflow standalone
5. record the password shown in the terminal
6. access the UI at localhost:8080 with username admin and password from step 5

### Running

Access the UI at localhost: 8080
Turn the Dag on

## Background



## Principles


## Resources / Documentation
Airflow Local Setup: https://airflow.apache.org/docs/apache-airflow/stable/start/local.html
Airflow DAGs: https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html
Airflow Architecture: https://airflow.apache.org/docs/apache-airflow/stable/concepts/overview.html

Streamlit Setup: https://docs.streamlit.io/library/get-started/installation
Streamlit Usage Guide: https://docs.streamlit.io/library/get-started/main-concepts

## Troubleshooting
export AIRFLOW_HOME
change all paths 