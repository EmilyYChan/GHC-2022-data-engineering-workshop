# GHC 2022: Demystifying Data Engineering

## Goals
The goal of this workshop is to demystify data engineering and provide a simple
demonstration of some of the tools and tech stack. These include:
- Airflow: pipeline orchestration

Most data engineering workflows also make use of cloud computing services and
cloud data warehouses such as AWS, GCP, Azure, and Snowflake, but this demo
won't touch on those, since it's set up to run locally.

This repo contains a simple pipeline that scrapes data from the front pages of
Twitter, Reddit, Hackernews, etc., stores them in a local Postgres (SQLite?)
database, searches the articles for keywords you're interested in (crypto, rust,
Elon Musk, etc.), and then generates a daily newsletter that prominently features
any news that matches your interests.

Alternative idea if the above is too slow locally: a simple pipeline that runs
every 2 minute to grab weather data, at the end we plot weather over time.

## Setup

install requirements using pip install -r 

### Running

Access the UI at localhost: 
Turn the Dag on

## Background 

What is a DAG

Principles





