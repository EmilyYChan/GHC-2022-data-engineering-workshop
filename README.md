# GHC 2022 9/23: Demystifying Data Engineering


## Background
The goal of this workshop is to demystify data engineering and demonstrate some
of the important concepts and principles of data engineering via a toy data pipeline.

This repo contains a pipeline that scrapes the ticker price of a company from Yahoo Finance, recent articles about the company from Bing News, and information about the company from Wikipedia.

[screenshot of DAG]

We visualize the data that we've acquired using a simple Streamlit app.

[screenshot of app]

Most data engineering workflows make use of cloud computing services and cloud data
warehouses such as AWS, GCP, Azure, and Snowflake, but in order to be accessible and
replicable, this demo is set up to run locally.

### Concepts
- **Unstructured Storage**: \
files
- **Structured Storage**:  \
company db
- **Orchestration**:  \
We use Apache Airflow, an open source tool originally developed at Airbnb, as our orchestration tool. Orchestration means programmatically managing the scheduling of tasks that need to happen in a certain order. 
airflow
DAG

- **Minimizing Data Loss**: \
In order to minimize data loss, we
- **Independence of Runs**: \
Sometimes we may want to rerun the DAG in the past, in order to fix errors or apply updated curation logic to data collected in the past. This is called backfilling. In order to do this, we need to make sure each run is independent of the others and the actual run date. We do this by labelling each piece of data with the execution date of the run that produced it, and having a run only touch the data associated with its execution date.
- **Idempotency**: \
Since tasks can be rerun at any time and run any number of times, we need to ensure that each run is idempotent. This means that no matter how many times the DAG is run, it will have the same effect as having been run once. We do this by associating each piece of data produced by a run with a unique id (which can be the execution date), and having our DAG overwrite previously produced data with the same ids.
- **Data Quality Testing**: \
We use a tool called Great Expectations to create and run our data quality tests. These tests (also called expectations) are manually generated based on domain knowledge of what the data should look like. You can read more about Great Expectations in the documentation linked below.
 great expectations


## Setup
1. **Clone the repo**
2. **Change Airflow configuration paths**  \
The configuration file `demo/airflow/airflow.cfg` contains a number of paths that tell Airflow where to access certain resources. Change any paths that begin with `/Users/trpij38/Documents/GHC-talk` to include the path of your own cloned repo directory, which you can find by typing `pwd` into your terminal.
3. **Create and activate a virtual environment**  \
`python3 -m venv ghc-venv` will create a new virtual environment called `ghc-venv`, and new directory of the same name where libraries will be installed. We can then activate this virtual environment using `source ghc-venv/bin/activate`.
4. **Install requirements**  \
`pip install -r demo/requirements.txt`
5. **Set environment variables**:  \
`export AIRFLOW_HOME=<path_to_your_cloned_repo>/demo/airflow`
6. **Spin up Airflow**  \
`airflow standalone`
7. **Record the password shown in the terminal**
8. **Access the Airflow UI**  \
Visit localhost:8080 with username `admin` and password from the previous step.
9. **Access the Streamlit App**  \
Running `streamlit run demo/company_report.py` will automatically open the application at localhost:8502.


## Resources / Documentation

Airflow Local Setup: https://airflow.apache.org/docs/apache-airflow/stable/start/local.html

Airflow DAGs: https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html

Airflow Architecture: https://airflow.apache.org/docs/apache-airflow/stable/concepts/overview.html

 

Streamlit Setup: https://docs.streamlit.io/library/get-started/installation

Streamlit Usage Guide: https://docs.streamlit.io/library/get-started/main-concepts

 

Great Expectations Documentation: https://docs.greatexpectations.io/docs/

Great Expectations Data Tests: https://greatexpectations.io/expectations

