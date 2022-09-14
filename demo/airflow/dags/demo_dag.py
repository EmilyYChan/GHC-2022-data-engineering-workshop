import pendulum
from datetime import datetime
from typing import Tuple, List
import re
import requests
import json
import sqlite3
from pickle import load, dump
from bs4 import BeautifulSoup
from wikipediaapi import Wikipedia
import pandas as pd
import matplotlib.pyplot as plt
from pandas_profiling import ProfileReport
from airflow.decorators import dag, task
from great_expectations import DataContext



@dag(
    schedule_interval='*/2 * * * *',  # run every 2 min
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
)
def demo_pipeline():
    """
    ### GHC 2022 Data Engineering Workshop Demo Pipeline
    This is a simple ETL data pipeline which demonstrates ingesting data from
    different sources, monitoring data quality, and generating a report.
    """

    # [START Ingestion Steps]

    @task(retries=3, retry_exponential_backoff=True)
    def get_data_from_yahoo_finance(company_ticker: str, execution_date: datetime = None):
        # scrape the company's yahoo finance page
        yahoo_finance_html = scrape(
            url=f"https://finance.yahoo.com/quote/{company_ticker}"
        )

        # save yahoo finance page to local directory as html
        with open(f"demo/files/yahoo_finance/{execution_date}.html", "w") as file:
            file.write(yahoo_finance_html)


    @task(retries=3, retry_exponential_backoff=True)
    def get_data_from_bing_news(company_name: str, execution_date: datetime = None):
        # scrape bing news with company name as the search query
        query_string = company_name.replace(" ", "+")
        bing_news_html = scrape(
            url=f"https://www.bing.com/news/search?q={query_string}"
        )

        # save search results page to local directory as html
        with open(f"demo/files/bing_news/{execution_date}.html", "w") as file:
            file.write(bing_news_html)


    @task(retries=3, retry_exponential_backoff=True)
    def get_data_from_wikipedia(company_name: str, execution_date: datetime = None):
        # get company wiki page using wikipedia api library
        wiki_page = Wikipedia("en").page(company_name)

        # save wiki page to local directory as a pickle object
        dump(wiki_page, open(f"demo/files/wikipedia/{execution_date}.obj", "wb"))

    # [END Ingestion Steps]

    # [START Curation Steps]

    @task()
    def clean_yahoo_finance_data(company_ticker: str, execution_date: datetime = None):
        yahoo_finance_html = open(f"demo/files/yahoo_finance/{execution_date}.html", "r").read()

        market_price = get_yahoo_finance_market_price(yahoo_finance_html, company_ticker)
        save_to_db(table="stock_price", values=[market_price, execution_date])

        # alternate curation
        volume = get_yahoo_finance_volume(yahoo_finance_html, company_ticker)
        avg_volume = get_yahoo_finance_avg_volume(yahoo_finance_html)
        save_to_db(table="volume", values=[volume, avg_volume, execution_date])

        esg_info = get_yahoo_finance_esg_info(yahoo_finance_html)
        save_to_db(table="esg", values=(*esg_info, execution_date))


    @task()
    def clean_bing_news_data(execution_date: datetime = None):
        bing_news_html = open(f"demo/files/bing_news/{execution_date}.html", "r").read()

        # parse out headlines, blurbs, and source links for each article
        parsed_articles = parse_bing_news_articles(bing_news_html)

        for headline, blurb, link in parsed_articles:
            save_to_db(table="news", values=(headline, blurb, link, execution_date))


    @task()
    def clean_wikipedia_data(execution_date: datetime = None):
        """
        #### Curate Wikipedia Page
        Save Wikipedia page to database in table "wikipedia"
        """
        wiki_page = load(open(f"demo/files/wikipedia/{execution_date}.obj", "rb"))
        section_titles = json.dumps([section.title for section in wiki_page.sections])
        save_to_db(
            table="wikipedia",
            values=[
                wiki_page.title,
                wiki_page.fullurl,
                wiki_page.summary,
                section_titles,
                wiki_page.text,
                execution_date,
            ],
        )

    # [END Curation Steps]

    # [START Data Quality Monitoring Steps]

    @task()
    def run_data_quality_tests():
        data_context = DataContext("/Users/trpij38/Documents/GHC-talk/demo/great_expectations")
        data_context.run_checkpoint(checkpoint_name="checkpoint")
        print(data_context.build_data_docs())

    # [END Data Quality Monitoring Steps]

    # [START Report Generation Step]

    @task()
    def generate_report():
        # title - Company Name
        # wikipedia summary
        wiki_summary = get_wikipedia_summary_from_db()
        wiki_url = get_wikipedia_url_from_db()
        print(wiki_summary, wiki_url)

    # [END Report Generation Step]

    company_ticker = "TROW"
    company_name = "T. Rowe Price"

    [
        get_data_from_yahoo_finance(company_ticker) >> clean_yahoo_finance_data(company_ticker),
        get_data_from_bing_news(company_name) >> clean_bing_news_data(),
        get_data_from_wikipedia(company_name) >> clean_wikipedia_data()
    ] >> run_data_quality_tests() >> generate_report() 



def parse_bing_news_articles(html: str) -> List[Tuple[str, str, str]]: 
    html_parser = BeautifulSoup(html, "html.parser")

    bing_news_article_cards = html_parser.find_all(
        name="div", attrs={"class": "news-card-body card-with-cluster"}
    )

    def extract_article_info(article_card):
        headline = get_text(article_card, tag="div", attributes={"class": "t_t"})
        blurb = get_text(article_card, tag="div", attributes={"class": "snippet"})
        source_link = get_link(article_card)

        return headline, blurb, source_link

    return list(map(extract_article_info, bing_news_article_cards))


def get_yahoo_finance_market_price(html: str, company_ticker: str) -> float:
    html_parser = BeautifulSoup(html, "html.parser")
    return float(get_text(
            html_parser,
            tag="fin-streamer",
            attributes={
                "data-symbol": company_ticker,
                "data-field": "regularMarketPrice",
            },
        ))


def get_yahoo_finance_volume(html: str, company_ticker: str) -> int:  # TODO: do we need company ticker?
    html_parser = BeautifulSoup(html, "html.parser")
    return int(get_text(
            html_parser,
            tag="fin-streamer",
            attributes={
                "data-symbol": company_ticker,
                "data-field": "regularMarketVolume",
            },
        ).replace(',', ''))


def get_yahoo_finance_avg_volume(html: str) -> int:
    html_parser = BeautifulSoup(html, "html.parser")
    return int(get_text(
            html_parser, tag="td", attributes={"data-test": "AVERAGE_VOLUME_3MONTH-value"}
        ).replace(',', ''))


def get_yahoo_finance_esg_info(html: str) -> Tuple[float, str, int]:
    html_parser = BeautifulSoup(html, "html.parser")
    ESG_html = html_parser.find(
                name="div", attrs={"data-yaft-module": "tdv2-applet-miniESGScore"}
            )
    _, risk_score, risk_level, percentile = ESG_html.find_all(string=re.compile(r".*"))
    risk_score = float(risk_score)
    percentile = int(re.sub("\D","", percentile))
    return risk_score, risk_level, percentile


def scrape(url: str) -> str:
    try:
        page = requests.get(url)
        print(f"Response recieved from {url} with status code: {page.status_code}")
    except Exception as e:
        print(f'Request to {url} failed with exception "{e}"')
        raise e

    return page.text


def get_text(html_parser: BeautifulSoup, tag: str, attributes: dict) -> str:
    return html_parser.find(name=tag, attrs=attributes).text


def get_link(html_parser: BeautifulSoup) -> str:
    return html_parser.find("a", href=True)["href"]


def save_to_db(table: str, values: list):
    conn = sqlite3.connect("demo/company_info.db")

    value_bindings = ", ".join(["?"] * len(values))
    insert_statement = f"INSERT OR IGNORE INTO {table} VALUES ({value_bindings});"

    try:
        conn.cursor().execute(insert_statement, values)
        conn.commit()
    except Exception as e:
        print(f'Insertion to database failed with error "{e}"')

    conn.close()


def get_wikipedia_summary_from_db() -> str:
    conn = sqlite3.connect("demo/company_info.db")
    # SELECT * FROM Table ORDER BY ID DESC LIMIT 1
    query = f"SELECT summary FROM wikipedia WHERE scrape_date=(SELECT max(scrape_date) FROM wikipedia);"
    result = pd.read_sql_query(query, conn)
    conn.close()
    return result.at[0, 'summary']


def get_wikipedia_url_from_db() -> str:
    conn = sqlite3.connect("demo/company_info.db")
    # SELECT * FROM Table ORDER BY ID DESC LIMIT 1
    query = f"SELECT url FROM wikipedia WHERE scrape_date=(SELECT max(scrape_date) FROM wikipedia);"
    result = pd.read_sql_query(query, conn)
    conn.close()
    return result.at[0, 'url']


def get_esg_info_from_db() -> Tuple[float, str, int]:
    conn = sqlite3.connect("demo/company_info.db")
    query = f"SELECT * FROM esg WHERE scrape_date=(SELECT max(scrape_date) FROM esg);"
    result = pd.read_sql_query(query, conn)
    conn.close()
    return result.itertuples(index=False, name=None)[0]


def get_all_data_from_table(table: str) -> pd.DataFrame:
    conn = sqlite3.connect("demo/company_info.db")
    query = f"SELECT * FROM {table}"
    result = pd.read_sql_query(query, conn)
    conn.close()
    return result


def get_data_from_column(table: str, column: str) -> pd.DataFrame:
    conn = sqlite3.connect("demo/company_info.db")
    query = f"SELECT {column} FROM {table}"
    result = pd.read_sql_query(query, conn)
    conn.close()
    return result

# def plot_timestamps(data):
#     valid_times = get_timestamps(get_valid_pages(data))
#     invalid_times = get_timestamps(get_invalid_pages(data))
#     date = min(valid_times) if valid_times else min(invalid_times)
    
#     f = plt.figure()
#     f.set_figwidth(15)
#     f.set_figheight(10)

#     if valid_times:
#         plt.hist(valid_times, bins=20, alpha=0.5, color="skyblue", label='Valid Pages')
#     if invalid_times:
#         plt.hist(invalid_times, bins=20, alpha=0.5, color="orange", label='Cloudflare Protected Pages')
#     plt.legend(loc='upper right')
#     plt.xlabel("Timestamp (Date and Hour)")
#     plt.ylabel("Number of Scraped Pages")
#     plt.title(f"Valid vs Invalid Pages Over Time for {date.month}/{date.day} (based on last modifed timestamp in AWS S3)")
#     plt.show()


# invoke DAG
demo_dag = demo_pipeline()
