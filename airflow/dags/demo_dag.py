import pendulum
from datetime import datetime
import re
import requests
import sqlite3
from pickle import load, dump
from bs4 import BeautifulSoup
from wikipediaapi import Wikipedia
import pandas as pd
from pandas_profiling import ProfileReport
from airflow.decorators import dag, task

ALTERNATE_CURATION = False


def scrape(url: str) -> str:
    try:
        page = requests.get(url)
        print(f"Response recieved from {url} with status code: {page.status_code}")
    except Exception as e:
        print(f'Request to {url} failed with exception "{e}"')
        raise e

    return page.text


def get_text(html: BeautifulSoup, tag: str, attributes: dict) -> str:
    return html.find(name=tag, attrs=attributes).text


def get_link(html: BeautifulSoup) -> str:
    return html.find("a", href=True)["href"]


def save_to_db(table: str, values: list):
    conn = sqlite3.connect("company_report.db")

    value_bindings = ", ".join(["?"] * len(values))
    insert_statement = f"INSERT INTO {table} VALUES ({value_bindings});"

    try:
        conn.cursor().execute(insert_statement, values)
        conn.commit()
    except Exception as e:
        print(f'Insertion to database failed with error "{e}"')

    conn.close()


@dag(
    schedule_interval=None,
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
    def scrape_yahoo_finance(company_ticker: str, execution_date: datetime = None) -> str:
        """
        #### Scrape Yahoo Finance
        Scrape Yahoo Finance for a given company and save the html to the local directory
        """
        yahoo_finance_html = scrape(
            url=f"https://finance.yahoo.com/quote/{company_ticker}"
        )
        with open(f"files/yahoo_finance/{execution_date}.html", "w") as file:
            file.write(yahoo_finance_html)


    @task(retries=3, retry_exponential_backoff=True)
    def scrape_bing_news(company_name: str, execution_date: datetime = None):
        """
        #### Scrape Bing News
        Get the Bing News page for a given company and save the page to the local directory
        """
        query_string = company_name.replace(" ", "+")
        bing_news_html = scrape(
            url=f"https://www.bing.com/news/search?q={query_string}"
        )
        with open(f"files/bing_news/{execution_date}.html", "w") as file:
            file.write(bing_news_html)


    @task(retries=3, retry_exponential_backoff=True)
    def hit_wikipedia_api(company_name: str, execution_date: datetime = None):
        """
        #### Hit Wikipedia API
        Get the Wikipedia page for a given company and save the page to the local directory
        """
        wiki_page = Wikipedia("en").page(company_name)
        dump(wiki_page, open(f"files/wikipedia/{execution_date}.obj", "wb"))

    # [END Ingestion Steps]

    # [START Curation Steps]

    @task()
    def curate_yahoo_finance(execution_date: datetime = None):
        """
        #### Curate Yahoo Finance HTML
        Pull out the market ticker price, save to database in table "ticker_price"

        Alternate curation also extracts and saves volume, average volume, and ESG score information in tables "volume" and "esg_risk_score"
        """
        yahoo_finance_html = open(f"files/yahoo_finance/{execution_date}.html", "r").read()
        html = BeautifulSoup(yahoo_finance_html, "html.parser")
        market_price = get_text(
            html,
            tag="fin-streamer",
            attributes={
                "data-symbol": company_ticker,
                "data-field": "regularMarketPrice",
            },
        )

        save_to_db("ticker_price", [execution_date, market_price])

        if ALTERNATE_CURATION:
            # a high volume ratio can indicate euphoria or fear while the opposite can mean apathy or disinterest
            volume = get_text(
                html,
                tag="fin-streamer",
                attributes={
                    "data-symbol": company_ticker,
                    "data-field": "regularMarketVolume",
                },
            )
            avg_volume = get_text(
                html, tag="td", attributes={"data-test": "AVERAGE_VOLUME_3MONTH-value"}
            )

            save_to_db("volume", [execution_date, volume, avg_volume])

            # esg score
            ESG_score_component = html.find(
                name="div", attrs={"data-yaft-module": "tdv2-applet-miniESGScore"}
            )
            text = ESG_score_component.find_all(string=re.compile(r".*"))
            save_to_db("esg_risk_score", [execution_date] + text[1:])

        return market_price

    @task()
    def curate_bing_news(execution_date: datetime = None):
        """
        #### Curate Bing News HTML
        Parse out headlines, blurbs, and source links for each article, save to database in table "news_articles"
        """
        bing_news_html = open(f"files/bing_news/{execution_date}.html", "r").read()
        parser = BeautifulSoup(bing_news_html, "html.parser")
        bing_news_article_cards = parser.find_all(
            name="div", attrs={"class": "news-card-body card-with-cluster"}
        )

        def extract_article_info(article_card):
            headline = get_text(article_card, tag="div", attributes={"class": "t_t"})
            blurb = get_text(article_card, tag="div", attributes={"class": "snippet"})
            source_link = get_link(article_card)

            return headline, blurb, source_link

        parsed_articles = list(map(extract_article_info, bing_news_article_cards))

        # demonstrate upsert -> "ON CONFLICT(headline) DO ..."
        for headline, blurb, link in parsed_articles:
            save_to_db("news_articles", (headline, blurb, link))

        return parsed_articles

    @task()
    def curate_wikipedia(execution_date: datetime = None):
        """
        #### Curate Wikipedia Page
        Save Wikipedia page to database in table "wikipedia"
        """
        wiki_page = load(open(f"files/wikipedia/{execution_date}.obj", "rb"))
        section_titles = [section.title for section in wiki_page.sections]
        save_to_db(
            "wikipedia",
            [
                wiki_page.title,
                wiki_page.fullurl,
                wiki_page.summary,
                section_titles,
                wiki_page.sections,
                wiki_page.text,
            ],
        )

        return wiki_page.summary, wiki_page.fullurl

    # [END Curation Steps]

    # [START Data Quality Monitoring Steps]

    @task()
    def profile_data():
        # https://medium.com/analytics-vidhya/pandas-profiling-5ecd0b977ecd
        conn = sqlite3.connect("company_report.db")
        df = pd.read_sql_query("SELECT * FROM news_articles", conn)
        profile = ProfileReport(df, title="News Articles Data Profile", minimal=True)
        profile.to_file("news_articles_data_profile.html")

    # TODO: great expectations

    # [END Data Quality Monitoring Steps]

    # [START Report Generation Step]

    @task()
    def generate_report():
        """
        #### Generate a report
        """
        print("hey")
        # print(ticker_prices)
        # print("Summary: \n", wiki_page.summary, "\n")
        # print("Further reading: \n", wiki_page.fullurl)
        # print(bing_articles[0])

    # [END Report Generation Step]

    company_ticker = "TROW"
    company_name = "T. Rowe Price"

    [
        scrape_yahoo_finance(company_ticker) >> curate_yahoo_finance(),
        scrape_bing_news(company_name) >> curate_bing_news(),
        hit_wikipedia_api(company_name) >> curate_wikipedia()
    ] >> profile_data() >> generate_report()


# invoke DAG
demo_dag = demo_pipeline()
