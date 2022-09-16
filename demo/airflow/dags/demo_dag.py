import pendulum
from datetime import datetime
from typing import Tuple, List
from pickle import load, dump
import pandas as pd
# from pandas_profiling import ProfileReport
from airflow.decorators import dag, task




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

    # [START Ingestion Tasks]

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
        from wikipediaapi import Wikipedia

        # get company wiki page via wikipedia api library
        wiki_page = Wikipedia("en").page(company_name)

        # save wiki page to local directory as a pickle object
        dump(wiki_page, open(f"demo/files/wikipedia/{execution_date}.obj", "wb"))

    # [END Ingestion Tasks]

    # [START Cleaning Tasks]

    @task()
    def clean_yahoo_finance_data(company_ticker: str, execution_date: datetime = None):
        yahoo_finance_html = open(f"demo/files/yahoo_finance/{execution_date}.html", "r").read()

        market_price = get_yahoo_finance_market_price(yahoo_finance_html, company_ticker)
        save_to_db(table="stock_price", values=[market_price, execution_date])

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
        wiki_page = load(open(f"demo/files/wikipedia/{execution_date}.obj", "rb"))
        save_to_db(
            table="wikipedia",
            values=[
                wiki_page.title,
                wiki_page.fullurl,
                wiki_page.summary,
                wiki_page.text,
                execution_date,
            ],
        )

    # [END Cleaning Tasks]

    # [START Data Quality Monitoring Tasks]

    @task()
    def test_yahoo_finance_data():
        run_data_quality_tests("yahoo_finance")

    @task()
    def test_bing_news_data():
        run_data_quality_tests("bing_news")

    @task()
    def test_wikipedia_data():
        run_data_quality_tests("bing_news")

    @task()
    def test_curated_data():
        run_data_quality_tests("all_curated_data")

    # [END Data Quality Monitoring Tasks]

    # [START Curation Task]

    @task()
    def curate_data():
        # calculate percent change for price over the day
        pass


    # [END Curation Task]

    company_ticker = "TROW"
    company_name = "T. Rowe Price"

    [
        get_data_from_yahoo_finance(company_ticker) >> clean_yahoo_finance_data(company_ticker) >> test_yahoo_finance_data(),
        get_data_from_bing_news(company_name) >> clean_bing_news_data() >> test_bing_news_data(),
        get_data_from_wikipedia(company_name) >> clean_wikipedia_data() >> test_wikipedia_data(),
    ] >> curate_data() >> test_curated_data()



def parse_bing_news_articles(html: str) -> List[Tuple[str, str, str]]: 
    from bs4 import BeautifulSoup

    html_parser = BeautifulSoup(html, "html.parser")

    bing_news_article_cards = html_parser.find_all(
        name="div", attrs={"class": "news-card-body card-with-cluster"}
    )

    def extract_article_info(article_card):
        headline = article_card.find("div", attrs={"class": "t_t"}).text
        blurb = article_card.find("div", attrs={"class": "snippet"}).text
        source_link = article_card.find("a", href=True)["href"]

        return headline, blurb, source_link

    return list(map(extract_article_info, bing_news_article_cards))


def get_yahoo_finance_market_price(html: str, company_ticker: str) -> float:
    from bs4 import BeautifulSoup

    html_parser = BeautifulSoup(html, "html.parser")
    return float(
            html_parser.find(
            "fin-streamer",
            attrs={
                "data-symbol": company_ticker,
                "data-field": "regularMarketPrice",
            }).text
        )


def get_yahoo_finance_volume(html: str, company_ticker: str) -> int:  # TODO: do we need company ticker?
    from bs4 import BeautifulSoup

    html_parser = BeautifulSoup(html, "html.parser")
    return int(
            html_parser.find(
            "fin-streamer",
            attrs={
                "data-symbol": company_ticker,
                "data-field": "regularMarketVolume",
            },
        ).text.replace(',', ''))


def get_yahoo_finance_avg_volume(html: str) -> int:
    from bs4 import BeautifulSoup

    html_parser = BeautifulSoup(html, "html.parser")
    return int(
            html_parser.find("td", attrs={"data-test": "AVERAGE_VOLUME_3MONTH-value"}
        ).text.replace(',', ''))


def get_yahoo_finance_esg_info(html: str) -> Tuple[float, str, int]:
    from bs4 import BeautifulSoup
    import re

    html_parser = BeautifulSoup(html, "html.parser")
    ESG_html = html_parser.find(
                name="div", attrs={"data-yaft-module": "tdv2-applet-miniESGScore"}
            )
    _, risk_score, risk_level, percentile = ESG_html.find_all(string=re.compile(r".*"))
    risk_score = float(risk_score)
    percentile = int(re.sub("\D","", percentile))
    return risk_score, risk_level, percentile


def scrape(url: str) -> str:
    import requests
    
    try:
        page = requests.get(url)
        print(f"Response recieved from {url} with status code: {page.status_code}")
    except Exception as e:
        print(f'Request to {url} failed with exception "{e}"')
        raise e

    return page.text


def save_to_db(table: str, values: list):
    import sqlite3

    conn = sqlite3.connect("demo/company_info.db")

    value_bindings = ", ".join(["?"] * len(values))
    insert_statement = f"INSERT OR IGNORE INTO {table} VALUES ({value_bindings});"

    try:
        conn.cursor().execute(insert_statement, values)
        conn.commit()
    except Exception as e:
        print(f'Insertion to database failed with error "{e}"')

    conn.close()


def get_from_db(query: str) -> pd.DataFrame:
    import sqlite3

    conn = sqlite3.connect("demo/company_info.db")
    result = pd.read_sql_query(query, conn)
    conn.close()
    return result


def run_data_quality_tests(test_suite: str):
    from great_expectations import DataContext

    data_context = DataContext("/Users/trpij38/Documents/GHC-talk/demo/great_expectations")
    data_context.run_checkpoint(checkpoint_name=test_suite)
    print(data_context.build_data_docs())

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
