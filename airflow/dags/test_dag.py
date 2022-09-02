import json
import pendulum
from airflow.decorators import dag, task


@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
)
def test_etl():
    """
    ### GHC 2022 Data Engineering Workshop Demo Pipeline
    This is a simple ETL data pipeline which demonstrates ingesting data from
    different sources, monitoring data quality, and generating a report.
    """

    @task(retries=3, retry_exponential_backoff=True)
    def scrape_yahoo_finance(company_ticker):  # return type
        """
        #### Scrape Yahoo Finance
        Scrape Yahoo Finance for a given company and save the html to the local directory
        """
        # import requests

        # url = f'https://finance.yahoo.com/quote/{company_ticker}'
        # try:
        #     yahoo_finance_page = requests.get(url)
        # except Exception as e:
        #     print(f"Request to {url} failed with exception \"{e}\"")
        #     raise e
        # print("Response recieved from Yahoo Finance")

        # with open('yahoo_finance.html', 'w') as file:
        #     file.write(yahoo_finance_page.text)

        # SIGSEGV workaround
        import os

        print("Current working directory:", os.getcwd())
        yahoo_finance_page = open("yahoo_finance.html", "r").read()

        return yahoo_finance_page

    @task()
    def curate_yahoo_finance_html(yahoo_finance_page):
        """
        #### Curate Yahoo Finance HTML
        Pull out the market price and after hours price
        """
        from bs4 import BeautifulSoup

        parser = BeautifulSoup(yahoo_finance_page, "html.parser")

        market_price = parser.find(
            name="fin-streamer",
            attrs={"data-symbol": company_ticker, "data-field": "regularMarketPrice"},
        )
        after_hours_trading_price = parser.find(
            name="fin-streamer",
            attrs={"data-symbol": company_ticker, "data-field": "postMarketPrice"},
        )
        print(
            "\nMarket price: ",
            market_price.text,
            "\nAfter hours trading price: ",
            after_hours_trading_price.text,
        )

        # TODO: save to sqlite db

        return market_price.text, after_hours_trading_price.text

    @task(retries=3, retry_exponential_backoff=True)
    def hit_wikipedia_api(company_name):
        """
        #### Hit Wikipedia API
        Get the Wikipedia page for a given company and save the page to the local directory
        """
        # from wikipediaapi import Wikipedia

        # try:
        #     wiki_page = Wikipedia('en').page(company_name)
        # except Exception as e:
        #     print(f"Request to Wikipedia API for {company_name} page failed with exception \"{e}\"")
        #     raise e
        # print("Summary: \n", wiki_page.summary, "\n")
        # print("Further reading: \n", wiki_page.fullurl)

        # import pickle
        # pickle.dump(wiki_page, open('wikipedia.obj', 'wb'))

        # SIGSEGV workaround
        import pickle

        wiki_file = open("wikipedia.obj", "rb")
        wiki_page = pickle.load(wiki_file)

        return wiki_page

    @task()
    def hit_news_api():
        """
        #### Extract task
        A simple Extract task to get data ready for the rest of the data
        pipeline.
        """
        pass

    @task()
    def generate_report(wiki_page, ticker_prices):
        """
        #### Generate a report
        """
        print(ticker_prices)
        print("Summary: \n", wiki_page.summary, "\n")
        print("Further reading: \n", wiki_page.fullurl)
        pass

    company_ticker = "TROW"
    company_name = "T. Rowe Price"

    yahoo_finance_page = scrape_yahoo_finance(company_ticker)
    ticker_prices = curate_yahoo_finance_html(yahoo_finance_page)
    wiki_page = hit_wikipedia_api(company_name)
    generate_report(wiki_page, ticker_prices)


# invoke DAG
demo_dag = test_etl()
