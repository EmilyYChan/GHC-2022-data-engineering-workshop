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

        # alternative curation
        # parse out different info -> ratio of volume to different volume
        # exceptionally high volume compared to a moving average of volume can reveal euphoria or fear while much lower than average volume can reflect apathy or disinterest
        volume = parser.find(
            name="fin-streamer",
            attrs={"data-symbol": company_ticker, "data-field": "regularMarketVolume"},
        )
        avg_volume = parser.find(
            name="td", attrs={"data-test": "AVERAGE_VOLUME_3MONTH-value"}
        )
        print(
            "Daily Volume: ", volume.text, "\n3 Month Average Volume: ", avg_volume.text
        )

        # esg score
        ESG_score_component = parser.find(
            name="div", attrs={"data-yaft-module": "tdv2-applet-miniESGScore"}
        )
        tags = ESG_score_component.find_all(string=re.compile(r".*"))
        print(tags)
        return volume, avg_volume, tags

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

    @task(retries=3, retry_exponential_backoff=True)
    def scrape_bing_news(company_name):
        """
        #### Scrape Bing News
        Get the Bing News page for a given company and save the page to the local directory
        """
        # import requests

        # url = f"https://www.bing.com/news/search?q={company_name.replace(" ", "+")}"
        # bing_news_page = requests.get(url)
        # print("Request to {url} returned status code: ", bing_news_page.status_code)

        # # save response as html
        # with open('bing_news.html', 'w') as file:
        #     file.write(bing_news_page.text)

        # SIGSEGV workaround
        bing_news_page = open("bing_news.html", "r").read()

        return bing_news_page

    @task()
    def curate_bing_news_html(bing_news_page):
        """
        #### Curate Bing News HTML
        Parse out headlines and source links for each article
        """
        from bs4 import BeautifulSoup

        parser = BeautifulSoup(bing_news_page, "html.parser")
        bing_news_results = parser.find(name="div", attrs={"class": "main-container"})

        # save for use in report
        with open("bing_articles.html", "w") as file:
            file.write(bing_news_results.prettify())

        bing_news_article_cards = bing_news_results.find_all(
            name="div", attrs={"class": "news-card-body card-with-cluster"}
        )

        # pull out headline, blurb, link
        def extract_article_info(article_div):
            headline = article_div.find("div", attrs={"class": "t_t"}).text
            blurb = article_div.find("div", attrs={"class": "snippet"}).text
            source_link = article_div.find("a", href=True)["href"]

            return headline, blurb, source_link

        parsed_articles = list(map(extract_article_info, bing_news_article_cards))

        # save to sqlite db
        import sqlite3
        conn = sqlite3.connect('company_report.db')
        cur = conn.cursor()
        for headline, blurb, link in parsed_articles:
            cur.execute("INSERT INTO news_articles VALUES (?, ?, ?) ON CONFLICT(headline) DO UPDATE SET source_link= ?;", (headline, blurb, link, link))
        conn.commit()
        conn.close()

        return parsed_articles

    @task()
    def profile_data():
        # https://medium.com/analytics-vidhya/pandas-profiling-5ecd0b977ecd
        import sqlite3
        import pandas as pd
        from pandas_profiling import ProfileReport

        conn = sqlite3.connect('company_report.db')
        df = pd.read_sql_query("SELECT * FROM news_articles", conn)
        profile = ProfileReport(df, title="News Articles Data Profile")
        profile.to_file("news_articles_data_profile.html")

    @task()
    def generate_report(wiki_page, ticker_prices, bing_articles):
        """
        #### Generate a report
        """
        print(ticker_prices)
        print("Summary: \n", wiki_page.summary, "\n")
        print("Further reading: \n", wiki_page.fullurl)
        print(bing_articles[0])
        

    company_ticker = "TROW"
    company_name = "T. Rowe Price"

    yahoo_finance_page = scrape_yahoo_finance(company_ticker)
    ticker_prices = curate_yahoo_finance_html(yahoo_finance_page)

    bing_news_page = scrape_bing_news(company_name)
    bing_articles = curate_bing_news_html(bing_news_page)

    wiki_page = hit_wikipedia_api(company_name)

    generate_report(wiki_page, ticker_prices, bing_articles)

    profile_data()

# invoke DAG
demo_dag = test_etl()


# TODO: set up sql lite db
# pandas profiling
# great expectations
