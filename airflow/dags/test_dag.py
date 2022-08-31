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

    @task()
    def scrape_yahoo_finance(company_ticker):  # return type
        """
        #### Extract task
        A simple Extract task to get data ready for the rest of the data
        pipeline. 
        """
        # import requests

        # yahoo_finance_page = requests.get(f'https://finance.yahoo.com/quote/{company_ticker}')
        # # with open('yahoo_finance.html', 'w') as file:
        # #     file.write(yahoo_finance_page.text)

        import os 
        print("Current working directory:", os.getcwd())
        yahoo_finance_page = open('yahoo_finance.html', 'r').read()
        return yahoo_finance_page


    @task()
    def curate_yahoo_finance_html(yahoo_finance_page):
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(yahoo_finance_page, 'html.parser')

        market_price = soup.find(name="fin-streamer", attrs={"data-symbol": company_ticker, "data-field":"regularMarketPrice"})
        after_hours_trading_price = soup.find(name="fin-streamer", attrs={"data-symbol": company_ticker, "data-field":"postMarketPrice"})
        print("\nMarket price: ", market_price.text, "\nAfter hours trading price: ", after_hours_trading_price.text)
        

    @task()
    def hit_wikipedia_api():
        """
        #### Extract task
        A simple Extract task to get data ready for the rest of the data
        pipeline. 
        """
        import wikipediaapi

        company_name = 'T. Rowe Price'

        wiki = wikipediaapi.Wikipedia('en')
        page = wiki.page(company_name) # error handling
        print("\nSummary: \n", page.summary, "\n")
        print("Further reading: \n", page.fullurl)



    @task()
    def hit_news_api():
        """
        #### Extract task
        A simple Extract task to get data ready for the rest of the data
        pipeline. 
        """
        pass


    @task(multiple_outputs=True)
    def transform(order_data_dict: dict):
        """
        #### Transform task
        A simple Transform task which takes in the collection of order data and
        computes the total order value.
        """
        total_order_value = 0

        for value in order_data_dict.values():
            total_order_value += value

        return {"total_order_value": total_order_value}

    # [END transform]

    # [START load]
    @task()
    def load(total_order_value: float):
        """
        #### Load task
        A simple Load task which takes in the result of the Transform task and
        instead of saving it to end user review, just prints it out.
        """

        print(f"Total order value is: {total_order_value:.2f}")



    # [START main_flow]
    company_ticker = "TROW"
    yahoo_finance_page = scrape_yahoo_finance(company_ticker)
    curate_yahoo_finance_html(yahoo_finance_page)
    hit_wikipedia_api()
    #order_summary = transform(order_data)
    #load(order_summary["total_order_value"])
    # [END main_flow]


# [START dag_invocation]
demo_dag = test_etl()
# [END dag_invocation]


