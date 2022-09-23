from typing import Tuple
import streamlit as st
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import ticker
import streamlit.components.v1 as components


docstring_1 = """
This file defines our Streamlit app, which can be accessed at localhost:8502 after 
invoking streamlit on this file using `streamlit run demo/company_report.py` 
(assuming that the repo is your current directory).

Since Streamlit parses the file from top to bottom, we need first define the helper 
functions that we'll use to fetch and visualize the data. 

Note: We assign this docstring to a variable so that Streamlit doesn't render it. 
By default, Streamlit writes any literal values that appear on their own to the app.
"""


def get_from_db(query: str) -> pd.DataFrame:
    conn = sqlite3.connect("demo/company_info.db")
    result = pd.read_sql_query(query, conn)
    conn.close()
    return result


def get_all_data_from_table(table: str) -> pd.DataFrame:
    return get_from_db(f"SELECT * FROM {table}")


def get_wikipedia_summary_from_db() -> str:
    query = "SELECT summary FROM wikipedia WHERE scrape_date=(SELECT max(scrape_date) FROM wikipedia);"
    return get_from_db(query).at[0, "summary"]


def get_wikipedia_url_from_db() -> str:
    query = "SELECT url FROM wikipedia WHERE scrape_date=(SELECT max(scrape_date) FROM wikipedia);"
    return get_from_db(query).at[0, "url"]


def get_esg_info_from_db() -> Tuple[float, str, int]:
    query = "SELECT * FROM esg WHERE scrape_date=(SELECT max(scrape_date) FROM esg);"
    for score, risk_level, percentile, _ in get_from_db(query).itertuples(
        name=None, index=False
    ):
        return score, risk_level, percentile


def generate_news_card(headline: str, blurb: str, link: str, index: int) -> str:
    card_number = str(index)
    return f"""
            <div class="card" style="width: fit-content; width: 900px; text-overflow: ellipses">
              <div class="card-header" id="heading{card_number}" style="overflow-x: auto; border: 1px rgba(0,0,0,.125); background-color: transparent;">
                <h5 class="mb-0">
                  <button class="btn btn-link" data-toggle="collapse" data-target="#collapse{card_number}" aria-expanded="true" aria-controls="collapse{card_number}" style="color:rgb(49, 51, 63); padding: 0px; font-family: \"Source Sans Pro\", sans-serif;">
                  {headline}
                  </button>
                </h5>
              </div>
              <div id="collapse{card_number}" class="collapse show" aria-labelledby="heading{card_number}" data-parent="#accordion">
                <div class="card-body">
                  <a href="{link}" style="color:rgb(149, 151, 163); font-size: small;"> {blurb} </a>
                </div>
              </div>
            </div>
            """


def generate_news_html() -> str:
    styling = """<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css" integrity="sha384-Gn5384xqQ1aoWXA+058RXPxPg6fy4IWvTNh0E263XmFcJlSAwiGgFAW/dAiS6JXm" crossorigin="anonymous">
        <script src="https://code.jquery.com/jquery-3.2.1.slim.min.js" integrity="sha384-KJ3o2DKtIkvYIK3UENzmM7KCkRr/rE9/Qpg6aAZGJwFDMVNA/GpGFF93hXpG5KkN" crossorigin="anonymous"></script>
        <script src="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/js/bootstrap.min.js" integrity="sha384-JZR6Spejh4U02d8jOt6vLEHfe/JQGiRRSQQxSfFWpi1MquVdAyjUar5+76PVCmYl" crossorigin="anonymous"></script>
        <div id="accordion">
        """
    news_df = get_all_data_from_table("news")
    for index, headline, blurb, link, _ in news_df.itertuples(name=None):
        styling += generate_news_card(headline, blurb, link, index)
    return styling


def create_esg_score_plot(score: float, risk_level: str, percentile: int) -> plt.figure:
    fig = plt.figure(figsize=(8, 0.5))
    ax = fig.add_subplot()
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 1)

    # remove everything except x-axis
    ax.yaxis.set_major_locator(ticker.NullLocator())
    ax.spines.right.set_color("none")
    ax.spines.left.set_color("none")
    ax.spines.top.set_color("none")

    # define tick positions
    ax.xaxis.set_major_locator(ticker.MultipleLocator(5.00))
    ax.xaxis.set_minor_locator(ticker.MultipleLocator(1.00))
    ax.xaxis.set_ticks_position("bottom")
    ax.tick_params(which="major", width=1.00, length=5, labelsize=7)
    ax.tick_params(which="minor", width=0.75, length=2.5)

    plt.annotate(
        f"Risk: {risk_level} \n Percentile: {percentile}",
        (score, 0),
        xytext=(score, 1),
        horizontalalignment="center",
    )
    plt.scatter(x=score, y=0, c="red")
    return fig


def calculate_percent_change(prices_df: pd.DataFrame) -> float:
    first_price = float(prices_df.iloc[0]["price"])
    last_price = float(prices_df.iloc[-1]["price"])
    return round((last_price - first_price) / first_price * 100, 3)


docstring_2 = """
Now that we have our helper functions, we can define the layout of the different data 
visualizations on our page. Streamlit will add the different components to the page in 
the order it reads them, from top to bottom.
"""

st.set_page_config(layout="wide")

# Header
logo, name = st.columns([1, 10])
logo.image("./demo/files/trp_logo.png", width=85)
name.title("T. Rowe Price")

# Split out news column
main, news = st.columns([1.75, 1], gap="large")

# News
with news:
    components.html(generate_news_html(), height=500, scrolling=True)

# Description
main.markdown(get_wikipedia_summary_from_db().replace("$", "\$"))
main.markdown("Further reading: " + get_wikipedia_url_from_db())

# ESG section
score, risk_level, percentile = get_esg_info_from_db()
main.subheader(f"ESG Risk Score: {score}")
fig = create_esg_score_plot(score, risk_level, percentile)
main.pyplot(fig=fig)

# Stock price
prices_df = get_all_data_from_table("stock_price")
st.subheader(f"Stock Price: {calculate_percent_change(prices_df)}%")

prices_df["scrape_date"] = pd.to_datetime(prices_df.scrape_date).apply(
    lambda x: x.strftime("%Y-%m-%d %H:%M")
)

col1, col2 = st.columns([2, 1], gap="large")
col1.line_chart(prices_df, y="price", x="scrape_date")
col2.write(prices_df)
