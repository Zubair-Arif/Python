import awswrangler as wr
import pandas as pd

from datetime import datetime as dt


def get_s3_data(p_s3_url):

    s3_url = p_s3_url

    df = wr.s3.read_parquet(path=s3_url)

    return df


def write_s3_data(p_df, p_s3_url):

    df = p_df

    s3_url = p_s3_url


    print(df.dtypes)


    print(s3_url)

    print(df["_airbyte_additional_properties"])


def main():
    tables_name = [
        "source=age_gender_overview",
        "source=daily_active_users",
        "source=devices",
        "source=four_weekly_active_users",
        "source=locations",
        "source=monthly_active_users",
        "source=pages",
        "source=traffic_sources",
        "source=two_weekly_active_users",
        "source=website__vitals",
        "source=website_overview",
        "source=weekly_active_users",
    ]
    verticals = ["vertical=IAPTU", "vertical=Lifestyle", "vertical=Manga", "vertical=News", "vertical=Sports"]
    vertical_brand_url = {
        "vertical=IAPTU": [
            "/vertical=IAPTU/brand=Independent_Arabic/year=2015-2022-July-18/0.parquet",
            "/vertical=IAPTU/brand=Independent_Persian/year=2015-2022-July-18/0.parquet",
            "/vertical=IAPTU/brand=Independent_Turkey/year=2015-2022-July-18/0.parquet",
            "/vertical=IAPTU/brand=Independent_Urdu/year=2015-2022-July-18/0.parquet",
        ],
        "vertical=Lifestyle": [
            "/vertical=Lifestyle/brand=About_her/year=2015-2022-July-18/0.parquet",
            "/vertical=Lifestyle/brand=Al-Jamila/year=2015-2022-July-18/0.parquet",
            "/vertical=Lifestyle/brand=Arrajol/year=2015-2022-July-18/0.parquet",
            "/vertical=Lifestyle/brand=Hiamag/year=2015-2022-July-18/0.parquet",
            "/vertical=Lifestyle/brand=Sayidity_Magazine/year=2015-2022-July-18/0.parquet",
#           "/vertical=Lifestyle/brand=Sayidity_net/year=2015-2022 -July-18/0.parquet",
            "/vertical=Lifestyle/brand=www.Sayidity.net/year=2015-2022-July-18/0.parquet",
        ],
        "vertical=Manga": ["/vertical=Manga/brand=Manga_Arabia/year=2015-2022-July-18/0.parquet"],
        "vertical=News": [
            "/vertical=News/brand=AAA/year=2015-2022-july-18/0.parquet",
            "/vertical=News/brand=Aleqt/year=2015-2022-July-18/0.parquet",
            "/vertical=News/brand=Arab_new_FR/year=2015-2022-July-18/0.parquet",
            "/vertical=News/brand=Arab_News_all_website/year=2015-2022-July-18/0.parquet",
            "/vertical=News/brand=Arab_news_JP/year=2015-2022-July-18/0.parquet",
            "/vertical=News/brand=Arab_News_PK/year=2015-2022-July-18/0.parquet",
            "/vertical=News/brand=Malayalam_News/year=2015-2022-July-18/0.parquet",
            "/vertical=News/brand=Urdu_News/year=2015-2022-July-18/0.parquet",
        ],   
        
        "vertical=Sports": ["/vertical=Sports/brand=Arriyadiyah/year=2015-2022-July-18/0.parquet"],
    }

    s3_url = "s3://srmg-datalake-test/google_analytics_data/UA/"

    for table_name in tables_name:
        for vertical in verticals:
            for url in range(len(vertical_brand_url[vertical])):
                s3_full_url = s3_url + table_name + vertical_brand_url[vertical][url]
                df = get_s3_data(s3_full_url)
                write_s3_data(df, s3_full_url)


if __name__ == "__main__":
    main()
