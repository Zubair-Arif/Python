
import configparser as cp
import json
from datetime import date as d, datetime as dt, timedelta
import pandas as pd
import awswrangler as wr
import boto3

def main():

    """main function"""
    global PARAM_CHOICE
    PARAM_CHOICE = ["Facebook", "Instagram", "Youtube", "LinkedIn", "Twitter"]

    df_s3_data_2015_2022=wr.s3.read_parquet(path="s3://srmg-datalake-test/google_analytics_data/UA/source=age_gender_overview/vertical=IAPTU/brand=Independent_Turkey/year=2015-2022-July-18/0.parquet")

    df_s3_data_2022=wr.s3.read_parquet(path="s3://srmg-datalake-test/google_analytics_data/UA/source=age_gender_overview/vertical=IAPTU/brand=Independent_Turkey/year=2022/0.parquet")

    df_s3_data_2015_2022.to_csv("")

    print(df_s3_data_2022)
    
    return None

if __name__ == "__main__":
    main()
