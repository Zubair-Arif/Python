import boto3
import pandas as pd
from datetime import timedelta, date as d
import json
import configparser as cp
import awswrangler as wr
from datetime import datetime as dt

s3 = boto3.resource("s3")
s3_client = boto3.client("s3")


def main():

    df_s3_data_new_file = wr.s3.read_parquet("s3://srmg-datalake-test/ga_data_test/source=website_overview/vertical=News/brand=Arab_News/year=2023/1.parquet")
    print("Before Removing Duplicates")
    print(df_s3_data_new_file)
    df = pd.json_normalize(df_s3_data_new_file["ga_date"])
    df_s3_data_new_file["date"] = df["member0"]
    df_s3_data_new_file.pop("ga_date")
    df_s3_data_new_file.to_csv("C:/Users/Praveen/Documents/GA_airbyte_test/airbyte_data_with_duplicate.csv", header=False)
    wr.s3.to_parquet(df=df_s3_data_new_file, path="s3://srmg-datalake-test/ga_data_test/source=website_overview/vertical=News/brand=Arab_News/year=2023/0.parquet", mode="append", dataset=True)
    s3.Object("srmg-datalake-test", "ga_data_test/source=website_overview/vertical=News/brand=Arab_News/year=2023/1.parquet").delete()
    BUCKET = "srmg-datalake-test"
    PREFIX = "ga_data_test/source=website_overview/vertical=News/brand=Arab_News/year=2023/0.parquet/"

    df_s3_data = wr.s3.read_parquet("s3://srmg-datalake-test/ga_data_test/source=website_overview/vertical=News/brand=Arab_News/year=2023/0.parquet")

    response = s3_client.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)

    for object in response["Contents"]:
        print("Deleting", object["Key"])
        s3_client.delete_object(Bucket=BUCKET, Key=object["Key"])
    print(df_s3_data)
    # df = pd.json_normalize(df_s3_data["ga_date"])
    # df_s3_data["date"] = df["member0"]
    # df_s3_data.pop("ga_date")
    df_s3_data = df_s3_data.drop_duplicates(["ga_users", "ga_exitRate", "ga_newUsers", "ga_sessions", "ga_pageviews", "isDataGolden", "ga_bounceRate", "ga_avgTimeOnPage", "ga_sessionsPerUser", "ga_avgSessionDuration", "ga_pageviewsPerSession", "_airbyte_additional_properties", "date"], keep="last")
    print("After Removing Duplicates")
    print(df_s3_data)
    df_s3_data.to_csv("C:/Users/Praveen/Documents/GA_airbyte_test/airbyte_data_without_duplicate.csv", header=False)
    wr.s3.to_parquet(df=df_s3_data, path="s3://srmg-datalake-test/ga_data_test/source=website_overview/vertical=News/brand=Arab_News/year=2023/0.parquet", dataset=True)


if __name__ == "__main__":
    main()
