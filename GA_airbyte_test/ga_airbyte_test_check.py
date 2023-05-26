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

    # Bronze Zone

    # df_s3_data = wr.s3.read_parquet("s3://srmg-datalake-test/google_analytics_test_1/source=devices/vertical=Argaam/brand=Akhbaar24/year=2023/0.parquet")
    # print(df_s3_data)
    # df = pd.json_normalize(df_s3_data["ga_date"])
    # df_s3_data["date"] = df["member0"]
    # df_s3_data.pop("ga_date")
    # wr.s3.to_parquet(df=df_s3_data, path="s3://srmg-datalake-test/srmg_silver_zone/source=devices/vertical=Argaam/brand=Akhbaar24/year=2023/0.parquet", dataset=True)
    
    
    # Bronze Zone to Silver Zone
     
    # df_s3_data = wr.s3.read_parquet("s3://srmg-datalake-test/google_analytics_test_1/source=devices/vertical=Argaam/brand=Akhbaar24/year=2023/1.parquet")
    # print(df_s3_data["ga_date"])
    # df = pd.json_normalize(df_s3_data["ga_date"])
    # df_s3_data["date"] = df["member0"]
    # df_s3_data.pop("ga_date")
    # wr.s3.to_parquet(df=df_s3_data, path="s3://srmg-datalake-test/srmg_silver_zone/source=devices/vertical=Argaam/brand=Akhbaar24/year=2023/0.parquet", dataset=True)
   
    
    # Silver Zone to Gold Zone

    # df_s3_data = wr.s3.read_parquet("s3://srmg-datalake-test/srmg_gold_zone/source=devices/vertical=Argaam/brand=Akhbaar24/year=2023/0.parquet")
    # df_s3_data.to_csv("C:/Users/Praveen/Documents/GA_airbyte_test/airbyte_gold_zone.csv", header=False)
    # print(df_s3_data)
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    # df_s3_data = wr.s3.read_parquet("s3://srmg-datalake-test/ga_data_test/source=website_overview/vertical=News/brand=Arab_News/year=2023/", path_suffix=".parquet")
    # print(df_s3_data)
    # df = pd.json_normalize(df_s3_data["ga_date"])
    # df_s3_data["date"] = df["member0"]
    # df_s3_data.pop("ga_date")
    # df_s3_data = df_s3_data.drop_duplicates(["ga_users", "ga_exitRate", "ga_newUsers", "ga_sessions", "ga_pageviews", "isDataGolden", "ga_bounceRate", "ga_avgTimeOnPage", "ga_sessionsPerUser", "ga_avgSessionDuration", "ga_pageviewsPerSession", "_airbyte_additional_properties", "date"])
    # wr.s3.to_parquet(df=df_s3_data, path="s3://srmg-datalake-test/srmg_gold_zone/source=devices/vertical=Argaam/brand=Akhbaar24/year=2023/0.parquet", dataset=True)
    # s3.Object("srmg-datalake-test", "srmg_silver_zone/source=devices/vertical=Argaam/brand=Akhbaar24/year=2023/1.parquet").delete()
    # BUCKET = "srmg-datalake-test"
    # PREFIX = "ga_data_test/source=website_overview/vertical=News/brand=Arab_News/year=2023/0.parquet/"

    # df_s3_data = wr.s3.read_parquet("s3://srmg-datalake-test/ga_data_test/source=website_overview/vertical=News/brand=Arab_News/year=2023/0.parquet")

    # response = s3_client.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)

    # for object in response["Contents"]:
    #     print("Deleting", object["Key"])
    #     s3_client.delete_object(Bucket=BUCKET, Key=object["Key"])
    # print(df_s3_data)
    # df_s3_data = wr.s3.read_parquet("s3://srmg-datalake-test/ga_data_test/source=website_overview/vertical=News/brand=Arab_News/year=2023/1.parquet")
    # print(df_s3_data)
    # df = pd.json_normalize(df_s3_data["ga_date"])
    # df_s3_data["date"] = df["member0"]
    # df_s3_data.pop("ga_date")
    # df_s3_data = df_s3_data.drop_duplicates(["ga_users", "ga_exitRate", "ga_newUsers", "ga_sessions", "ga_pageviews", "isDataGolden", "ga_bounceRate", "ga_avgTimeOnPage", "ga_sessionsPerUser", "ga_avgSessionDuration", "ga_pageviewsPerSession", "_airbyte_additional_properties", "date"])
    # print("After Removing Duplicates")
    # print(df_s3_data)
    # df_s3_data.to_csv("C:/Users/Praveen/Documents/GA_airbyte_test/airbyte_data_without_duplicate.csv", header=False)
    # wr.s3.to_parquet(df=df_s3_data, path="s3://srmg-datalake-test/ga_data_test/source=website_overview/vertical=News/brand=Arab_News/year=2023/0.parquet", mode= "",dataset=True)


if __name__ == "__main__":
    main()
