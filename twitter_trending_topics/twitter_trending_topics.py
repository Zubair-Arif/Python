import tweepy
import os
import json
import sys
import pandas as pd
import boto3
import configparser as cp
import awswrangler as wr
def read_config():
    config = cp.ConfigParser()
    config.read("config.ini")
    os.environ["API_KEY"] = config["Default"]["API_KEY"]
    os.environ["API_KEY"] = config["Default"]["API_KEY"]
    os.environ["API_SECRET_KEY"] = config["Default"]["API_SECRET_KEY"]
    os.environ["ACCESS_TOKEN"] = config["Default"]["ACCESS_TOKEN"]
    os.environ["ACCESS_TOKEN_SECRET"] = config["Default"]["ACCESS_TOKEN_SECRET"]
    ids = json.loads(config["Default"]["ids"])
    countries = json.loads(config["Default"]["countries"])
    consumer_key= os.environ["API_KEY"]
    consumer_secret = os.environ["API_SECRET_KEY"]
    access_token = os.environ["ACCESS_TOKEN"]
    access_token_secret = os.environ["ACCESS_TOKEN_SECRET"]
    return [consumer_key,consumer_secret,access_token,access_token_secret,ids,countries]

def twitter_read(consumer_key, consumer_secret, access_token, access_token_secret):
    auth = tweepy.OAuth1UserHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    return api

def normalize_response(api,ids,countries):
    all_list = []
    for id, country in zip(ids, countries):
        country_loc = api.get_place_trends(id)
        df = pd.DataFrame(country_loc[0]["trends"])
        df["as_of"] = country_loc[0]["as_of"]
        df["created_at"] = country_loc[0]["created_at"]
        df["locations"] = country_loc[0]["locations"][0]["name"]
        df["woeid"] = country_loc[0]["locations"][0]["woeid"]
        all_list.append(df)
        twitter_write_files(df,country)
    df_bulk = pd.DataFrame(all_list[0])
    twitter_bulk_files(df_bulk)
    return df,df_bulk

def twitter_write_files(df,country):
    ''' This fn is used to write individual countries to s3'''
    wr.s3.to_json(df=df,path=f"s3://srmg-datalake/social_media_metrics/raw_json/twitter_trending_topics{country}.json")
    print("written")
    return "written"

def twitter_bulk_files(df_bulk):
    ''' This fn is used to write bulk files into s3'''

    wr.s3.to_json(df=df_bulk,path="s3://srmg-datalake/social_media_metrics/twitter_trending_topics/twitter_trending_topics.json")
    print("written")
    return "written"

if __name__=="__main__":
    [consumer_key, consumer_secret, access_token, access_token_secret,ids,countries] = read_config()
    api_response = twitter_read(consumer_key, consumer_secret, access_token, access_token_secret)
    normalize_response(api_response,ids,countries)



