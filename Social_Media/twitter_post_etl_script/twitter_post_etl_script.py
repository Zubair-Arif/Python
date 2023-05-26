import pandas as pd
import requests
import tweepy
import configparser as cp
from requests_oauthlib import OAuth1
import boto3
import awswrangler as wr
import json
from datetime import date as d, datetime as dt, timedelta

pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)

s3 = boto3.resource("s3")


def read_config():
    config = cp.ConfigParser()
    config.read("config.ini")

    bearer_token = config["Default"]["bearer_token"]
    CONSUMER_KEY = config["Default"]["API_KEY"]
    CONSUMER_SECRET = config["Default"]["API_SECRET_KEY"]
    ACCESS_TOKEN = config["Default"]["ACCESS_TOKEN"]
    ACCESS_SECRET = config["Default"]["ACCESS_TOKEN_SECRET"]
    profile_ids = json.loads(config["Default"]["profile_ids"])
    dict_datatype = json.loads(config["Default"]["dict_datatype"])
    s3_path = config["Default"]["s3_path"]
    brand_vertical_mapping_url = config["Default"]["brand_vertical_mapping_url"]

    return [bearer_token, CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET, profile_ids,
            brand_vertical_mapping_url, dict_datatype, s3_path]


def brand_vertical_mapping(p_brand_vertical_mapping_url):
    brand_vertical_mapping_url = p_brand_vertical_mapping_url
    brand_vertical_df = pd.DataFrame()
    try:
        obj = s3.Object("srmg-datalake", "brand_vertical_mapping/brand_vertical_mapping_v3.json")
        brand_vertical_json = json.load(obj.get()['Body'])
        brand_vertical_df = pd.DataFrame(brand_vertical_json)
    except Exception as gen_exception:
        print("Brand Vertical Mapping Read Error: ", gen_exception)
        return None
    brand_vertical_df["brand"] = brand_vertical_df["new_brand_name"].astype("str")
    brand_vertical_df = brand_vertical_df.astype("string")
    return brand_vertical_df


def process_tweepy_data(bearer_token, CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET, profile_ids,
                        brand_vertical_mapping_url, dict_datatype, s3_path):
    client = tweepy.Client(bearer_token)
    next_token = None

    headeroauth = OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET, signature_type='auth_header')

    for profile_id in profile_ids:
        data = []

        while True:
            response = client.get_users_tweets(id=profile_id, pagination_token=next_token, max_results=100)

            try:
                if response.meta["next_token"]:
                    next_token = response.meta["next_token"]

                    data.extend(response[0])
            except:
                try:
                    data.extend(response[0])
                except:
                    break
                break
        data_df = pd.DataFrame.from_records(data)
        print(data_df)
        tweet_ids = data_df["id"].to_list()

        get_dataframe(tweet_ids, headeroauth, profile_id, brand_vertical_mapping_url, dict_datatype, s3_path)
    return data_df

def get_dataframe(tweet_ids, headeroauth, profile_id, brand_vertical_mapping_url, dict_datatype, s3_path):
    data_arr = []
    for i in range(0, len(tweet_ids), 100):
        temp_tweet_ids = tweet_ids[i:i + 100]
        tweet_ids_string = ','.join(map(str, temp_tweet_ids))
        url = f'https://api.twitter.com/2/tweets?ids={tweet_ids_string}&tweet.fields=public_metrics,created_at,entities,source,conversation_id,lang&expansions=attachments.media_keys&media.fields=public_metrics'
        # non_public_metrics_url=f'https://api.twitter.com/2/tweets?ids={tweet_ids_string}&tweet.fields=non_public_metrics,organic_metrics&media.fields=non_public_metrics,organic_metrics&expansions=attachments.media_keys'

        response = requests.get(url, auth=headeroauth)
        d = response.json()
        try:
            for i in range(len(d["data"])):

                try:
                    views_count = d["includes"]["media"][i]["public_metrics"]["view_count"]
                except:
                    views_count = 0
                try:
                    type = d["includes"]["media"][i]["type"]
                except:
                    type = None
                try:
                    media_keys = d["data"][i]["attachments"]["media_keys"][i]
                except:
                    media_keys = None
                try:
                    mentioned_name = d["data"][i]["entities"]["mentions"][i]["username"]
                except:
                    mentioned_name = None

                try:
                    url = d["data"][i]["entities"]["urls"][i]["url"]
                except:
                    url = None

                try:
                    hashtag = d["data"][i]["entities"]["hashtags"][i]["tag"]
                except:
                    hashtag = None

                # print(id)

                dict_data = {
                    "profile_id": profile_id,
                    "id": d["data"][i]["id"],
                    "text": d["data"][i]["text"],
                    "retweet_count": d["data"][i]["public_metrics"]["retweet_count"],
                    "created_date": d["data"][i]["created_at"],
                    "reply_count": d["data"][i]["public_metrics"]["reply_count"],
                    "like_count": d["data"][i]["public_metrics"]["like_count"],
                    "quote_count": d["data"][i]["public_metrics"]["quote_count"],
                    "media_keys": media_keys,
                    "views_count": views_count,
                    "type": type,
                    "url": url,
                    "hashtag": hashtag,
                    "conversation_id": d["data"][i]["conversation_id"],
                    "source": d["data"][i]["source"],
                    "language": d["data"][i]["lang"],
                    "mentioned_name": mentioned_name,
                    "coordinates": None,
                    "outlinks": None,
                    "inreplytouser": None,
                    "inreplytotweetid": None,
                    "retweetedtweet": None,
                    "place": None,
                    "cashtags": None,
                    "Impression_count": 0,
                    "url_link_clicks": 0,
                    "user_profile_click": 0,
                    "playback_0_count": 0,
                    "playback_100_count": 0,
                    "playback_25_count": 0,
                    "playback_50_count": 0,
                    "playback_75_count": 0
                }
                data_arr.append(dict_data)
        except:
            pass

    df = pd.DataFrame(data_arr)
    df = df.astype(dict_datatype)
    brand_vertical_mapping_df = brand_vertical_mapping(brand_vertical_mapping_url)

    brand_vertical_df_row = brand_vertical_mapping_df.loc[
        brand_vertical_mapping_df["twitter_profile_id"] == profile_id]

    df["vertical"] = brand_vertical_df_row["vertical"].values[0]
    df["brand"] = brand = brand_vertical_df_row["brand"].values[0]
    df["created_date"] = pd.to_datetime(df["created_date"])
    df["date"] = df["created_date"].dt.date
    df["year"] = df["created_date"].dt.year
    df["month"] = df["created_date"].dt.strftime("%b")
    # write_to_s3(df, brand, s3_path)
    print(df)


def write_to_s3(df, brand, s3_path):
    wr.s3.to_parquet(df=df, path=s3_path, dataset=True, partition_cols=["vertical", "brand", "year"])
    print("brand", df["brand"].values[0])
    print("done")
    print(df.shape[0])
    print("vertical", df["vertical"].values[0])


def main():
    [bearer_token, CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET, profile_ids, brand_vertical_mapping_url,
     dict_datatype, s3_path] = read_config()
    profile_ids = event

    process_tweepy_data(bearer_token, CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET, profile_ids,
                        brand_vertical_mapping_url, dict_datatype, s3_path)


if __name__ == "__main__":
    main()




