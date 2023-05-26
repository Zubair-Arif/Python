"""
This document contain the python code that can check whether the data in AWS S3 contain duplicates or not
"""

import configparser as cp
import json
from datetime import date as d, datetime as dt, timedelta
import pandas as pd
import awswrangler as wr
import boto3

s3 = boto3.client("s3")

def read_gen_config():

    """Reading data from config file"""

    config = cp.ConfigParser()
    config.read("config.ini")
    table_names = int(config["Default"]["table_names"])
    vertical_brand_url = json.loads(config["Default"]["vertical_brand_url"])
    s3_url = json.loads(config["Default"]["s3_url"])

    return table_names, vertical_brand_url,s3_url


def get_cloud_data(p_tab, p_vertical_brand_url,p_s3_url):

    """Reading data from AWS S3 bucket"""

    table_name = p_tab
    p_vertical_brand_url = p_vertical_brand_url
    profile_url = p_profile_url
    df_s3_data = pd.DataFrame()
    try:
        df_s3_data = wr.s3.read_parquet(path=profile_url + PARAM_CHOICE[user_choice].lower() + "_profile/" + str(year) + ".parquet")

    except Exception as gen_exception:
        print("Error Occured while reading the AWS S3 File : ", gen_exception)

    return df_s3_data

def main():

    """main function"""
    global PARAM_CHOICE
    PARAM_CHOICE = ["Facebook", "Instagram", "Youtube", "LinkedIn", "Twitter"]

    table_names,vertical_brand_url,s3_url = read_gen_config()

    for tab in range(len(table_names)):
        df_s3_data = get_cloud_data(tab, vertical_brand_url,s3_url)

    return None

if __name__ == "__main__":
    main()
