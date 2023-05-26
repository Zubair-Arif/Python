"""
This document contains the python ETL pipeline code that will collect the IO_Technologies metrics for SRMG
from IO_Technologies dashboard using an API and loads the obtained data into an AWS S3 bucket.
The API configuration and the AWS S3 configuration are available in the config.ini file along with all
other supporting parameters.
Access Secrets are read from AWS SSM Parameter Store.
"""
import boto3
import warnings
import pandas as pd
import awswrangler as wr
from datetime import date, timedelta
from IO_TECH_ETL_Scripts.io_tech_etl_script import *


warnings.filterwarnings("ignore")
s3 = boto3.client("s3")

def validate():
    secret_keys, end_point, start_date, end_date, io_tech_s3_url, brand_vertical_mapping_url = read_config('IO_TECH_ETL_Scripts/test/config.ini')
    date_range_arr = date_limit_handler(start_date, end_date)
    brand_vertical_df = get_brand_vertical_mapping(brand_vertical_mapping_url)
    print(f"IOTech Validation from {start_date} to {end_date}")
    print('*'*64)

    global log_list
    log_list = ["*"*80,"IOTech Validation","*"*80]
    year = start_date.year

    data_list = wr.s3.list_objects(path = f"{io_tech_s3_url}processed/")
    data_list = set([i[:-13] for i in data_list])
    count = 0
    not_updated_arr = []
    s = 0
    for url in data_list:
        if 'articles' not in url:
            continue
        df_s3_data = pd.DataFrame()
        try:
            df_s3_data = wr.s3.read_parquet(f'{url}/{year}.parquet')
        except Exception as gen_exception:
            log = f"Exception: {gen_exception}"
            log_list.append(log)
            print(log)
            count += 1
        not_updated_dict = validate_s3_data(df_s3_data, url)
        if bool(not_updated_dict):
            not_updated_arr.append(not_updated_dict)
        flag = duplicate_s3_data(df_s3_data, url)
        if flag:
            s += 1
    mismatch_df = pd.DataFrame.from_records(not_updated_arr)
    if s != 0:
        count += 1
        log_list.append("IOTech Duplicate validation : Failed")
    else:
        log_list.append("IOTech Duplicate validation : PASSED")

    if len(not_updated_arr) != 0:
        count += 1
    if count == 0:
        log_list.append("All IOTech data validation : PASSED")
    print(mismatch_df.to_string(index=False))
    return mismatch_df, log_list

def validate_s3_data(df, s3_url):
    """validate last updated date"""

    if not df.empty:
        d = df["Date"]
        today = date.today()
        last_updated_date = max(d)
        diff = today - last_updated_date
        diff_str = str(diff)
        if diff_str[:2] != "0:":
            date_diff = diff_str[:2]
        else:
            date_diff = diff_str[:1]
        if int(date_diff) > 2:
            spl = s3_url.split("/")
            table = spl[5]
            vertical = spl[6][9:]
            brand = spl[7][6:]
            mismatch_dict = {"Table": table, "Vertical": vertical, "Brand": brand, "Last_updated_date": last_updated_date}

            return mismatch_dict
    mismatch_dict = {}
    return mismatch_dict

def duplicate_s3_data(df, s3_url):
    """check if there is any duplicates are there in ga tables"""
    if not df.empty:
        s_df = df.drop(["Date"], axis=1)
        duplicate = s_df[s_df.duplicated()]
        if not duplicate.empty:
            print("Duplicates are there in the file ", s3_url)
            log_list.append(f"Duplicates are there in the file {s3_url}")
            print(duplicate.to_string())
            return True
        else:
            return False