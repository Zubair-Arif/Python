from datetime import date, datetime as dt, timedelta
import json
import configparser as cp
import calendar
import requests as req
import awswrangler as wr
import boto3
import pandas as pd

config = cp.ConfigParser()


def read_gen_config():

    """Reading data from config file"""

    global from_email, to_email, cc_email, email_pass, log_list, log_link, verticals, tables_name, vertical_brand_url, s3_ga_url
    secret = boto3.client("ssm")
    log_list = []

    from_email = config["Default"]["from_email"]
    to_email = json.loads(config["Default"]["to_email"])
    cc_email = json.loads(config["Default"]["cc_email"])
    email_pass = secret.get_parameter(Name="srmg_validation_daily_report_email_pass", WithDecryption=True)["Parameter"]["Value"]
    log_link = config["Default"]["log_link"]
    verticals = json.loads(config["Default"]["verticals"])
    tables_name = json.loads(config["Default"]["tables_name"])
    vertical_brand_url = json.loads(config["Default"]["vertical_brand_url"])
    s3_ga_url = config["Default"]["s3_ga_url"]

def get_s3_data(p_s3_url):

    """getting ga data from s3"""

    s3_url = p_s3_url
    try:

        df = wr.s3.read_parquet(path=s3_url, path_suffix=".parquet")

    except:
        df = pd.DataFrame()

    return df


def validate_s3_data(p_df, p_s3_url):

    """validate last updated date"""

    df = p_df
    s3_url = p_s3_url
    if not df.empty:
        d = df["ga_date"].values.tolist()
        df1 = pd.DataFrame(d)
        today = date.today()
        diff = today - max(df1.member0)
        diff_str = str(diff)
        if diff_str[:2] != "0:":
            date_diff = diff_str[:2]
        else:
            date_diff = diff_str[:1]
        if int(date_diff) > 2:

            spl = s3_url.split("/")
            table = spl[5][7:]
            vertical = spl[6][9:]
            brand = spl[7][6:]
            last_updated_date = max(df1.member0)
            mismatch_dict = {"Table": table, "Vertical": vertical, "Brand": brand, "Last_updated_date": last_updated_date}

            return mismatch_dict
    mismatch_dict = {}
    return mismatch_dict


def duplicate_s3_data(p_df, p_s3_url):

    """check if there is any duplicates are there in ga tables"""

    df = p_df
    s3_url = p_s3_url
    if not df.empty:
        s_df = df.drop(["ga_date"], axis=1)
        duplicate = s_df[s_df.duplicated()]
        if not duplicate.empty:
            print("Duplicates are there in the file ", s3_url)
            print(duplicate.to_string())
            return True
        else:
            return False


def ga_validation():

    """validate the duplicate data and last updated date"""

    log_b = "Google Analytics data validation"
    log_c = "*********************************"
    log_list.append(log_b)
    log_list.append(log_c)
    print(log_b)
    print(log_c)

    not_updated_arr = []
    s = 0
    for table_name in tables_name:
        for vertical in verticals:
            for url in range(len(vertical_brand_url[vertical])):
                s3_full_url = s3_ga_url + table_name + vertical_brand_url[vertical][url]
                df = get_s3_data(s3_full_url)
                not_updated_dict = validate_s3_data(df, s3_full_url)
                if bool(not_updated_dict):
                    not_updated_arr.append(not_updated_dict)
                flag = duplicate_s3_data(df, s3_full_url)

                if flag:
                    s += 1
    count = 0
    mismatch_df = pd.DataFrame.from_records(not_updated_arr)
    if s != 0:
        count += 1
        log = "GA Duplicate validation : Failed"
        log_list.append(log)
       
    if len(not_updated_arr) != 0:
        count += 1
    if count == 0:
        log_list.append("All google analytics data validation : PASSED")
    print(mismatch_df.to_string(index=False))
    return mismatch_df

def main(event, context):
    ga_mismatch_df = ga_validation()
    
if __name__ == "__main__":
    main()