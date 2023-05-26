from types import SimpleNamespace
import pandas as pd
import configparser as cp
import argparse
import time
from datetime import timedelta, date, datetime
import json
import calendar
import boto3
import awswrangler as wr


def main():
    secret = boto3.client("ssm")
    config = secret.get_parameter(Name="srmg_adjust_config")["Parameter"]["Value"]

    c, aws = parseConfig(config)
    s3 = boto3.client("s3")

    start_year = int(c.start_date[:4])
    end_year = int(c.end_date[:4])
    start_date_arr = [c.start_date] + [f"{Y}-01-01" for Y in range(start_year + 1, end_year + 1)]
    end_date_arr = [f"{Y}-12-31" for Y in range(start_year, end_year)] + [c.end_date]
    
    start_date_arr = ["2022-05-01"]
    end_date_arr = ["2022-05-31"]
    for start, end in zip(start_date_arr, end_date_arr):
        st = time.time()

        year = int(start[:4])
        start_month = int(start[5:7])
        end_month = int(end[5:7])
        start_month_arr = [start] + [f"{year}-{M:02}-01" for M in range(start_month + 1, end_month + 1)]
        end_month_arr = [f"{year}-{M:02}-{calendar.monthrange(year, M)[1]:02}" for M in range(start_month, end_month)] + [end]

        for s, e in zip(start_month_arr, end_month_arr):
            df = pd.DataFrame()
            for b, v in daterange(s, e, timedelta(days=7)):
                for i in c.app_tokens:
                    print(b, v)
                    url = f"https://api.adjust.com/kpis/v1/{i}.csv?user_token={c.user_token}&start_date={b}&end_date={v}&kpis={c.kpis}&event_kpis={c.event_kpis}&grouping={c.grouping}&attribution_type={c.attribution_type}&human_readable_kpis={c.human_readable_kpis}"
                    temp = pd.read_csv(url, dtype=c.schema)
                    temp["brand"] = c.brand_vertical[i][1]
                    temp["vertical"] = c.brand_vertical[i][0]
                    if df.empty:
                        df = temp
                    else:
                        df = pd.concat([df, temp])
            print(f"Received data for month {s[5:7]} of {year}")
            df["date"] = pd.to_datetime(df["date"]).dt.date
            
            df["clicks"].fillna(0, inplace = True)
            df["impressions"].fillna(0, inplace = True)

            df['all_engage'] = df.clicks + df.impressions
  
            if not c.append:
                wr.s3.to_parquet(df=df, path=aws.raw_savepath.format(year=s[:4], month=s[5:7]), index=False)
                df.dropna(axis=1, how="all", inplace=True)
                df_group = df.groupby(["date", "country", "region","os_name","partner","clicks","impressions","all_engage"]).sum().reset_index()
                wr.s3.to_parquet(df=df_group, path=aws.processed_savepath.format(year=s[:4], month=s[5:7]), index=False)

            else:
                try:
                    master_df = wr.s3.read_parquet(path=aws.raw_savepath.format(year=start[:4], month=s[5:7]))
                    df = pd.concat([master_df, df])
                    df.dropna(axis=1, how="all", inplace=True)
                    df_group = df.groupby(["date", "country", "region","os_name","partner","clicks","impressions","all_engage"]).sum().reset_index()
                    wr.s3.to_parquet(df=df_group, path=aws.processed_savepath.format(year=s[:4], month=s[5:7]), index=False)

                except:
                    pass


def daterange(s, e, d):
    start = datetime.strptime(s, "%Y-%m-%d")
    end = datetime.strptime(e, "%Y-%m-%d")
    while start + d < end:
        yield (start.strftime("%Y-%m-%d"), (start + d).strftime("%Y-%m-%d"))
        start += d + timedelta(days=1)
    yield (start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))


def parseConfig(filename):
    config = cp.ConfigParser()
    config.read_string(filename)
    config1 = cp.ConfigParser()
    config1.read("config.ini")

    c = SimpleNamespace()
    aws = SimpleNamespace()

    aws.raw_savepath = config1["Default"]["raw_savepath"]

    aws.processed_savepath = config1["Default"]["processed_savepath"]

    c.user_token = config["Default"]["user_token"]
    c.app_tokens = json.loads(config["Default"]["app_tokens"])

    c.start_date = config["Default"]["start_date"]
    c.end_date = config["Default"]["end_date"]
    c.append = config["Default"]["append"]

    c.kpis = json.loads(config["Default"]["kpis"])
    c.event_kpis = json.loads(config["Default"]["event_kpis"])
    c.attribution_type = config["Default"]["attribution_type"]
    c.grouping = json.loads(config["Default"]["grouping"])
    c.human_readable_kpis = config["Default"]["human_readable_kpis"]
    c.schema = json.loads(config["Default"]["schema"])

    b, v = [], []
    for l in c.app_tokens.values():
        v.append(l[0])
        b.append(l[1])
    c.brand_vertical = c.app_tokens
    c.app_tokens = c.app_tokens.keys()

    yesterday = str(date.today() - timedelta(days=1))
    c.start_date = yesterday if c.start_date == "" else c.start_date
    c.start_date = c.start_date.format(year=yesterday[:4], month=yesterday[5:7], day=yesterday[8:])
    c.end_date = yesterday if c.end_date == "" else c.end_date
    c.end_date = c.end_date.format(year=yesterday[:4], month=yesterday[5:7], day=yesterday[8:])
    c.append = c.append.upper() == "TRUE" or c.append == "1"

    c.kpis = ",".join(c.kpis)
    c.event_kpis = "all_" + "|".join(c.event_kpis)
    c.grouping = ",".join(c.grouping)
    c.human_readable_kpis = c.human_readable_kpis.capitalize == "TRUE" or c.human_readable_kpis == "1"

    return c, aws


if __name__ == "__main__":
    main()
