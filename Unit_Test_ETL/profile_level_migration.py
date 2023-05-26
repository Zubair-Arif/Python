"""
This document contains the python ETL pipeline code that will collect the post-level social media metrics for SRMG
from SocialBakers using an API and loads the obtained data into an AWS S3 bucket.
The API configuration and the AWS S3 configuration are available in the config.ini file along with all
other supporting parameters.
Access Secrets are read from AWS SSM Parameter Store.
"""

import configparser as cp
import json
from datetime import date as d, datetime as dt, timedelta
import time as t
import warnings
import pandas as pd
import requests as req
import awswrangler as wr
import boto3

warnings.filterwarnings("ignore")
s3 = boto3.client("s3")
s3 = boto3.resource("s3")

SYNC_TYPE = 1

config = cp.ConfigParser()
config.read("./config.ini")

user_choice = int(config["Default"]["user_choice"])

PARAM_CHOICE = ["Facebook", "Instagram", "Youtube", "LinkedIn", "Twitter"]
DTYPE_DICT = {}
POST_LEVEL_METRICS_MANDATORY = json.loads(config[PARAM_CHOICE[user_choice]]["post_level_metrics_mandatory"])
METRICS_TO_NORMALIZE = json.loads(config[PARAM_CHOICE[user_choice]]["metrics_to_normalize"])
METRICS_TO_NORMALIZE_INTO = json.loads(config[PARAM_CHOICE[user_choice]]["metrics_to_normalize_into"])
METRICS_TO_NORMALIZE_MAPPING = []

for i in enumerate(METRICS_TO_NORMALIZE_INTO):
    METRICS_TO_NORMALIZE_MAPPING.append([METRICS_TO_NORMALIZE[i[0]], METRICS_TO_NORMALIZE_INTO[i[0]]])


def read_gen_config():

    """Reading the generic configuration parameters"""

    secret = boto3.client("ssm")
    user_id = secret.get_parameter(Name="srmg_socialbakers_secret_param_user_id", WithDecryption=True)["Parameter"]["Value"]
    password = secret.get_parameter(Name="srmg_socialbakers_secret_param_password", WithDecryption=True)["Parameter"]["Value"]

    SYNC_TYPE = int(config["Default"]["sync_type"])

    if SYNC_TYPE == 1:
        print("Historical Run")
        start_date = dt.strptime(config["Default"]["start_date"], "%Y-%m-%d").date()
        end_date = dt.strptime(config["Default"]["end_date"], "%Y-%m-%d").date()

    else:
        print("Daily Sync-up")
        start_date = d.today() - timedelta(days=8)
        end_date = d.today() - timedelta(days=1)

    yrs_in_req_period = list(range(start_date.year, end_date.year + 1))

    limit = config["Default"]["post_level_limit"]
    post_url = json.loads(config["Default"]["post_url"])
    brand_vertical_mapping_url = json.loads(config["Default"]["brand_vertical_mapping_url"])
    post_level_metrics_sort = json.loads(config["Default"]["post_level_metrics_sort"])

    return [user_id, password, user_choice, start_date, end_date, yrs_in_req_period, limit, post_url, brand_vertical_mapping_url, post_level_metrics_sort]


def read_sm_config(p_user_choice):

    """Reading the social media configuration parameters"""

    user_choice = p_user_choice - 1

    config = cp.ConfigParser()
    config.read("config.ini")

    profile_ids = json.loads(config[PARAM_CHOICE[user_choice]]["profile_ids"])
    post_level_metrics = json.loads(config[PARAM_CHOICE[user_choice]]["post_level_metrics"])

    global POST_LEVEL_METRICS_MANDATORY, POST_LEVEL_METRICS_MANDATORY_DATATYPE, DTYPE_DICT

    POST_LEVEL_METRICS_MANDATORY_DATATYPE = json.loads(config[PARAM_CHOICE[user_choice]]["post_level_metrics_mandatory_datatype"])

    for i in enumerate(POST_LEVEL_METRICS_MANDATORY):
        DTYPE_DICT[POST_LEVEL_METRICS_MANDATORY[i[0] - 1]] = POST_LEVEL_METRICS_MANDATORY_DATATYPE[i[0] - 1]

    return profile_ids, post_level_metrics


def get_brand_vertical_mapping(p_brand_vertical_mapping_url):

    """Fetching the brand and vertical information"""

    brand_vertical_mapping_url = p_brand_vertical_mapping_url
    brand_vertical_df = pd.DataFrame()

    try:
        obj = s3.Object("srmg-datalake", "social_media_metrics/social_bakers/brand_vertical_mapping.json")
        brand_vertical_json = json.load(obj.get()["Body"])
        for i in range(len(brand_vertical_json)):
            brand_vertical_json[i]["instagram_profile_id"] = str(brand_vertical_json[i]["instagram_profile_id"])
        brand_vertical_df = pd.DataFrame(brand_vertical_json)
        brand_vertical_df["facebook_profile_id"] = brand_vertical_df["facebook_profile_id"].astype("Int64")
        brand_vertical_df["twitter_profile_id"] = brand_vertical_df["twitter_profile_id"].astype("Int64")
        brand_vertical_df = brand_vertical_df.astype("string")

    except Exception as gen_exception:
        print("Brand Vertical Mapping Read Error: ", gen_exception)
        return None

    return brand_vertical_df


def date_limit_handler(p_start_date, p_end_date, p_user_choice):

    """Handling 1 year limit on the API"""

    start_date = p_start_date
    end_date = p_end_date
    date_diff = end_date - start_date
    user_choice = p_user_choice - 1

    date_diff_bal = date_diff.days % 365
    date_diff_yrs = int(date_diff.days / 365)

    start_date_arr = []
    end_date_arr = []

    if SYNC_TYPE == 1 and user_choice in [0, 1, 2]:

        if date_diff.days > 365:

            add_days = 364
            end_date = start_date + timedelta(days=add_days)
            start_date_arr = [start_date]
            end_date_arr = [end_date]

            for i in range(date_diff_yrs):

                if i == date_diff_yrs - 1:
                    add_days = date_diff_bal

                start_date = end_date + timedelta(days=1)
                end_date = start_date + timedelta(days=add_days)
                start_date_arr.append(start_date)
                end_date_arr.append(end_date)
        else:

            start_date_arr = [start_date]
            end_date_arr = [end_date]
    else:

        for i in range(date_diff.days + 1):
            start_date_arr.append(start_date.strftime("%Y-%m-%d"))
            end_date_arr.append(start_date.strftime("%Y-%m-%d"))
            start_date = start_date + timedelta(days=1)
            end_date = start_date

    print("\nStart_date: ", start_date_arr[0])
    print("End_date: ", end_date_arr[-1])
    print("Year Segmentation: ", date_diff_yrs + 1, "\n")

    return start_date_arr, end_date_arr


def get_api_response(p_user_id, p_password, p_user_choice, p_start_date_arr, p_end_date_arr, p_profile_ids, p_post_level_metrics, p_limit, p_post_level_metrics_sort):

    """Making the API Calls"""

    user_id = p_user_id
    password = p_password
    user_choice = p_user_choice - 1
    start_date_arr = p_start_date_arr
    end_date_arr = p_end_date_arr
    post_level_metrics = p_post_level_metrics
    profile_ids = p_profile_ids
    limit = p_limit
    post_level_metrics_sort = p_post_level_metrics_sort
    post_metrics_response = []

    print("\nFetching API Response...")
    starting_time = t.time()
    api_url_smp_part = {0: "facebook/page/posts", 1: "instagram/profile/posts", 2: "youtube/profile/videos", 3: "linkedin/profile/posts", 4: "twitter/profile/tweets"}
    next_val = None
    k = 0

    for i in enumerate(start_date_arr):
        start_date = start_date_arr[i[0]]
        end_date = end_date_arr[i[0]]
        post_metric_body_json = {"profiles": profile_ids, "date_start": str(start_date), "date_end": str(end_date), "fields": post_level_metrics, "limit": int(limit)}

        if user_choice != 4:
            post_metric_body_json["sort"] = post_level_metrics_sort

        while True:

            if next_val:
                post_metric_body_json = {"after": next_val}

            try:
                response = req.post("https://api.socialbakers.com/3/" + api_url_smp_part[user_choice], auth=(user_id, password), json=post_metric_body_json, timeout=10)
                temp = response.json()

            except Exception as gen_exception:
                print("API Call Exception: ", gen_exception)
                break

            if SYNC_TYPE == 2:
                print("\nDate:", start_date)

            k += 1

            if temp["success"] is True:

                if "next" in list(temp["data"].keys()):
                    next_val = json.dumps(temp["data"]["next"])
                    remaining_val = int(temp["data"]["remaining"])

                    for i in temp["data"]["posts"]:
                        post_metrics_response.append({"start_date": start_date, "end_date": end_date, "api_response": i, "iteration": int(k), "next": next_val, "remaining": remaining_val})

                    print("remaining_val:", remaining_val)

                else:

                    for i in temp["data"]["posts"]:
                        post_metrics_response.append({"start_date": start_date, "end_date": end_date, "api_response": i, "iteration": int(k), "next": None, "remaining": None})

                    print("remaining_val:", None)
                    next_val = None
                    break

            else:
                print(temp)
                print("API response received!\n")
                ending_time = t.time()
                elapsed_time = ending_time - starting_time
                print("API Response Time:", elapsed_time, "seconds", "\n")
                print("Total API calls made:", k, "\n")

                return post_metrics_response

    print("API response received!\n")
    ending_time = t.time()
    elapsed_time = ending_time - starting_time
    print("API Response Time:", elapsed_time, "seconds", "\n")
    print("Total API calls made:", k, "\n")

    return post_metrics_response


def post_level_metrics_fetcher(p_user_choice, p_post_metrics_response, p_post_url):

    """Stitching the API results together"""

    user_choice = p_user_choice - 1
    post_metrics_response = p_post_metrics_response
    post_url = p_post_url

    print("Stitching data together...")
    post_metrics_response_df = pd.DataFrame(post_metrics_response)

    if not post_metrics_response_df.empty:
        file_name = PARAM_CHOICE[user_choice].lower() + "_raw_" + dt.strftime(dt.today(), "%Y_%m_%d_%I_%M_%S") + ".txt"
        # s3_url = post_url + "raw/" + PARAM_CHOICE[user_choice].lower() + "_post/" + file_name
        # wr.s3.to_csv(post_metrics_response_df.loc[:, ["api_response"]], s3_url)
        post_metrics_response_df = pd.concat([post_metrics_response_df, post_metrics_response_df.api_response.apply(pd.Series)], axis=1)
        post_metrics_response_df.drop(columns=["api_response"], inplace=True)

        for i in POST_LEVEL_METRICS_MANDATORY:

            if i not in post_metrics_response_df:
                post_metrics_response_df[i] = pd.Series()

        post_metrics_response_df = post_metrics_response_df[POST_LEVEL_METRICS_MANDATORY]

    print("Data stitching completed!", "\n")

    return post_metrics_response_df


def beautify_post_metrics(p_user_choice, p_brand_vertical_df, p_post_metrics_response_df):

    """Formatting the unnested full post metrics dataframe"""

    user_choice = p_user_choice - 1
    brand_vertical_df = p_brand_vertical_df
    post_metrics_response_df = p_post_metrics_response_df

    if not post_metrics_response_df.empty:

        if user_choice not in [2, 3]:
            post_metrics_response_df["profileId"] = post_metrics_response_df["profileId"].astype("Int64")
            post_metrics_response_df["profileId"] = post_metrics_response_df["profileId"].astype("string")

        post_metrics_response_df = post_metrics_response_df.astype(DTYPE_DICT)

        for i in METRICS_TO_NORMALIZE:
            temp = post_metrics_response_df[i].apply(pd.Series)

            if list(temp.columns) != METRICS_TO_NORMALIZE_MAPPING[METRICS_TO_NORMALIZE.index(i)][1]:
                temp = pd.DataFrame(columns=METRICS_TO_NORMALIZE_MAPPING[METRICS_TO_NORMALIZE.index(i)][1])
            temp.columns = [str(i) + "_" + str(j) for j in temp.columns]
            post_metrics_response_df = pd.concat([post_metrics_response_df, temp], axis=1)
            post_metrics_response_df.drop(columns=[i], inplace=True)

        profile_id_col_name = PARAM_CHOICE[user_choice].lower() + "_profile_id"
        post_metrics_response_df = pd.merge(how="right", right=post_metrics_response_df, left=brand_vertical_df.loc[:, ["brand", "vertical", profile_id_col_name]].drop_duplicates().reset_index(drop=True), right_on=["profileId"], left_on=[profile_id_col_name])
        post_metrics_response_df.insert(0, "profile_id", post_metrics_response_df["profileId"])
        post_metrics_response_df.insert(1, "post_id", post_metrics_response_df["id"])
        post_metrics_response_df.drop(columns=["profileId", "id", profile_id_col_name], inplace=True)
        post_metrics_response_df.insert(2, "month", pd.Series())
        post_metrics_response_df.insert(2, "year", pd.Series())
        post_metrics_response_df.insert(2, "date", pd.Series())
        post_metrics_response_df.insert(2, "updated_date", dt.today())
        post_metrics_response_df["date"] = pd.to_datetime(post_metrics_response_df["created_time"]).dt.date
        post_metrics_response_df["year"] = pd.to_datetime(post_metrics_response_df["created_time"]).dt.year
        post_metrics_response_df["month"] = pd.to_datetime(post_metrics_response_df["created_time"]).dt.strftime("%b")
        print("Last date in set:", post_metrics_response_df["date"].max())

    return post_metrics_response_df


def file_sync(p_user_choice, p_yrs_in_req_period, p_post_metrics_response_df, p_post_url):

    """Exporting data to Parquet files year-wise and handling daily sync updates"""

    user_choice = p_user_choice - 1
    yrs_in_req_period = p_yrs_in_req_period
    post_metrics_response_df = p_post_metrics_response_df
    post_url = p_post_url

    if not post_metrics_response_df.empty:
        print("Exporting files...")

        for i in yrs_in_req_period:
            current_post_metrics_response_df = post_metrics_response_df[post_metrics_response_df["year"] == i].reset_index(drop=True)
            file_name = str(i) + ".parquet"
            s3_url = post_url + "processed/" + PARAM_CHOICE[user_choice].lower() + "_post/" + file_name
            print("Target s3 location:", s3_url)

            try:
                prev_post_metrics_response_df = wr.s3.read_parquet(path=[s3_url])
                print("Size of current response", current_post_metrics_response_df.shape)
                print("Size of existing file", prev_post_metrics_response_df.shape)
                current_post_metrics_response_df = pd.concat([prev_post_metrics_response_df, current_post_metrics_response_df], axis=0).reset_index(drop=True)
                print("Size of merged file", current_post_metrics_response_df.shape)
                current_post_metrics_response_df = current_post_metrics_response_df.sort_values(by=["post_id", "updated_date"]).drop_duplicates(subset=["post_id"], keep="last").reset_index(drop=True)
                print("Size of merged file without duplicates", current_post_metrics_response_df.shape)

            except Exception as gen_exception:
                print("S3 bucket File Read Exception ", gen_exception, " occured while reading parquet file in ", s3_url)
                return None

            # try:
            #     wr.s3.to_parquet(current_post_metrics_response_df, s3_url)

            # except Exception as gen_exception:
            #     print("S3 bucket File Write Exception ", gen_exception, " occured while writing to parquet file in ", s3_url)
            #     return None

        print("File export / update completed!")

    else:
        print("No data to export")

    return 1


def main():
    # def main():

    """Main Function"""

    global PARAM_CHOICE

    [user_id, password, user_choice, start_date, end_date, yrs_in_req_period, limit, post_url, brand_vertical_mapping_url, post_level_metrics_sort] = read_gen_config()
    brand_vertical_df = get_brand_vertical_mapping(brand_vertical_mapping_url)

    if brand_vertical_df.empty:
        print("Exiting post metrics migration due to missing brand_vertical_mapping.json file in location ", brand_vertical_mapping_url)
        return 1

    start_date_arr, end_date_arr = date_limit_handler(start_date, end_date, user_choice)

    user_choice = event["user_choice"]

    if user_choice == 0:
        st_overall = t.time()
        print("Overall Starting Time:", t.strftime("%I:%M:%S %p"))

        for i in range(1, 5):
            print("Fetching Post Level Metrics for ", PARAM_CHOICE[i - 1])
            starting_time = t.time()
            print("Execution Starting Time:", t.strftime("%I:%M:%S %p"))
            profile_ids, post_level_metrics = read_sm_config(i)
            post_metrics_response = get_api_response(user_id, password, i, start_date_arr, end_date_arr, profile_ids, post_level_metrics, limit, post_level_metrics_sort)
            post_metrics_response_df = post_level_metrics_fetcher(i, post_metrics_response, post_url)
            post_metrics_response_df = beautify_post_metrics(i, brand_vertical_df, post_metrics_response_df)
            file_sync_status = file_sync(i, yrs_in_req_period, post_metrics_response_df, post_url)

            if file_sync_status is None:
                print("Exiting post metrics migration due to File Write error in AWS S3 location ", post_url)
                return 1

            ending_time = t.time()
            print("Execution Ending Time:", t.strftime("%I:%M:%S %p"))
            elapsed_time = (ending_time - starting_time) / 60
            print("Total execution time:", elapsed_time, "minutes")

        et_overall = t.time()
        print("Overall Ending Time:", t.strftime("%I:%M:%S %p"))
        elapsed_time = (et_overall - st_overall) / 60
        print("Total execution time:", elapsed_time, "minutes")

    else:
        print("Fetching Post Level Metrics for ", PARAM_CHOICE[user_choice - 1])
        starting_time = t.time()
        print("Execution Starting Time:", t.strftime("%I:%M:%S %p"))
        profile_ids, post_level_metrics = read_sm_config(user_choice)
        post_metrics_response = get_api_response(user_id, password, user_choice, start_date_arr, end_date_arr, profile_ids, post_level_metrics, limit, post_level_metrics_sort)
        post_metrics_response_df = post_level_metrics_fetcher(user_choice, post_metrics_response, post_url)
        post_metrics_response_df = beautify_post_metrics(user_choice, brand_vertical_df, post_metrics_response_df)
        file_sync_status = file_sync(user_choice, yrs_in_req_period, post_metrics_response_df, post_url)

        if file_sync_status is None:
            print("Exiting post metrics migration due to File Write error in AWS S3 location ", post_url)
            return 1

        ending_time = t.time()
        elapsed_time = (ending_time - starting_time) / 60
        print("Execution Ending Time:", t.strftime("%I:%M:%S %p"))
        print("Total execution time:", elapsed_time, "minutes")

    print("Done")
    return 1


# if __name__ == "__main__":
#     main(event, lambda_context)

if __name__ == "__main__":
    main()
