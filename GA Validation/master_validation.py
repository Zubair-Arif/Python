
from datetime import date, datetime as dt, timedelta
import json
import configparser as cp
import calendar
import requests as req
import awswrangler as wr
import boto3
import pandas as pd
from pretty_html_table import build_table
from validation_script.send_mail import send_email
import IO_TECH_ETL_Scripts.test.validate as IOTech

s3 = boto3.resource("s3")
config = cp.ConfigParser()
config.read("validation_script/config.ini")
PARAM_CHOICE = ["Facebook", "Instagram", "Youtube", "LinkedIn", "Twitter"]

SYNC_TYPE = int(config["Default"]["sync_type"])


def read_gen_config():

    """Reading data from config file"""

    secret = boto3.client("ssm")
    user_id = secret.get_parameter(Name="srmg_socialbakers_secret_param_user_id", WithDecryption=True)["Parameter"]["Value"]
    password = secret.get_parameter(Name="srmg_socialbakers_secret_param_password", WithDecryption=True)["Parameter"]["Value"]

    global from_email, to_email, cc_email, email_pass, log_list, log_link, verticals, tables_name, vertical_brand_url, s3_ga_url
    log_list = []

    user_choice = int(config["Default"]["user_choice"])

    if SYNC_TYPE == 1:
        start_date = dt.strptime(config["Default"]["start_date"], "%Y-%m-%d").date()
        end_date = dt.strptime(config["Default"]["end_date"], "%Y-%m-%d").date()
    else:
        start_date = date.today() - timedelta(days=8)
        end_date = date.today() - timedelta(days=2)

    limit = int(config["Default"]["post_level_limit"])
    post_url = json.loads(config["Default"]["post_url"])
    post_api_url = json.loads(config["Default"]["post_api_url"])
    post_level_metrics_sort = json.loads(config["Default"]["post_level_metrics_sort"])
    profile_ids = json.loads(config[PARAM_CHOICE[user_choice - 1]]["profile_ids"])
    post_level_metrics = json.loads(config[PARAM_CHOICE[user_choice - 1]]["post_level_metrics"])
    brand_vertical_mapping_url = json.loads(config["Default"]["brand_vertical_mapping_url"])
    from_email = config["Default"]["from_email"]
    to_email = json.loads(config["Default"]["to_email"])
    cc_email = json.loads(config["Default"]["cc_email"])
    email_pass = secret.get_parameter(Name="srmg_validation_daily_report_email_pass", WithDecryption=True)["Parameter"]["Value"]
    log_link = config["Default"]["log_link"]
    verticals = json.loads(config["Default"]["verticals"])
    tables_name = json.loads(config["Default"]["tables_name"])
    vertical_brand_url = json.loads(config["Default"]["vertical_brand_url"])
    s3_ga_url = config["Default"]["s3_ga_url"]

    return [user_id, password, user_choice, start_date, end_date, limit, post_url, post_api_url, post_level_metrics_sort, profile_ids, post_level_metrics, brand_vertical_mapping_url]


def get_brand_vertical_mapping(p_brand_vertical_mapping_url):

    """Fetching the brand and vertical information"""

    brand_vertical_mapping_url = p_brand_vertical_mapping_url
    brand_vertical_df = pd.DataFrame()

    try:
        obj = s3.Object(brand_vertical_mapping_url[5:18], brand_vertical_mapping_url[19:])
        brand_vertical_json = json.load(obj.get()["Body"])
        for i in range(len(brand_vertical_json)):
            brand_vertical_json[i]["instagram_profile_id"] = str(brand_vertical_json[i]["instagram_profile_id"])
        brand_vertical_df = pd.DataFrame(brand_vertical_json)
        brand_vertical_df["facebook_profile_id"] = brand_vertical_df["facebook_profile_id"].astype("Int64")
        brand_vertical_df["twitter_profile_id"] = brand_vertical_df["twitter_profile_id"].astype("Int64")
        brand_vertical_df = brand_vertical_df.astype("string")

    except Exception as gen_exception:
        log = "Brand Vertical Mapping Read Error: " + gen_exception
        log_list.append(log)
        print("Brand Vertical Mapping Read Error: ", gen_exception)
        return brand_vertical_df

    return brand_vertical_df


def get_cloud_data(p_date, p_prev_date, p_next_date, p_user_choice, p_post_url):

    """Reading data from AWS S3 bucket"""

    year = p_date
    user_choice = p_user_choice - 1
    post_url = p_post_url
    prev_date = p_prev_date
    next_date = p_next_date
    df_s3_data = pd.DataFrame()
    try:
        df_s3_data = wr.s3.read_parquet(path=post_url + "processed/" + PARAM_CHOICE[user_choice].lower() + "_post/" + str(year) + ".parquet")
        df_s3_data = df_s3_data.loc[(df_s3_data["date"] > prev_date) & (df_s3_data["date"] < next_date)]
    except Exception as gen_exception:
        log = "Exception" + str(gen_exception) + "has occured while reading the s3 file" + post_url
        print("Exception", gen_exception, "has occured while reading the s3 file")
        try:
            log_list.append(log)
        except:
            pass
        return df_s3_data

    return df_s3_data


def get_api_response(p_user_id, p_password, p_user_choice, p_start_date, p_end_date, p_next_date, p_post_api_url, p_profile_ids, p_post_level_metrics, p_limit, p_post_level_metrics_sort):

    """Getting api response and return the exact count between start date and end date"""

    user_id = p_user_id
    password = p_password
    user_choice = p_user_choice - 1
    start_date = p_start_date
    end_date = p_end_date
    next_date = p_next_date
    post_level_metrics = p_post_level_metrics
    profile_ids = p_profile_ids
    limit = p_limit
    post_api_url = p_post_api_url
    post_level_metrics_sort = p_post_level_metrics_sort
    start_end_date_list = [start_date, next_date]

    param_choice_url = {0: "facebook/page/posts", 1: "instagram/profile/posts", 2: "youtube/profile/videos", 3: "linkedin/profile/posts", 4: "twitter/profile/tweets"}
    next_val = None
    k = 0
    post_metrics_response_nested = []
    post_metric_body_json = {"profiles": profile_ids, "date_start": str(start_date), "date_end": str(end_date), "fields": post_level_metrics, "limit": int(limit)}
    url = post_api_url + param_choice_url[user_choice]
    try:
        api_response = req.post(url, auth=(user_id, password), json=post_metric_body_json)
    except Exception as gen_exception:
        print("API call exception", gen_exception)
        return -1
    data = api_response.text
    parse_json = json.loads(data)
    if parse_json["success"]:
        if "next" in list(parse_json["data"].keys()):
            total_val = int(parse_json["data"]["remaining"]) + limit
        else:
            total_val = len(parse_json["data"]["posts"])
        for i in start_end_date_list:
            post_metrics_response = []
            post_metric_body_json = {"profiles": profile_ids, "date_start": str(i), "date_end": str(i), "fields": post_level_metrics, "limit": int(limit)}
            if user_choice != 4:
                post_metric_body_json["sort"] = post_level_metrics_sort
            while True:
                if next_val:
                    post_metric_body_json = {"after": next_val}
                try:
                    response = req.post(url, auth=(user_id, password), json=post_metric_body_json)
                except Exception as gen_exception:
                    print("API call exception", gen_exception)
                    return -1
                temp = response.json()
                k += 1
                if temp["success"] == True:
                    if "next" in list(temp["data"].keys()):
                        next_val = json.dumps(temp["data"]["next"])
                        remaining_val = int(temp["data"]["remaining"])
                        for j in temp["data"]["posts"]:
                            post_metrics_response.append({"api_response": j, "iteration": int(k), "next": next_val, "remaining": remaining_val})
                        temp_df = pd.DataFrame(temp["data"]["posts"])
                        temp_df_count = temp_df.loc[temp_df["created_time"] > str(i)[0:10]].shape[0]
                        if temp_df_count != 0:
                            next_val = None
                            break
                    else:
                        for j in temp["data"]["posts"]:
                            post_metrics_response.append({"api_response": j, "iteration": int(k), "next": None, "remaining": None})
                        next_val = None
                        break

                else:
                    return -1
            post_metrics_response_nested.append(post_metrics_response)
    else:
        return -1
    df_post_metric_response_start_date = pd.DataFrame(post_metrics_response_nested[0])
    if not df_post_metric_response_start_date.empty:
        df_api_response_start_date = pd.DataFrame(df_post_metric_response_start_date.api_response.apply(pd.Series))
        filtered_prev_year_count = df_api_response_start_date.loc[df_api_response_start_date["created_time"] < str(start_date)].shape[0]
    else:
        filtered_prev_year_count = 0
    df_post_metric_response_end_date = pd.DataFrame(post_metrics_response_nested[1])
    if not df_post_metric_response_end_date.empty:
        df_api_response_end_date = pd.DataFrame(df_post_metric_response_end_date.api_response.apply(pd.Series))
        filtered_next_day_count = df_api_response_end_date.loc[df_api_response_end_date["created_time"] < str(next_date)].shape[0]
    else:
        filtered_next_day_count = 0
    count_in_api_call = (total_val - filtered_prev_year_count) + filtered_next_day_count
    return count_in_api_call


def historical_source_targer_count_check(s3_df, count_difference, today_date, user_id, password, user_choice, start_date, end_date, next_date, post_api_url, profile_ids, post_level_metrics, limit, post_level_metrics_sort):
    if count_difference < 100:
        return True
    else:
        print("Data count not matching for the year ", start_date.year)
        print("Count Difference         :", count_difference)
        print("Started checking month wise...")
        print("Count for s3 and API month wise for the year:", start_date.year)
        s3_data_month_wise_count = s3_df.groupby(s3_df.created_time.dt.month)["post_id"].count()
        s3_data_month_wise_count_list = []
        for i in range(1, 13):
            try:
                s3_data_month_wise_count_list.append(s3_data_month_wise_count.loc[[i]].values[0])
            except:
                s3_data_month_wise_count_list.append(0)
        source_data_month_wise_list = []
        for i in range(1, 13):
            start_date_month = dt.strptime(str(start_date.year) + "-" + str(i) + "-" + "01", "%Y-%m-%d").date()
            last_date_of_month = calendar.monthrange(start_date.year, i)[1]
            end_date_month = dt.strptime(str(start_date.year) + "-" + str(i) + "-" + str(last_date_of_month), "%Y-%m-%d").date()
            next_date_month = end_date_month + timedelta(days=1)
            if next_date_month <= today_date - timedelta(days=8):
                count_in_api_call = get_api_response(user_id, password, user_choice, start_date_month, end_date_month, next_date_month, post_api_url, profile_ids, post_level_metrics, limit, post_level_metrics_sort)
                if count_in_api_call != 0:
                    source_data_month_wise_list.append(count_in_api_call)
                else:
                    return False
            elif start_date_month < today_date:
                end_date_month = end_date
                next_date_month = end_date_month + timedelta(days=1)
                count_in_api_call = get_api_response(user_id, password, user_choice, start_date_month, end_date_month, next_date_month, post_api_url, profile_ids, post_level_metrics, limit, post_level_metrics_sort)
                if count_in_api_call != -1:
                    source_data_month_wise_list.append(count_in_api_call)
                else:
                    return False
            else:
                if len(source_data_month_wise_list) != 12:
                    for i in range(len(source_data_month_wise_list), 12):
                        source_data_month_wise_list.append(0)
                    break
        data_count_month_wise = {"month": ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], "API_count": source_data_month_wise_list, "s3_count": s3_data_month_wise_count_list}

        df_data_count_month_wise = pd.DataFrame(data_count_month_wise)
        df_data_count_month_wise["Count_diff"] = abs(df_data_count_month_wise["API_count"] - df_data_count_month_wise["s3_count"])
        df_mismatched_month = df_data_count_month_wise.loc[df_data_count_month_wise["Count_diff"] != 0]
        df_mismatched_month.index += 1
        print(df_mismatched_month)
        print("Started checking week wise...")
        mismatch_month = []
        for i in range(12):
            if source_data_month_wise_list[i] != s3_data_month_wise_count_list[i]:
                mismatch_month.append(i + 1)
        s3_month_week_list = []
        for i in mismatch_month:
            s3_df_month = s3_df.loc[(s3_df.created_time.dt.month == i)]
            df_s3_month_day = s3_df_month.groupby(s3_df_month.created_time.dt.day)["post_id"].count()
            last_date_of_month = int(calendar.monthrange(start_date.year, i)[1])
            if end_date.month == i and end_date.year == today_date.year:
                last_date_of_month = int(end_date.day)
            s3_week_list = []
            day_group = 0
            a = 7
            for j in range(1, last_date_of_month + 1):
                try:
                    day_group += df_s3_month_day.loc[[j]].values[0]
                    if j == a or j == last_date_of_month:
                        s3_week_list.append(day_group)
                        day_group = 0
                        a += 7
                except:
                    day_group += 0
                    if j == a or j == last_date_of_month:
                        s3_week_list.append(day_group)
                        day_group = 0
                        a += 7
            for k in range(len(s3_week_list), 5):
                s3_week_list.append(0)
            s3_month_week_list.append(s3_week_list)
        source_data_week_wise_count_list_nested = []
        for i in mismatch_month:
            date_format = "%Y-%m-%d"
            d1 = dt.strptime(str(start_date.year) + "-" + str(i) + "-01", date_format).date()
            last_date_of_month = calendar.monthrange(start_date.year, i)[1]
            if today_date.month == i and today_date.year == end_date.year:
                last_date_of_month = end_date.day
            d2 = dt.strptime(str(start_date.year) + "-" + str(i) + "-" + str(last_date_of_month), date_format).date()
            d = d1
            step = timedelta(days=7)
            source_data_week_wise_list = []
            count = 0
            while d <= d2:
                count += 1
                start_date_week = d
                end_date_week = d + timedelta(days=6)
                if count == 5:
                    end_date_week = d2
                next_date_week = end_date_week + timedelta(days=1)
                if today_date.month == i and today_date.year == end_date.year and end_date_week >= end_date:
                    end_date_week = end_date
                count_in_api_call = get_api_response(user_id, password, user_choice, start_date_week, end_date_week, next_date_week, post_api_url, profile_ids, post_level_metrics, limit, post_level_metrics_sort)
                if count_in_api_call != -1:
                    source_data_week_wise_list.append(count_in_api_call)
                    d += step
                else:
                    return False
            for k in range(len(source_data_week_wise_list), 5):
                source_data_week_wise_list.append(0)
            source_data_week_wise_count_list_nested.append(source_data_week_wise_list)
        df_week_wise_count = pd.DataFrame()
        j = 0
        for i in mismatch_month:
            datetime_object = dt.strptime(str(i), "%m")
            month_name = datetime_object.strftime("%b")
            data = {"month": [month_name, month_name, month_name, month_name, month_name], "week": ["week 1", "week 2", "week 3", "week 4", "week 5"], "API_count": source_data_week_wise_count_list_nested[j], "s3_count": s3_month_week_list[j]}
            df_temp = pd.DataFrame(data)
            df_week_wise_count = pd.concat([df_week_wise_count, df_temp])
            j += 1
        df_week_wise_count["Count_diff"] = abs(df_week_wise_count["API_count"] - df_week_wise_count["s3_count"])
        df_mismatched_week_wise = df_week_wise_count.loc[df_week_wise_count["Count_diff"] != 0]
        df_mismatched_week_wise.index += 1
    print(df_mismatched_week_wise)
    return True


def source_target_count_check(p_user_id, p_password, p_user_choice, p_prev_date, p_start_date, p_end_date, p_next_date, p_limit, p_post_api_url, p_post_level_metrics_sort, p_profile_ids, p_post_level_metrics, p_s3_df):

    """Validating count between source and target"""

    user_id = p_user_id
    password = p_password
    user_choice = p_user_choice
    start_date = p_start_date
    end_date = p_end_date
    post_level_metrics = p_post_level_metrics
    profile_ids = p_profile_ids
    limit = p_limit
    post_api_url = p_post_api_url
    post_level_metrics_sort = p_post_level_metrics_sort
    s3_df = p_s3_df
    prev_date = p_prev_date
    next_date = p_next_date
    today_date = date.today()

    count_in_api_call = get_api_response(user_id, password, user_choice, start_date, end_date, next_date, post_api_url, profile_ids, post_level_metrics, limit, post_level_metrics_sort)
    if count_in_api_call != -1:
        total_s3_count = s3_df.shape[0]
        count_difference = abs(count_in_api_call - total_s3_count)
        if SYNC_TYPE == 1:
            flag = historical_source_targer_count_check(s3_df, count_difference, today_date, user_id, password, user_choice, start_date, end_date, next_date, post_api_url, profile_ids, post_level_metrics, limit, post_level_metrics_sort)
            return flag
        elif SYNC_TYPE == 2:
            if count_in_api_call == total_s3_count:
                print("source target count check : PASSED")
            else:
                log = PARAM_CHOICE[user_choice - 1] + "_post source target count check : FAILED"
                log_list.append(log)
                print("source target count check : FAILED")
                print("Daily sync count not matching")
                print("Count in API Call        :", count_in_api_call)
                log = "Count in API Call        :" + str(count_in_api_call)
                log_list.append(log)
                log = "Total count in S3 Bucket :" + str(total_s3_count)
                log_list.append(log)
                print("Total count in S3 Bucket :", total_s3_count)
                log = "Count Difference         :" + str(abs(count_in_api_call - total_s3_count))
                log_list.append(log)
                print("Count Difference         :", abs(count_in_api_call - total_s3_count))
            return True
    else:
        return False


def year_limit_validate(p_year, p_start_date, p_end_date, p_df_s3_data):

    """Checks whether the year range in the AWS S3 Bucket is matching with the given year or not"""
    year = p_year
    start_date = p_start_date
    end_date = p_end_date
    df_s3_data = p_df_s3_data
    post_id = pd.DataFrame()
    temp = pd.DataFrame(df_s3_data[(df_s3_data["date"] < start_date) | (df_s3_data["date"] > end_date)])
    if temp.shape[0] > 0:
        post_id = temp
    return post_id


def dupe_check(p_s3_df):

    """checking duplicates and return the dataframe"""

    df_s3 = p_s3_df
    dup_df = df_s3[df_s3.duplicated()]
    if dup_df.shape[0] == 0:
        dup_df = pd.DataFrame()
    return dup_df


def extract_date_validate(p_df_s3_data):

    """Checks whether the created time and extracted date,month and year are matching with the given year or not"""

    df_s3_data = p_df_s3_data

    matched_post_id = pd.DataFrame()

    year_check = df_s3_data[df_s3_data.created_time.dt.year != df_s3_data.year]
    month_check = df_s3_data[df_s3_data.created_time.dt.strftime("%b") != df_s3_data.month]
    date_check = df_s3_data[df_s3_data.created_time.dt.date != df_s3_data.date]

    if year_check.shape[0] > 0:
        return "year", year_check

    elif month_check.shape[0] > 0:
        return "month", month_check

    elif date_check.shape[0] > 0:
        return "date", date_check
    else:
        return "None", matched_post_id


def validate_brand_vertical_mapping(p_df_brand_vertical_mapping, p_df_s3, p_user_choice):

    """validating brand vertical mapping"""

    df_s3 = p_df_s3
    user_choice = p_user_choice - 1
    df_brand_vertical_mapping_config = p_df_brand_vertical_mapping
    profile_id = PARAM_CHOICE[user_choice].lower() + "_profile_id"
    df_s3_null = df_s3.loc[(df_s3["profile_id"].isnull()) | (df_s3["post_id"].isnull()) | (df_s3["brand"].isnull()) | (df_s3["vertical"].isnull())]
    if df_s3_null.shape[0] == 0:
        df_s3_brand_vertical = df_s3[["profile_id", "brand", "vertical"]].drop_duplicates().reset_index(drop=True)
        df_brand_vertical_config = df_brand_vertical_mapping_config[[profile_id, "brand", "vertical"]].dropna().drop_duplicates().reset_index(drop=True)
        df_brand_vertical_config.rename(columns={profile_id: "profile_id"}, inplace=True)
        df_brand_vertical = pd.merge(df_brand_vertical_config, df_s3_brand_vertical, how="inner")
        if df_brand_vertical.shape[0] == df_s3_brand_vertical.shape[0]:
            df_diff = pd.DataFrame()
            return 1, df_diff
        else:
            df_diff = pd.concat([df_brand_vertical, df_s3_brand_vertical]).drop_duplicates(keep=False)
            return 2, df_diff
    else:
        return 3, df_s3_null


def social_bakers_validation():

    """social bakers validation main Function"""

    [user_id, password, user_choice, start_date, end_date, limit, post_url, post_api_url, post_level_metrics_sort, profile_ids, post_level_metrics, brand_vertical_mapping_url] = read_gen_config()
    today_date = date.today()
    df_brand_vertical_mapping = get_brand_vertical_mapping(brand_vertical_mapping_url)

    log_b = "post_metrics validation from :" + str(start_date) + " to " + str(end_date)
    log_c = "************************************************************"
    log_list.append(log_b)
    log_list.append(log_c)
    print(log_b)
    print(log_c)
    for user_choice in range(1, 5):

        profile_ids = json.loads(config[PARAM_CHOICE[user_choice - 1]]["profile_ids"])
        post_level_metrics = json.loads(config[PARAM_CHOICE[user_choice - 1]]["post_level_metrics"])
        check = True
        if SYNC_TYPE == 1:
            print(PARAM_CHOICE[user_choice - 1])
            start_year = 2015
            end_year = int(today_date.year)
            for year in range(start_year, end_year + 1):
                check = True
                start_date = dt.strptime(f"{year}-01-01", "%Y-%m-%d").date()
                end_date = dt.strptime(f"{year}-12-31", "%Y-%m-%d").date()
                if year == today_date.year:
                    end_date = end_date = today_date - timedelta(days=8)
                prev_date = start_date - timedelta(days=1)
                next_date = end_date + timedelta(days=1)
                s3_df = get_cloud_data(year, prev_date, next_date, user_choice, post_url)
                if not s3_df.empty:
                    flag = source_target_count_check(user_id, password, user_choice, prev_date, start_date, end_date, next_date, limit, post_api_url, post_level_metrics_sort, profile_ids, post_level_metrics, s3_df)
                    print("validation year :", year)
                    if flag:
                        print("source target count validation : PASSED")
                    else:
                        print("success : False while hitting the API")
                        check = False
                    if check:
                        post_id = year_limit_validate(year, start_date, end_date, s3_df)
                        if len(post_id) > 0:
                            print("Date Out of range")
                            print(post_id)
                        else:
                            print("year Limit validation : PASSED")

                        df_dub_s3_data = dupe_check(s3_df)

                        if df_dub_s3_data.empty:
                            print("Duplicates validation : PASSED ")
                        else:

                            print("Duplicates found for the year : ", year)
                            print(df_dub_s3_data)
                        mismatch_val, mismatched_post_id = extract_date_validate(s3_df)
                        if len(mismatched_post_id) > 0:
                            print("There is a mismatch in the " + mismatch_val)
                            print(mismatched_post_id)
                        else:
                            print("Augmented column validation : PASSED")
                        flag, df_diff = validate_brand_vertical_mapping(df_brand_vertical_mapping, s3_df, user_choice)
                        if flag == 1:
                            print("brand vertical mapping validation : PASSED ")
                        elif flag == 2:
                            print("brand vertical mapping not matching for the year :", year)
                            print(df_diff)
                        else:
                            print("Null values are there brand vertical mapping for the year :", year)
                            print(df_diff)
        elif SYNC_TYPE == 2:
            prev_date = start_date - timedelta(days=1)
            next_date = end_date + timedelta(days=1)
            print()
            print(PARAM_CHOICE[user_choice - 1], "post")
            print("**********************")
            print()
            s3_df = get_cloud_data(start_date.year, prev_date, next_date, user_choice, post_url)
            flag = source_target_count_check(user_id, password, user_choice, prev_date, start_date, end_date, next_date, limit, post_api_url, post_level_metrics_sort, profile_ids, post_level_metrics, s3_df)

            if flag:
                pass
            else:
                print("success : False while hitting the API")
                check = False
            if check:

                post_id = year_limit_validate(start_date.year, start_date, end_date, s3_df)
                if len(post_id) > 0:
                    log = PARAM_CHOICE[user_choice - 1] + "_post year Limit validation : Failed"
                    log_list.append(log)
                    print("year Limit validation : Failed")
                    print("Date Out of range")
                    print(post_id)
                else:
                    print("year Limit validation : PASSED")

                df_dub_s3_data = dupe_check(s3_df)

                if df_dub_s3_data.empty:
                    print("Duplicates validation : PASSED ")
                else:
                    log = PARAM_CHOICE[user_choice - 1] + "_post Duplicates validation : Failed"
                    log_list.append(log)
                    print("Duplicates validation : FAILED ")
                    print("Duplicates found for the year : ", start_date.year)
                    print(df_dub_s3_data)
                mismatch_val, mismatched_post_id = extract_date_validate(s3_df)
                if len(mismatched_post_id) > 0:
                    log = PARAM_CHOICE[user_choice - 1] + "_post augmented column validation FAILED "
                    log_list.append(log)
                    print("There is a mismatch in the ", mismatch_val)
                    print(mismatched_post_id)
                else:
                    print("Augmented column validation : PASSED")
                flag, df_diff = validate_brand_vertical_mapping(df_brand_vertical_mapping, s3_df, user_choice)
                if flag == 1:
                    print("brand vertical mapping validation : PASSED ")
                elif flag == 2:
                    log = PARAM_CHOICE[user_choice - 1] + "brand vertical mapping validation : FAILED "
                    log_list.append(log)
                    print("brand vertical mapping validation : FAILED ")
                    print("brand vertical mapping not matching for the year :", start_date.year)
                    print(df_diff)
                else:
                    log = PARAM_CHOICE[user_choice - 1] + "brand vertical mapping validation : FAILED "
                    log_list.append(log)
                    print("brand vertical mapping validation : FAILED ")
                    print("Null values are there brand vertical mapping for the year :", start_date.year)
                    print(df_diff)
        print("---------------------------------------------------------------------------------")
    print("---------------------------------------------------------------------------------")
    if len(log_list) == 2:
        log_list.append(" All social media post level metrics validation : PASSED")

    return "success"


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
    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_colwidth", 100)
    iotech_mismatch_df, iotech_logs = IOTech.validate()
    social_bakers_validation()
    ga_mismatch_df = ga_validation()
    ga_data_df = build_table(ga_mismatch_df, "blue_light")
    iotech_data_df = build_table(iotech_mismatch_df, "blue_light")
    send_email([ga_data_df, iotech_data_df], [log_list, iotech_logs], from_email, to_email, cc_email, email_pass, log_link)

if __name__ == "__main__":
    main()