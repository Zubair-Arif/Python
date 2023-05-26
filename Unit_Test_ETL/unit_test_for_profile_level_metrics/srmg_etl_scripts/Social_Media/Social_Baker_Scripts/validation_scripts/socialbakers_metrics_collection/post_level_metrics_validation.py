from datetime import date as d, datetime as dt, timedelta
import pandas as pd
import json
import requests as req
import awswrangler as wr
import time as t
import configparser as cp
import calendar
import boto3

s3 = boto3.client("s3")

# ************************************************************************************************************************************************** #
#                                                           *** Reading data from config ***                                                            #
# ************************************************************************************************************************************************** #


def read_gen_config():
    config = cp.ConfigParser()
    config.read("config.ini")

    secret = boto3.client("ssm")
    user_id = secret.get_parameter(Name="srmg_socialbakers_secret_param_user_id", WithDecryption=True)["Parameter"]["Value"]
    password = secret.get_parameter(Name="srmg_socialbakers_secret_param_password", WithDecryption=True)["Parameter"]["Value"]

    global sync_type
    sync_type = int(config["Default"]["sync_type"])
    user_choice = int(config["Default"]["user_choice"])

    if sync_type == 1:
        start_date = dt.strptime(config["Default"]["start_date"], "%Y-%m-%d").date()
        end_date = dt.strptime(config["Default"]["end_date"], "%Y-%m-%d").date()
        print("Historical Count check from :", start_date, "to", end_date)
    else:
        print("Daily Sync-up")
        start_date = d.today() - timedelta(days=8)
        end_date = d.today() - timedelta(days=1)

    limit = config["Default"]["post_level_limit"]
    post_url = json.loads(config["Default"]["post_url"])
    post_level_metrics_sort = json.loads(config["Default"]["post_level_metrics_sort"])
    profile_ids = json.loads(config[param_choice[user_choice - 1]]["profile_ids"])
    post_level_metrics = json.loads(config[param_choice[user_choice - 1]]["post_level_metrics"])

    return user_id, password, user_choice, start_date, end_date, limit, post_url, post_level_metrics_sort, profile_ids, post_level_metrics


# ************************************************************************************************************************************************** #
#                                                            *** Getting S3 data ***                                                            #
# ************************************************************************************************************************************************** #


def get_cloud_data(p_date, p_user_choice, p_post_url):

    year = p_date
    user_choice = p_user_choice - 1
    post_url = p_post_url
    s3_url_smp_part = {0: "facebook_post/", 1: "instagram_post/", 2: "youtube_post/", 3: "linkedin_post/", 4: "twitter_post/"}
    try:
        df_s3_data = wr.s3.read_parquet(path=post_url + s3_url_smp_part[user_choice] + str(year) + ".parquet")
    except Exception as e:
        print("Exception", e, "has occured while reading the s3 file")

        return False, pd.DataFrame()

    return True, df_s3_data


# ************************************************************************************************************************************************** #
#                                                            *** Making the API Calls ***                                                            #
# ************************************************************************************************************************************************** #


def get_api_response(p_user_id, p_password, p_user_choice, p_start_date, p_end_date, p_next_date, p_profile_ids, p_post_level_metrics, p_limit, p_post_level_metrics_sort):
    user_id = p_user_id
    password = p_password
    user_choice = p_user_choice - 1
    start_date = p_start_date
    end_date = p_end_date
    next_date = p_next_date
    post_level_metrics = p_post_level_metrics
    profile_ids = p_profile_ids
    limit = p_limit
    post_level_metrics_sort = p_post_level_metrics_sort
    start_end_date_list = [start_date, next_date]

    api_url_smp_part = {0: "facebook/page/posts", 1: "instagram/profile/posts", 2: "youtube/profile/videos", 3: "linkedin/profile/posts", 4: "twitter/profile/tweets"}

    next_val = None
    k = 0
    post_metrics_response_nested = []
    post_metric_body_json = {"profiles": profile_ids, "date_start": str(start_date), "date_end": str(end_date), "fields": post_level_metrics, "limit": int(limit)}
    url = "https://api.socialbakers.com/3/" + api_url_smp_part[user_choice]
    response_API = req.post(url, auth=(user_id, password), json=post_metric_body_json)
    data = response_API.text
    parse_json = json.loads(data)
    if parse_json["success"]:

        total_val = int(parse_json["data"]["remaining"]) + int(post_metric_body_json["limit"])

        for i in start_end_date_list:
            post_metrics_response = []
            post_metric_body_json = {"profiles": profile_ids, "date_start": str(i), "date_end": str(i), "fields": post_level_metrics, "limit": int(limit)}

            if user_choice != 4:
                post_metric_body_json["sort"] = post_level_metrics_sort

            while True:
                if next_val:
                    post_metric_body_json = {"after": next_val}
                response = req.post("https://api.socialbakers.com/3/" + api_url_smp_part[user_choice], auth=(user_id, password), json=post_metric_body_json)
                temp = response.json()

                k += 1
                if temp["success"] == True:
                    if "next" in list(temp["data"].keys()):
                        next_val = json.dumps(temp["data"]["next"])
                        remaining_val = int(temp["data"]["remaining"])
                        for j in temp["data"]["posts"]:
                            post_metrics_response.append({"api_response": j, "iteration": int(k), "next": next_val, "remaining": remaining_val})
                        temp_df = pd.DataFrame(temp["data"]["posts"])

                        temp_df_list = temp_df["created_time"].to_list()
                        temp_df_list = pd.to_datetime(temp_df_list, format="%Y-%m-%d")
                        if str(i)[0:10] in temp_df_list:
                            next_val = None

                            break
                    else:
                        for j in temp["data"]["posts"]:
                            post_metrics_response.append({"api_response": j, "iteration": int(k), "next": None, "remaining": None})
                        next_val = None
                        break

                else:
                    print("status : failed while hitting API")
                    return 0, 0, 0
            post_metrics_response_nested.append(post_metrics_response)
    else:
        print("status : failed while hitting API")
        return 0, 0, 0

    return post_metrics_response_nested[0], post_metrics_response_nested[1], total_val


# ************************************************************************************************************************************************** #
#                                                           *** source target count check  ***                                                                #
# ************************************************************************************************************************************************** #


def source_target_count_check(p_user_id, p_password, p_user_choice, p_start_date, p_end_date, p_limit, p_post_level_metrics_sort, p_profile_ids, p_post_level_metrics, p_s3_df):
    user_id = p_user_id
    password = p_password
    user_choice = p_user_choice
    start_date = p_start_date
    end_date = p_end_date
    post_level_metrics = p_post_level_metrics
    profile_ids = p_profile_ids
    limit = p_limit
    post_level_metrics_sort = p_post_level_metrics_sort
    s3_df = p_s3_df
    next_date = end_date + timedelta(days=1)

    post_metrics_response_start, post_metrics_response_end, total_val = get_api_response(user_id, password, user_choice, start_date, end_date, next_date, profile_ids, post_level_metrics, limit, post_level_metrics_sort)
    if total_val != 0:
        df_post_metric_response_start_date = pd.DataFrame(post_metrics_response_start)
        df_api_response_start_date = pd.DataFrame(df_post_metric_response_start_date.api_response.apply(pd.Series))
        filtered_prev_year_count = df_api_response_start_date.loc[df_api_response_start_date["created_time"] < str(start_date)].shape[0]

        df_post_metric_response_end_date = pd.DataFrame(post_metrics_response_end)
        df_api_response_end_date = pd.DataFrame(df_post_metric_response_end_date.api_response.apply(pd.Series))
        filtered_next_year_count = df_api_response_end_date.loc[df_api_response_end_date["created_time"] < str(next_date)].shape[0]

        total_s3_count = len(s3_df)
        count_in_API_call = (total_val - filtered_prev_year_count) + filtered_next_year_count
        print("Count in API Call        :", count_in_API_call)
        print("Total count in S3 Bucket :", total_s3_count)
        print("Count Difference         :", abs(count_in_API_call - total_s3_count))

        if count_in_API_call == total_s3_count:
            return True
        else:
            print("Data count not matching for the year ", start_date.year)
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
                post_metrics_response_start_date_month, post_metric_response_end_date_month, total_val_month = get_api_response(user_id, password, user_choice, start_date_month, end_date_month, next_date_month, profile_ids, post_level_metrics, limit, post_level_metrics_sort)
                if total_val_month != 0:
                    df_post_metric_response_month_start_date = pd.DataFrame(post_metrics_response_start_date_month)
                    df_api_response_month_start_date = pd.DataFrame(df_post_metric_response_month_start_date.api_response.apply(pd.Series))
                    filtered_prev_month_count = df_api_response_month_start_date.loc[df_api_response_month_start_date["created_time"] < str(start_date_month)].shape[0]

                    df_post_metric_response_month_end_date = pd.DataFrame(post_metric_response_end_date_month)
                    df_api_response_month_end_date = pd.DataFrame(df_post_metric_response_month_end_date.api_response.apply(pd.Series))
                    filtered_next_month_count = df_api_response_month_end_date.loc[df_api_response_month_end_date["created_time"] < str(next_date_month)].shape[0]

                    count_in_API_call = (total_val_month - filtered_prev_month_count) + filtered_next_month_count
                    source_data_month_wise_list.append(count_in_API_call)
                else:
                    return False
            data_count_month_wise = {"month": ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], "API_count": source_data_month_wise_list, "s3_count": s3_data_month_wise_count_list}

            df_data_count_month_wise = pd.DataFrame(data_count_month_wise)
            df_data_count_month_wise["Count_diff"] = abs(df_data_count_month_wise["API_count"] - df_data_count_month_wise["s3_count"])
            df_mismatched_month = df_data_count_month_wise.loc[df_data_count_month_wise["Count_diff"] != 0]
            df_mismatched_month.index += 1
            print(df_mismatched_month)
            mismatch_month = []
            for i in range(12):
                if source_data_month_wise_list[i] != s3_data_month_wise_count_list[i]:
                    mismatch_month.append(i + 1)
            s3_month_week_list = []
            for i in mismatch_month:
                s3_df_month = s3_df.loc[(s3_df.created_time.dt.month == i)]
                df_s3_month_day = s3_df_month.groupby(s3_df_month.created_time.dt.day)["post_id"].count()
                last_date_of_month = int(calendar.monthrange(start_date.year, i)[1])
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
                if len(s3_week_list) != 5:
                    s3_week_list.append(0)
                s3_month_week_list.append(s3_week_list)
            source_data_week_wise_count_list_nested = []
            for i in mismatch_month:
                date_format = "%Y-%m-%d"
                d1 = dt.strptime(str(start_date.year) + "-" + str(i) + "-01", date_format).date()
                last_date_of_month = calendar.monthrange(start_date.year, i)[1]
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
                    next_date_3 = end_date_week + timedelta(days=1)
                    post_metrics_response_start_date_week, post_metrics_response_end_date_week, total_val_week = get_api_response(user_id, password, user_choice, start_date_week, end_date_week, next_date_3, profile_ids, post_level_metrics, limit, post_level_metrics_sort)
                    if total_val_week != 0:
                        df_post_metric_response_week_start_date = pd.DataFrame(post_metrics_response_start_date_week)
                        df_api_response_week_start_date = pd.DataFrame(df_post_metric_response_week_start_date.api_response.apply(pd.Series))
                        filtered_prev_week_date_count = df_api_response_week_start_date.loc[df_api_response_week_start_date["created_time"] < str(start_date_week)].shape[0]

                        df_post_metric_response_week_end_date = pd.DataFrame(post_metrics_response_end_date_week)
                        df_api_response_week_end_date = pd.DataFrame(df_post_metric_response_week_end_date.api_response.apply(pd.Series))
                        filtered_next_week_date_count = df_api_response_week_end_date.loc[df_api_response_week_end_date["created_time"] < str(next_date_3)].shape[0]

                        count_in_API_call = (total_val_week - filtered_prev_week_date_count) + filtered_next_week_date_count
                        source_data_week_wise_list.append(count_in_API_call)

                        d += step
                    else:
                        return False
                if len(source_data_week_wise_list) != 5:
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
    else:
        return False


# ************************************************************************************************************************************************** #
#                                                               *** Main Function ***                                                                #
# ************************************************************************************************************************************************** #


def main():
    global param_choice
    param_choice = ["Facebook", "Instagram", "Youtube", "LinkedIn", "Twitter"]
    user_id, password, user_choice, start_date, end_date, limit, post_url, post_level_metrics_sort, profile_ids, post_level_metrics = read_gen_config()
    flag, s3_df = get_cloud_data(start_date.year, user_choice, post_url)
    if flag:
        flag = source_target_count_check(user_id, password, user_choice, start_date, end_date, limit, post_level_metrics_sort, profile_ids, post_level_metrics, s3_df)
        if flag:
            print("Data count matching for the year :", start_date.year)
    else:
        pass


if __name__ == "__main__":
    main()
