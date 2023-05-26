from datetime import date as d, datetime as dt, timedelta
import pandas as pd
import json
import requests as req
import awswrangler as wr
import time as t
import configparser as cp
import calendar
import boto3
s3 = boto3.client('s3')

def read_gen_config():
    config = cp.ConfigParser()
    config.read('config.ini')

    secret = boto3.client('ssm')
    user_id = secret.get_parameter(Name='srmg_socialbakers_secret_param_user_id', WithDecryption=True)['Parameter'][
        'Value']
    password = secret.get_parameter(Name='srmg_socialbakers_secret_param_password', WithDecryption=True)['Parameter'][
        'Value']

    global sync_type
    sync_type = int(config['Default']['sync_type'])
    user_choice = int(config['Default']['user_choice'])

    if sync_type == 1:
        start_date = [str(dt.strptime(config['Default']['start_date'], '%Y-%m-%d').date())]
        end_date = [str(dt.strptime(config['Default']['end_date'], '%Y-%m-%d').date())]
        print("Historical Count check from :", start_date[0], "to",end_date[0])
    else:
        print("Daily Sync-up")
        start_date = d.today() - timedelta(days=8)
        end_date = d.today() - timedelta(days=1)

    limit = config['Default']['post_level_limit']
    post_url = json.loads(config['Default']['post_url'])
    post_level_metrics_sort = json.loads(config['Default']['post_level_metrics_sort'])
    profile_ids = json.loads(config[param_choice[user_choice - 1]]['profile_ids'])
    post_level_metrics = json.loads(config[param_choice[user_choice - 1]]['post_level_metrics'])

    return user_id, password, user_choice, start_date, end_date, limit, post_url, post_level_metrics_sort, profile_ids, post_level_metrics


def get_cloud_data(date):
    df = wr.s3.read_parquet(
        path="s3://srmg-datalake/test_2/social_bakers/post-level-metrics/processed/facebook_post/" + str(
            date) + ".parquet")

    return df


def get_api_response(p_user_id, p_password, p_user_choice, p_start_date_arr, p_end_date_arr, p_profile_ids
                     , p_post_level_metrics, p_limit, p_post_level_metrics_sort, p_check):
    user_id = p_user_id
    password = p_password
    user_choice = p_user_choice - 1
    start_date_arr = p_start_date_arr
    end_date_arr = p_end_date_arr
    post_level_metrics = p_post_level_metrics
    profile_ids = p_profile_ids
    limit = p_limit
    post_level_metrics_sort = p_post_level_metrics_sort
    check = p_check

    post_metrics_response = []

    st = t.time()

    api_url_smp_part = {0: "facebook/page/posts"
        , 1: "instagram/profile/posts"
        , 2: "youtube/profile/videos"
        , 3: "linkedin/profile/posts"
        , 4: "twitter/profile/tweets"
                        }

    next_val = None
    k = 0
    post_metrics_response = []

    for i in range(len(start_date_arr)):
        start_date = dt.strptime(start_date_arr[i], "%Y-%m-%d")
        end_date = dt.strptime(end_date_arr[i], "%Y-%m-%d")
        post_metric_body_json = {
            "profiles": profile_ids,
            "date_start": str(start_date_arr[i]),
            "date_end": str(end_date_arr[i]),
            "fields": post_level_metrics,
            "limit": int(limit)
        }
        url = "https://api.socialbakers.com/3/" + api_url_smp_part[user_choice]
        response_API = req.post(url, auth=(user_id, password), json=post_metric_body_json)
        data = response_API.text
        parse_json = json.loads(data)
        #       print(parse_json)
        total_val = int(parse_json['data']['remaining']) + int(post_metric_body_json['limit'])
        if check == 0:
            post_metric_body_json = {
                "profiles": profile_ids,
                "date_start": str(start_date_arr[i]),
                "date_end": str(start_date_arr[i]),
                "fields": post_level_metrics,
                "limit": int(limit)
            }
        else:
            post_metric_body_json = {
                "profiles": profile_ids,
                "date_start": str(end_date + timedelta(days=1))[0:10],
                "date_end": str(end_date + timedelta(days=1))[0:10],
                "fields": post_level_metrics,
                "limit": int(limit)
            }

        if user_choice != 4:
            post_metric_body_json["sort"] = post_level_metrics_sort

        while True:
            if next_val:
                post_metric_body_json = {
                    "after": next_val
                }
            response = req.post("https://api.socialbakers.com/3/" + api_url_smp_part[user_choice],
                                auth=(user_id, password),
                                json=post_metric_body_json)
            temp = response.json()

            k += 1
            if temp['success'] == True:
                if 'next' in list(temp['data'].keys()):
                    next_val = json.dumps(temp['data']['next'])
                    remaining_val = int(temp['data']['remaining'])
                    for i in temp['data']['posts']:
                        post_metrics_response.append({'start_date': start_date
                                                         , 'end_date': end_date
                                                         , 'api_response': i
                                                         , 'iteration': int(k)
                                                         , 'next': next_val
                                                         , 'remaining': remaining_val
                                                      })
                    temp_df = pd.DataFrame(temp['data']['posts'])
                    temp_df_list = temp_df['created_time'].to_list()
                    temp_df_list = pd.to_datetime(temp_df_list, format="%Y-%m-%d")
#                    print(temp_df_list[0])
                    if str(start_date)[0:10] in temp_df_list:
#                        print("hi")
                        break
                    if str(end_date + timedelta(days=1))[0:10] in temp_df_list:
                        break

                else:
                    for i in temp['data']['posts']:
                        post_metrics_response.append({'start_date': start_date
                                                         , 'end_date': end_date
                                                         , 'api_response': i
                                                         , 'iteration': int(k)
                                                         , 'next': None
                                                         , 'remaining': None
                                                      })
                    #
                    next_val = None
                    break

            else:
                et = t.time()
                elapsed_time = (et - st)
                return post_metrics_response, total_val

    et = t.time()
    elapsed_time = (et - st)

    return post_metrics_response, total_val


def source_target_count_check():
    global param_choice
    param_choice = ["Facebook", "Instagram", "Youtube", "LinkedIn", "Twitter"]
    user_id, password, user_choice, start_date, end_date, limit, post_url, post_level_metrics_sort, profile_ids, post_level_metrics = read_gen_config()
    start_date_1 = dt.strptime(start_date[0], "%Y-%m-%d")
    end_date_1 = dt.strptime(end_date[0], "%Y-%m-%d")
    next_date = str(end_date_1 + timedelta(days=1))[0:10]

    post_metrics_response, total_val = get_api_response(user_id, password, user_choice, start_date, end_date,
                                                        profile_ids, post_level_metrics, limit, post_level_metrics_sort,
                                                        0)

    df_post_metric_response_1 = pd.DataFrame(post_metrics_response)
    df_api_response_1 = pd.DataFrame(df_post_metric_response_1.api_response.apply(pd.Series))
    df_api_response_1['created_time'] = pd.to_datetime(df_api_response_1['created_time'], format='%Y-%m-%d')
    filtered_prev_year_df = df_api_response_1.loc[(df_api_response_1['created_time'] < start_date[0])]

    post_metrics_response, total_val = get_api_response(user_id, password, user_choice, start_date, end_date,
                                                        profile_ids, post_level_metrics, limit, post_level_metrics_sort,
                                                        1)
    df_post_metric_response_2 = pd.DataFrame(post_metrics_response)
    df_api_response_2 = pd.DataFrame(df_post_metric_response_2.api_response.apply(pd.Series))
    df_api_response_2['created_time'] = pd.to_datetime(df_api_response_2['created_time'], format='%Y-%m-%d')
    filtered_next_year_df = df_api_response_2.loc[(df_api_response_2['created_time'] < next_date)]

    s3_df = get_cloud_data(start_date_1.year)
    total_s3_count = len(s3_df)
    count_in_API_call = (total_val - len(filtered_prev_year_df)) + len(filtered_next_year_df)
    print("Count in API Call        :", count_in_API_call)
    print("Total count in S3 Bucket :", total_s3_count)
    print("Count Difference         :", abs(count_in_API_call - total_s3_count))

    if (count_in_API_call == total_s3_count):
        print("Data count matching")
    else:
        print("Data count not matching")
        print("Started checking month wise...")
        #       print("count in s3 bucket month wise for the year",start_date_1.year)
        s3_data_month_wise_count = s3_df.groupby(s3_df.created_time.dt.month)['post_id'].count()
        s3_data_month_wise_count_list = s3_data_month_wise_count.to_list()
        #       print(s3_data_month_wise_count)
        source_data_month_wise_list = []
        print("Count for s3 and API month wise for the year:", start_date_1.year)
        for i in range(1, 13):
            start_date_2 = dt.strptime(start_date[0], "%Y-%m-%d")
            if (i > 9):
                start_date_3 = str(start_date_2.year) + "-" + str(i) + "-" + "01"
                start_arr = [start_date_3]
            else:
                start_date_3 = str(start_date_2.year) + "-0" + str(i) + "-" + "01"
                start_arr = [start_date_3]
            last_date_of_month = calendar.monthrange(start_date_2.year, i)[1]
            if i > 9:
                end_date_2 = str(start_date_2.year) + "-" + str(i) + "-" + str(last_date_of_month)
                end_arr = [end_date_2]
            else:
                end_date_2 = str(start_date_2.year) + "-0" + str(i) + "-" + str(last_date_of_month)
                end_arr = [end_date_2]
            post_metrics_response1, total_val1 = get_api_response(user_id, password, user_choice, start_arr, end_arr,
                                                                profile_ids, post_level_metrics, limit,
                                                                post_level_metrics_sort, 0)

            df_post_metric_response_3 = pd.DataFrame(post_metrics_response1)
            df_api_response_3 = pd.DataFrame(df_post_metric_response_3.api_response.apply(pd.Series))
            df_api_response_3['created_time'] = pd.to_datetime(df_api_response_3['created_time'], format='%Y-%m-%d')
            filtered_prev_month_df = df_api_response_3.loc[(df_api_response_3['created_time'] < start_arr[0])]
            post_metrics_response, total_val = get_api_response(user_id, password, user_choice, start_arr, end_arr,
                                                                profile_ids, post_level_metrics, limit,
                                                                post_level_metrics_sort, 1)
            end_date_1 = dt.strptime(end_arr[0], "%Y-%m-%d")
            next_date = str(end_date_1 + timedelta(days=1))[0:10]
            df_post_metric_response_4 = pd.DataFrame(post_metrics_response)
            df_api_response_4 = pd.DataFrame(df_post_metric_response_4.api_response.apply(pd.Series))
            df_api_response_4['created_time'] = pd.to_datetime(df_api_response_4['created_time'], format='%Y-%m-%d')
            filtered_next_month_df = df_api_response_4.loc[(df_api_response_4['created_time'] < next_date)]
            count_in_API_call = (total_val - len(filtered_prev_month_df)) + len(filtered_next_month_df)
            source_data_month_wise_list.append(count_in_API_call)
        #            print(i,"  ",count_in_API_call)
        if len(s3_data_month_wise_count_list) != 12:
            for i in range(len(s3_data_month_wise_count_list), 12):
                s3_data_month_wise_count_list.append(0)

        data_count_month_wise = {
            "month": ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
            "API_count": source_data_month_wise_list,
            "s3_count": s3_data_month_wise_count_list
        }

        data_count_month_wise_df = pd.DataFrame(data_count_month_wise)
        data_count_month_wise_df.index = data_count_month_wise_df.index + 1
        data_count_month_wise_df['Count_diff'] = abs(
            data_count_month_wise_df['API_count'] - data_count_month_wise_df['s3_count'])
        print(data_count_month_wise_df)
        mismatch_month =[]
        for i in range(12):
            if (source_data_month_wise_list[i]!=s3_data_month_wise_count_list[i]):
                mismatch_month.append(i+1)
        print("fetching s3 and API count week wise for the mismatched month...")
        for i in mismatch_month:
            s3_df_month = s3_df.loc[(s3_df.created_time.dt.month == i)]
            df_s3_month_day = s3_df_month.groupby(s3_df_month.created_time.dt.day)['post_id'].count()
#            print(df_s3_month_day)
            s3_df_day_list = df_s3_month_day.to_list()
            last_date_of_month = int(calendar.monthrange(start_date_2.year, i)[1])
            s3_week_list = []
            s3_month_week_list =[]

            for i in range(0, last_date_of_month, 7):
                s3_week_list.append(sum(s3_df_day_list[i:i + 7]))
            if len(s3_week_list) != 5:
                s3_week_list.append(0)
            s3_month_week_list.append(s3_week_list)
        for i in mismatch_month:
            date_format = "%Y-%m-%d"
            d1 = dt.strptime(str(start_date_2.year)+"-"+str(i)+"-01", date_format).date()
            last_date_of_month = calendar.monthrange(start_date_2.year, i)[1]
            d2 = dt.strptime(str(start_date_2.year)+"-"+str(i)+"-"+str(last_date_of_month),date_format).date()
            d = d1
            step = timedelta(days=7)
            week_arr=[]
            week_arr_nested = []
            source_data_week_wise_list = []
            source_data_week_wise_list_nested = []
            while d < d2:
                week_arr.append(d.strftime(date_format))
                start_date_week =[str(d)]
                end_date_week = [str(d+timedelta(days=6))]
                post_metrics_response1, total_val1 = get_api_response(user_id, password, user_choice, start_date_week,
                                                                      end_date_week,
                                                                      profile_ids, post_level_metrics, limit,
                                                                      post_level_metrics_sort, 0)

                df_post_metric_response_3 = pd.DataFrame(post_metrics_response1)
                df_api_response_3 = pd.DataFrame(df_post_metric_response_3.api_response.apply(pd.Series))
                df_api_response_3['created_time'] = pd.to_datetime(df_api_response_3['created_time'], format='%Y-%m-%d')
                filtered_prev_month_df = df_api_response_3.loc[(df_api_response_3['created_time'] < start_date_week[0])]
                post_metrics_response, total_val = get_api_response(user_id, password, user_choice, start_date_week, end_date_week,
                                                                    profile_ids, post_level_metrics, limit,
                                                                    post_level_metrics_sort, 1)
                end_date_1 = dt.strptime(end_date_week[0], "%Y-%m-%d")
                next_date = str(end_date_1 + timedelta(days=1))[0:10]
                df_post_metric_response_4 = pd.DataFrame(post_metrics_response)
                df_api_response_4 = pd.DataFrame(df_post_metric_response_4.api_response.apply(pd.Series))
                df_api_response_4['created_time'] = pd.to_datetime(df_api_response_4['created_time'], format='%Y-%m-%d')
                filtered_next_month_df = df_api_response_4.loc[(df_api_response_4['created_time'] < next_date)]
                count_in_API_call = (total_val - len(filtered_prev_month_df)) + len(filtered_next_month_df)
                source_data_week_wise_list.append(count_in_API_call)

                d += step
        week_arr_nested.append(week_arr)
        if len(source_data_week_wise_list) != 5:
            source_data_week_wise_list.append(0)
        source_data_week_wise_list_nested.append(source_data_week_wise_list)
        j=0
        df_week_wise_count = pd.DataFrame()

        for i in mismatch_month:
            datetime_object = dt.strptime(str(i), "%m")
            month_name = datetime_object.strftime("%b")

            data = {
                "month":[month_name,month_name,month_name,month_name,month_name],
                "week":["week 1","week 2","week 3","week 4","week 5"],
                "API_count":source_data_week_wise_list_nested[j],
                "s3_count": s3_month_week_list[j]
            }
            df_temp = pd.DataFrame(data)
            j+=1
            df_week_wise_count = pd.concat([df_week_wise_count,df_temp])
        df_week_wise_count['Count_diff'] = abs(df_week_wise_count['API_count'] - df_week_wise_count['s3_count'])
        print(df_week_wise_count)


def main():
    source_target_count_check()
    return 1


if __name__ == "__main__":
    main()



