"""
This document contains the python ETL pipeline code that will collect the IO_Technologies metrics for SRMG
from IO_Technologies dashboard using an API and loads the obtained data into an AWS S3 bucket.
The API configuration and the AWS S3 configuration are available in the config.ini file along with all
other supporting parameters.
Access Secrets are read from AWS SSM Parameter Store.
"""
import json
import boto3
import calendar
import warnings
import time as t
import collections
import pandas as pd
from pyparsing import null_debug_action
import requests as req
import awswrangler as wr
import configparser as cp
from datetime import date as d, datetime as dt, timedelta


warnings.filterwarnings("ignore")
s3 = boto3.client("s3")


try:
    from urllib.parse import urlencode as urllib_urlencode
except ImportError:
    from urllib import urlencode as urllib_urlencode
from collections import OrderedDict


def flatten(d):

    """Return a dict as a list of lists.
    >>> flatten({"a": "b"})
    [['a', 'b']]
    >>> flatten({"a": [1, 2, 3]})
    [['a', [1, 2, 3]]]
    >>> flatten({"a": {"b": "c"}})
    [['a', 'b', 'c']]
    >>> flatten({"a": {"b": {"c": "e"}}})
    [['a', 'b', 'c', 'e']]
    >>> flatten({"a": {"b": "c", "d": "e"}})
    [['a', 'b', 'c'], ['a', 'd', 'e']]
    >>> flatten({"a": {"b": "c", "d": "e"}, "b": {"c": "d"}})
    [['a', 'b', 'c'], ['a', 'd', 'e'], ['b', 'c', 'd']]
    """
    if not isinstance(d, dict):
        return [[d]]
    returned = []
    for key, value in sorted(d.items()):
        # Each key, value is treated as a row.
        nested = flatten(value)
        for nest in nested:
            current_row = [key]
            current_row.extend(nest)
            returned.append(current_row)
    return returned


def parametrize(params):

    """Return list of params as params.
    >>> parametrize(['a'])
    'a'
    >>> parametrize(['a', 'b'])
    'a[b]'
    >>> parametrize(['a', 'b', 'c'])
    'a[b][c]'
    """
    returned = str(params[0])
    returned += "".join("[" + str(p) + "]" for p in params[1:])
    return returned


def urlencode(params):

    """Urlencode a multidimensional dict."""
    # Not doing duck typing here. Will make debugging easier.

    if not isinstance(params, dict):
        raise TypeError("Only dicts are supported.")
    params = flatten(params)
    url_params = OrderedDict()
    for param in params:
        value = param.pop()
        name = parametrize(param)
        if isinstance(value, (list, tuple)):
            name += "[]"
        url_params[name] = value
    return urllib_urlencode(url_params, doseq=True)


def read_config():

    """Reading the generic configuration parameters."""

    config = cp.ConfigParser()
    config.read("config.ini")
    secret_keys = eval(config["Default"]["secretkey"])
    end_point = eval(config["Default"]["endpoint"])
    io_tech_s3_url = json.loads(config["Default"]["io_tech_s3_url"])
    global SYNC_TYPE

    SYNC_TYPE = int(config["Default"]["sync_type"])

    if SYNC_TYPE == 1:
        print("Historical Run")
        start_date = dt.strptime(config["Default"]["start_date"], "%Y-%m-%d").date()
        end_date = dt.strptime(config["Default"]["end_date"], "%Y-%m-%d").date()

    else:
        print("Daily Sync-up")
        start_date = d.today() - timedelta(days=8)
        end_date = d.today() - timedelta(days=1)

    global years_in_req_period

    years_in_req_period = list(range(start_date.year, end_date.year + 1))

    global ARTICLE_METRICS, ARTICLE_METRICS_DATATYPE, ARTICLE_DTYPE_DICT

    ARTICLE_METRICS = json.loads(config["Default"]["article_metrics"])
    ARTICLE_METRICS_DATATYPE = json.loads(config["Default"]["article_metrics_datatype"])
    ARTICLE_DTYPE_DICT = {}

    for i in enumerate(ARTICLE_METRICS):
        ARTICLE_DTYPE_DICT[ARTICLE_METRICS[i[0] - 1]] = ARTICLE_METRICS_DATATYPE[i[0] - 1]

    global AUTHOR_METRICS, AUTHOR_METRICS_DATATYPE, AUTHOR_DTYPE_DICT

    AUTHOR_METRICS = json.loads(config["Default"]["author_metrics"])
    AUTHOR_METRICS_DATATYPE = json.loads(config["Default"]["author_metrics_datatype"])
    AUTHOR_DTYPE_DICT = {}

    for i in enumerate(AUTHOR_METRICS):
        AUTHOR_DTYPE_DICT[AUTHOR_METRICS[i[0] - 1]] = AUTHOR_METRICS_DATATYPE[i[0] - 1]

    global CATEGORY_METRICS, CATEGORY_METRICS_DATATYPE, CATEGORY_DTYPE_DICT

    CATEGORY_METRICS = json.loads(config["Default"]["category_metrics"])
    CATEGORY_METRICS_DATATYPE = json.loads(config["Default"]["category_metrics_datatype"])
    CATEGORY_DTYPE_DICT = {}

    for i in enumerate(CATEGORY_METRICS):
        CATEGORY_DTYPE_DICT[CATEGORY_METRICS[i[0] - 1]] = CATEGORY_METRICS_DATATYPE[i[0] - 1]

    global SOURCE_METRICS, SOURCE_METRICS_DATATYPE, SOURCE_DTYPE_DICT

    SOURCE_METRICS = json.loads(config["Default"]["source_metrics"])
    SOURCE_METRICS_DATATYPE = json.loads(config["Default"]["source_metrics_datatype"])
    SOURCE_DTYPE_DICT = {}

    for i in enumerate(SOURCE_METRICS):
        SOURCE_DTYPE_DICT[SOURCE_METRICS[i[0] - 1]] = SOURCE_METRICS_DATATYPE[i[0] - 1]

    brand_vertical_mapping_url = json.loads(config["Default"]["brand_vertical_mapping_url"])

    return secret_keys, end_point, start_date, end_date, io_tech_s3_url, brand_vertical_mapping_url


def get_brand_vertical_mapping(p_brand_vertical_mapping_url):

    """Fetching the brand and vertical information"""

    brand_vertical_mapping_url = p_brand_vertical_mapping_url
    brand_vertical_df = pd.DataFrame()

    try:
        brand_vertical_df = wr.s3.read_json(path=[brand_vertical_mapping_url])

    except Exception as gen_exception:
        print("Brand Vertical Mapping Read Error: ", gen_exception)
        return None

    brand_vertical_df["brand"] = brand_vertical_df["new_brand_name"].astype("str")
    brand_vertical_df["io_tech_endpoint"] = brand_vertical_df["io_tech_endpoint"].astype("str")
    brand_vertical_df = brand_vertical_df.astype("string")

    return brand_vertical_df


def date_limit_handler(p_start_date, p_end_date):

    """Splitting a given date ranges into a single day"""

    start_date = p_start_date
    end_date = p_end_date
    date_range_arr = pd.date_range(start_date, end_date, freq="D")
    print(date_range_arr)
    return date_range_arr


def get_api_response(p_date_range_arr, p_secret_keys, p_end_point):

    """Making the API Calls"""

    date_range_arr = p_date_range_arr
    secret_keys = p_secret_keys
    end_point = p_end_point
    index = 0
    headers = {}

    data_list_nested = []
    headers["Content-Type"] = "application/x-www-form-urlencoded"

    for secret_key in secret_keys:
        data_list = []
        for date in date_range_arr:
            try:
                params = {
                    "key": secret_key,
                    "entities": {
                        "categories": {"entity": "categories", "details": ["author", "facebook", "page", "pageviews", "reference_time", "timeread_total", "url", "recirculation", "readability", "timeread", "type_article", "date_pub", "category", "sources", "social", "domain", "trend", "uniques"]},
                        "articles": {"entity": "articles", "details": ["author", "facebook", "page", "pageviews", "reference_time", "timeread_total", "url", "recirculation", "readability", "timeread", "type_article", "date_pub", "category", "sources", "social", "domain", "trend"]},
                        "authors": {"entity": "authors", "details": ["author", "facebook", "page", "pageviews", "reference_time", "timeread_total", "url", "recirculation", "readability", "timeread", "type_article", "date_pub", "category", "sources", "social", "domain", "trend", "count_pub"]},
                        "sources": {"entity": "sources", "details": ["author", "facebook", "page", "pageviews", "reference_time", "timeread_total", "url", "recirculation", "readability", "timeread", "type_article", "date_pub", "category", "sources", "social", "domain", "trend"]},
                        "summary": {"entity": "summary"},
                    },
                    "options": {"period": {"name": "range", "at_from": date._date_repr, "at_to": date._date_repr}, "per_page": 2500},
                }
                query = urlencode(params)
                end_points = "https://api.onthe.io/" + end_point[index]
                response = req.post(end_points, data=query, headers=headers)
                data = response.json()

                if data is not None:
                    data["date"] = {}
                    data["date"] = str(date)[0:10]
                    data["year"] = date.year
                    data["io_tech_endpoint"] = end_point[index]
                    data_list.append(data)
                    print(" data returned for the date:", str(date)[0:10], " for the endpoint: ", end_point[index])
                else:
                    print("No data returned for the date:", str(date)[0:10], " for the endpoint: ", end_point[index])
            except Exception as gen_exception:
                print("Exception1 ", gen_exception, "  occured for processing the date: ", str(date)[0:10], " for the endpoint: ", end_point[index])

        data_list_nested.append(data_list)
        index += 1

    return data_list_nested, len(date_range_arr), len(secret_keys)


def add_endpoint_date_year(df, p_date, p_year, p_io_tech_endpoint):

    """Adding Arabic Date, Date, year & endpoints columns into the dataframe"""

    df.insert(0, "Arabic_Date", pd.Series())
    df["Arabic_Date"] = pd.to_datetime(p_date).date()
    df.insert(1, "Date", pd.Series())
    df["Date"] = pd.to_datetime(p_date).date()
    df.insert(2, "year", pd.Series(dtype=pd.StringDtype()))
    df["year"] = p_year
    df.insert(3, "io_tech_endpoint", pd.Series(dtype=pd.StringDtype()))
    df["io_tech_endpoint"] = p_io_tech_endpoint

    return df


def save_data_frame_to_s3(p_data_list, p_date_length, p_secret_keys_length, io_tech_s3_url, brand_vertical_df):

    """Saving DataFrame into AWS S3 bucket"""

    save_dataframe_from_json_without_normalize(p_secret_keys_length, p_date_length, "articles", p_data_list, ARTICLE_METRICS, ARTICLE_DTYPE_DICT, io_tech_s3_url, "url", brand_vertical_df)

    save_dataframe_from_json_without_normalize(p_secret_keys_length, p_date_length, "authors", p_data_list, AUTHOR_METRICS, AUTHOR_DTYPE_DICT, io_tech_s3_url, "author", brand_vertical_df)

    save_dataframe_from_json_without_normalize(p_secret_keys_length, p_date_length, "categories", p_data_list, CATEGORY_METRICS, CATEGORY_DTYPE_DICT, io_tech_s3_url, "category", brand_vertical_df)

    save_dataframe_from_json_without_normalize(p_secret_keys_length, p_date_length, "sources", p_data_list, SOURCE_METRICS, SOURCE_DTYPE_DICT, io_tech_s3_url, "source", brand_vertical_df)

    save_dataframe_from_json_withjson_normalize(p_secret_keys_length, p_date_length, "summary", p_data_list, io_tech_s3_url, "brand", brand_vertical_df)


def save_dataframe_from_json_without_normalize(secret_keys_length, date_length, p_json_key, data, COLUMNS, DATA_TYPES, io_tech_s3_url, p_unique_field, brand_vertical_df):

    """Saving DataFrame without normalizing the json"""

    for key_index in range(secret_keys_length):
        result_df = pd.DataFrame()
        for date_index in range(date_length):
            try:
                if p_json_key == "sources":
                    values = data[key_index][date_index][p_json_key]
                else:
                    values = data[key_index][date_index][p_json_key]["list"]

                df_result = pd.DataFrame(values, columns=COLUMNS)
                df_result = df_result.astype(DATA_TYPES)

                if p_json_key == "articles":
                    df_result = populate_top_content(df_result)

                    if "sources" in df_result.columns:
                        df_result = df_result.drop("sources", axis=1)

                df_result = add_endpoint_date_year(df_result, data[key_index][date_index]["date"], data[key_index][date_index]["year"], data[key_index][date_index]["io_tech_endpoint"])
                result_df = pd.concat([result_df, df_result]).reset_index(drop=True)
            except Exception as gen_exception:
                try:
                    print("Exception2 ", gen_exception, "  occurred for processing the date ", data[key_index][date_index]["date"], " key:", data[key_index][date_index]["io_tech_endpoint"])
                except Exception as gen_exception:
                    print("Exception4 ", gen_exception, "  occurred for processing the data ")
        if not result_df.empty:
            file_sync(result_df, p_json_key, io_tech_s3_url, p_unique_field, brand_vertical_df)


def populate_top_content(df_result):

    """Populating top contents"""

    sources_top_search_type = []
    sources_top_search_count = []
    sources_top_referral_type = []
    sources_top_referral_count = []
    sources_top_mainpage_type = []
    sources_top_mainpage_count = []
    sources_top_otherpage_type = []
    sources_top_otherpage_count = []
    sources_top_direct_type = []
    sources_top_direct_count = []
    sources_top_news_type = []
    sources_top_news_count = []
    sources_top_social_type = []
    sources_top_social_count = []
    source_article_social_count = []
    source_article_search_count = []
    source_article_direct_count = []
    source_article_referral_count = []
    source_article_other_pages_count = []
    source_article_main_page_count = []
    source_article_news_count = []

    for index, row in df_result.iterrows():
        try:
            sources_top_search_type.append(row["sources"]["top"]["search"][0])
            sources_top_search_count.append(row["sources"]["top"]["search"][1])
        except Exception as gen_exception:
            sources_top_search_type.append("NULL")
            sources_top_search_count.append("0")
            # print("Exception ", gen_exception, "  occurred for processing the data")
        try:
            sources_top_referral_type.append(row["sources"]["top"]["referral"][0])
            sources_top_referral_count.append(row["sources"]["top"]["referral"][1])
        except Exception as gen_exception:
            sources_top_referral_type.append("NULL")
            sources_top_referral_count.append("0")
            # print("Exception ", gen_exception, "  occurred for processing the data")
        try:
            sources_top_mainpage_type.append(row["sources"]["top"]["Main Page"][0])
            sources_top_mainpage_count.append(row["sources"]["top"]["Main Page"][1])
        except Exception as gen_exception:
            sources_top_mainpage_type.append("NULL")
            sources_top_mainpage_count.append("0")
            # print("Exception ", gen_exception, "  occurred for processing the data")
        try:
            sources_top_direct_type.append(row["sources"]["top"]["direct"][0])
            sources_top_direct_count.append(row["sources"]["top"]["direct"][1])
        except Exception as gen_exception:
            sources_top_direct_type.append("NULL")
            sources_top_direct_count.append("0")
            # print("Exception ", gen_exception, "  occurred for processing the data")

        try:
            sources_top_otherpage_type.append(row["sources"]["top"]["Other Pages"][0])
            sources_top_otherpage_count.append(row["sources"]["top"]["Other Pages"][1])
        except Exception as gen_exception:
            sources_top_otherpage_type.append("NULL")
            sources_top_otherpage_count.append("0")
            # print("Exception ", gen_exception, "  occurred for processing the data")
        try:
            sources_top_news_type.append(row["sources"]["top"]["news"][0])
            sources_top_news_count.append(row["sources"]["top"]["news"][1])
        except Exception as gen_exception:
            sources_top_news_type.append("NULL")
            sources_top_news_count.append("0")
            # print("Exception ", gen_exception, "  occurred for processing the data")
        try:
            sources_top_social_type.append(row["sources"]["top"]["social"][0])
            sources_top_social_count.append(row["sources"]["top"]["social"][1])
        except Exception as gen_exception:
            sources_top_social_type.append("NULL")
            sources_top_social_count.append("0")
            # print("Exception ", gen_exception, "  occurred for processing the data")
        try:
            source_article_social_count.append(row["sources"]["article"]["social"])
        except Exception as gen_exception:
            source_article_social_count.append("0")
        try:
            source_article_search_count.append(row["sources"]["article"]["search"])
        except Exception as gen_exception:
            source_article_search_count.append("0")
        try:
            source_article_direct_count.append(row["sources"]["article"]["direct"])
        except Exception as gen_exception:
            source_article_direct_count.append("0")
        try:
            source_article_referral_count.append(row["sources"]["article"]["referral"])
        except Exception as gen_exception:
            source_article_referral_count.append("0")
        try:
            source_article_other_pages_count.append(row["sources"]["article"]["Other pages"])
        except Exception as gen_exception:
            source_article_other_pages_count.append("0")
        try:
            source_article_main_page_count.append(row["sources"]["article"]["Main page"])
        except Exception as gen_exception:
            source_article_main_page_count.append("0")
        try:
            source_article_news_count.append(row["sources"]["article"]["news"])
        except Exception as gen_exception:
            source_article_news_count.append("0")

    df_result["sources_top_search_type"] = pd.Series(sources_top_search_type)
    df_result["sources_top_search_count"] = pd.Series(sources_top_search_count).astype(int)

    df_result["sources_top_referral_type"] = pd.Series(sources_top_referral_type)
    df_result["sources_top_referral_count"] = pd.Series(sources_top_referral_count).astype(int)

    df_result["sources_top_mainpage_type"] = pd.Series(sources_top_mainpage_type)
    df_result["sources_top_mainpage_count"] = pd.Series(sources_top_mainpage_count).astype(int)

    df_result["sources_top_otherpage_type"] = pd.Series(sources_top_otherpage_type)
    df_result["sources_top_otherpage_count"] = pd.Series(sources_top_otherpage_count).astype(int)

    df_result["sources_top_direct_type"] = pd.Series(sources_top_direct_type)
    df_result["sources_top_direct_count"] = pd.Series(sources_top_direct_count).astype(int)

    df_result["sources_top_news_type"] = pd.Series(sources_top_news_type)
    df_result["sources_top_news_count"] = pd.Series(sources_top_news_count).astype(int)

    df_result["sources_top_social_type"] = pd.Series(sources_top_social_type)
    df_result["sources_top_social_count"] = pd.Series(sources_top_social_count).astype(int)

    df_result["sources_article_social_count"] = pd.Series(source_article_social_count).astype(int)

    df_result["sources_article_search_count"] = pd.Series(source_article_search_count).astype(int)

    df_result["sources_article_direct_count"] = pd.Series(source_article_direct_count).astype(int)

    df_result["sources_article_referral_count"] = pd.Series(source_article_referral_count).astype(int)

    df_result["sources_article_other_pages_count"] = pd.Series(source_article_other_pages_count).astype(int)

    df_result["sources_article_main_page_count"] = pd.Series(source_article_main_page_count).astype(int)

    df_result["sources_article_news_count"] = pd.Series(source_article_news_count).astype(int)

    return df_result


def save_dataframe_from_json_withjson_normalize(secret_keys_length, date_length, p_json_key, data, io_tech_s3_url, p_unique_field, brand_vertical_df):

    """Saving DataFrame by normalizing the JSON"""

    for key_index in range(secret_keys_length):
        result_df = pd.DataFrame()
        for date_index in range(date_length):
            try:
                result = data[key_index][date_index][p_json_key]
                df_result = pd.json_normalize(result, sep="_")
                df_result = add_endpoint_date_year(df_result, data[key_index][date_index]["date"], data[key_index][date_index]["year"], data[key_index][date_index]["io_tech_endpoint"])
                result_df = pd.concat([result_df, df_result]).reset_index(drop=True)
            except Exception as gen_exception:
                print("Exception3 ", gen_exception, "  occurred for processing the data")
        if not result_df.empty:
            file_sync(result_df, p_json_key, io_tech_s3_url, p_unique_field, brand_vertical_df)


def file_sync(p_io_tech_data_df, p_name, p_io_tech_s3_url, p_unique_field, brand_vertical_df):

    """Exporting data to Parquet files year-wise and handling daily sync updates"""

    brand_vertical_df_row = brand_vertical_df.loc[brand_vertical_df["io_tech_endpoint"] == p_io_tech_data_df["io_tech_endpoint"].values[0]]

    table_name = p_name
    try:
        vertical = brand_vertical_df_row["vertical"].values[0]
        brand = brand_vertical_df_row["brand"].values[0]

        file_name = table_name + "/" + "Vertical=" + vertical + "/" + "Brand=" + brand + "/" + dt.strftime(dt.today(), "%Y_%m_%d_%I_%M_%S") + ".txt"
        s3_url = p_io_tech_s3_url + "raw/" + file_name

        wr.s3.to_csv(p_io_tech_data_df, s3_url)
        print(years_in_req_period)

        for i in years_in_req_period:
            current_df = p_io_tech_data_df[p_io_tech_data_df["year"] == str(i)]
            parquet_file_name = table_name + "/" + "Vertical=" + vertical + "/" + "Brand=" + brand + "/" + str(i) + ".parquet"
            s3_url = p_io_tech_s3_url + "processed/" + parquet_file_name
            print("Target s3 location:", s3_url)

            try:
                df = wr.s3.read_parquet(s3_url)
            except:
                df = pd.DataFrame()

            print("Size of current response", p_io_tech_data_df.shape)
            print("Size of existing file", df.shape)
            p_io_tech_data_df = pd.concat([p_io_tech_data_df, df], axis=0).reset_index(drop=True)
            print("Size of merged file", p_io_tech_data_df.shape)
            # p_io_tech_data_df = pd.merge(how="right", right=p_io_tech_data_df, left=brand_vertical_df.loc[:, ["brand", "vertical", "io_tech_endpoint"]].drop_duplicates().reset_index(drop=True), right_on=["io_tech_endpoint"], left_on=["io_tech_endpoint"])
            p_io_tech_data_df = p_io_tech_data_df.sort_values(by=["Date"]).drop_duplicates(keep="last").reset_index(drop=True)
            print("Size of merged file without duplicates", p_io_tech_data_df.shape)

            try:
                wr.s3.to_parquet(p_io_tech_data_df, s3_url)

            except Exception as gen_exception:
                print("S3 bucket File Write Exception ", gen_exception, " occured while writing to parquet file in ", s3_url)
    except Exception as gen_exception:
        print("Error while syncing file to S3 ", gen_exception, " for ", p_io_tech_data_df["io_tech_endpoint"].values[0])

# def main(event, lambda_context):
def main():

    """Main Function"""

    secret_keys, end_point, start_date, end_date, io_tech_s3_url, brand_vertical_mapping_url = read_config()
    date_range_arr = date_limit_handler(start_date, end_date)
    brand_vertical_df = get_brand_vertical_mapping(brand_vertical_mapping_url)
    data_list, date_length, secret_keys_length = get_api_response(date_range_arr, secret_keys, end_point)
    save_data_frame_to_s3(data_list, date_length, secret_keys_length, io_tech_s3_url, brand_vertical_df)


if __name__ == "__main__":
    main()

# if __name__=="__main__":
#     main(event, lambda_context)
