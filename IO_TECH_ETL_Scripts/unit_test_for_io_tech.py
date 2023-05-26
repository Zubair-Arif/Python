import unittest as test
import io_tech_etl_script
from datetime import datetime as dt
import boto3
import pandas as pd
s3 = boto3.client("s3")

try:
    from urllib.parse import urlencode as urllib_urlencode
except ImportError:
    from urllib import urlencode as urllib_urlencode
from collections import OrderedDict


class test_io_tech_etl(test.TestCase):
    def test_read_config(self):
        original_read_config_output = io_tech_etl_script.read_config()
        expected_read_config_output = (
            [
                "7f530662dee6f132d96a6f3950defc9d",
                "2c4f6f2d071e5a9ada9991e454068b31",
                "7e59c1c4979fb966599248240fc83851",
                "f27ebc2078d52ffe5b7192b907d303d5",
                "677dafeddbc4fe8e1007461bd98f4403",
                "109a04d7b42cb8694ea8e35e081c1a44",
                "66133575581ae09d07ccb313b20d50dc",
                "1f5769be7af0183086a4e4ecf9a9b6fa",
                "f71f45dec84b36a2c79b0ca879a7cdeb",
                "f1b65d74197389632f9a1807d47f7875",
                "7ae0e679bbea8ee23ba2b25aadb70dc8",
                "f351fcac4f0f84431fcf220077b935f3",
                "0c62f6ea42512eb07ea698953aa91a26",
                "cbfb98e5b99e43d8bb3771c12fa204b8",
                "ce77a56f3eab62ab1cc208f9ffdc6f28",
                "7915c6e4786a517f9afd9121f8ddaedd",
                "994ac5770b531e73ed028c3621d49a51",
                "e5879264204078c300f010e27e2fdb8f",
                "dacd0c41d417ecb5d471682330c6ecf4",
                "6e1d596df33812569e07e964014ed1a4",
                "9c15ca7b17d63cfda07fb352da01e88a",
                "8980bf3651a4738cc80c83e0fe7b746c",
            ],
            [
                "6A6q0G6fFuuNzvHu46pbm0Iq8buSpaUw",
                "2hEeiYt1xLR5IJ3g8wHqqIeiIeSWtDxv",
                "73qs3NhTerlWozwpy4o9948jdtQ2cGDO",
                "7u3ZcGmFL4aMlWs8smahjtgkIrJtYbg5",
                "BFok9cvrhCmZtilK2jV8CYLB4WX9AXXA",
                "BdpMsVUDhUZhwXsKQnEaSh77jb2Fb_iN",
                "BlyC7do20doUe9cclcZSjEPpGXObw7d7",
                "FvsFwM7bwYj1GiusZbTZF7gTJ9X0wIfa",
                "HKVTJYLu7THaK4ez4bnFzFUGmEh1ppD6",
                "LDAFb11T1eRaxw6aQwek1dRpejLwnkf7",
                "OEVtymQaduxKg5nFgxoM9CSfC_kqsOvf",
                "Rnv5_LwVknq6ZJJw3sBK6m3_vHLTNwaE",
                "geY2ZqBEAIqzxjF9TQykAfuRWZW6uJJj",
                "mosAolWBc9R_lAtQ28japl8kIc2yXfzi",
                "nMK3Dr8gHRJa8y9ovWY28_BAB0iIkLQH",
                "tAge0d5xw7qd4S2t6LYti0zNfc51W6tv",
                "wSpqaE7zHm4E61w5uu_Vog76qo3DCDK7",
                "xOQOYUFwp78JvfqmLkHrYi9ekfDrcmcY",
                "D2qxGdpwU_Q3sIH6VTx7jC4iVfgSrWef",
                "3UocHAx0pg74X_9OjjpZnNvHad6fZXY1",
                "DVgbj6yeqPZy6MFQXhEPKwJanVP024pG",
                "HC7Le_124LvZt8vj374hhOH3jO8q4b9L",
            ],
            dt.strptime("2018-01-01", "%Y-%m-%d").date(),
            dt.strptime("2018-01-01", "%Y-%m-%d").date(),
            "s3://srmg-datalake/io_technologies/",
            "s3://srmg-datalake/brand_vertical_mapping/brand_vertical_mapping_v2.json",
        )
        self.assertEqual(original_read_config_output, expected_read_config_output)

    def test_brand_vertical_mapping(self):
        original_brand_vertical_mapping_output = io_tech_etl_script.get_brand_vertical_mapping("s3://srmg-datalake/brand_vertical_mapping/brand_vertical_mapping_v2.json")
        expected_brand_vertical_output_0 = 62
        expected_brand_vertical_output_1 = 9
        self.assertEqual(original_brand_vertical_mapping_output.shape[0], expected_brand_vertical_output_0)
        self.assertEqual(original_brand_vertical_mapping_output.shape[1], expected_brand_vertical_output_1)

    def test_date_limit_handler(self):
        original_date_limit_handler_output = io_tech_etl_script.date_limit_handler(dt.strptime("2018-01-01", "%Y-%m-%d").date(), dt.strptime("2018-01-01", "%Y-%m-%d").date())
        expected_date_limit_handler_output = ["2018-01-01"]
        self.assertEqual(original_date_limit_handler_output, expected_date_limit_handler_output)

    def test_urlencode(self):
        original_urlencode_output = io_tech_etl_script.urlencode(
            {
                "key": "7f530662dee6f132d96a6f3950defc9d",
                "entities": {
                    "categories": {"entity": "categories", "details": ["author", "facebook", "page", "pageviews", "reference_time", "timeread_total", "url", "recirculation", "readability", "timeread", "type_article", "date_pub", "category", "sources", "social", "domain", "trend", "uniques"]},
                    "articles": {"entity": "articles", "details": ["author", "facebook", "page", "pageviews", "reference_time", "timeread_total", "url", "recirculation", "readability", "timeread", "type_article", "date_pub", "category", "sources", "social", "domain", "trend"]},
                    "authors": {"entity": "authors", "details": ["author", "facebook", "page", "pageviews", "reference_time", "timeread_total", "url", "recirculation", "readability", "timeread", "type_article", "date_pub", "category", "sources", "social", "domain", "trend", "count_pub"]},
                    "sources": {"entity": "sources", "details": ["author", "facebook", "page", "pageviews", "reference_time", "timeread_total", "url", "recirculation", "readability", "timeread", "type_article", "date_pub", "category", "sources", "social", "domain", "trend"]},
                    "summary": {"entity": "summary"},
                },
                "options": {"period": {"name": "range", "at_from": "2018-01-01", "at_to": "2018-01-01"}, "per_page": 2500},
            }
        )
        expected_urlencode_output = "entities%5Barticles%5D%5Bdetails%5D%5B%5D=author&entities%5Barticles%5D%5Bdetails%5D%5B%5D=facebook&entities%5Barticles%5D%5Bdetails%5D%5B%5D=page&entities%5Barticles%5D%5Bdetails%5D%5B%5D=pageviews&entities%5Barticles%5D%5Bdetails%5D%5B%5D=reference_time&entities%5Barticles%5D%5Bdetails%5D%5B%5D=timeread_total&entities%5Barticles%5D%5Bdetails%5D%5B%5D=url&entities%5Barticles%5D%5Bdetails%5D%5B%5D=recirculation&entities%5Barticles%5D%5Bdetails%5D%5B%5D=readability&entities%5Barticles%5D%5Bdetails%5D%5B%5D=timeread&entities%5Barticles%5D%5Bdetails%5D%5B%5D=type_article&entities%5Barticles%5D%5Bdetails%5D%5B%5D=date_pub&entities%5Barticles%5D%5Bdetails%5D%5B%5D=category&entities%5Barticles%5D%5Bdetails%5D%5B%5D=sources&entities%5Barticles%5D%5Bdetails%5D%5B%5D=social&entities%5Barticles%5D%5Bdetails%5D%5B%5D=domain&entities%5Barticles%5D%5Bdetails%5D%5B%5D=trend&entities%5Barticles%5D%5Bentity%5D=articles&entities%5Bauthors%5D%5Bdetails%5D%5B%5D=author&entities%5Bauthors%5D%5Bdetails%5D%5B%5D=facebook&entities%5Bauthors%5D%5Bdetails%5D%5B%5D=page&entities%5Bauthors%5D%5Bdetails%5D%5B%5D=pageviews&entities%5Bauthors%5D%5Bdetails%5D%5B%5D=reference_time&entities%5Bauthors%5D%5Bdetails%5D%5B%5D=timeread_total&entities%5Bauthors%5D%5Bdetails%5D%5B%5D=url&entities%5Bauthors%5D%5Bdetails%5D%5B%5D=recirculation&entities%5Bauthors%5D%5Bdetails%5D%5B%5D=readability&entities%5Bauthors%5D%5Bdetails%5D%5B%5D=timeread&entities%5Bauthors%5D%5Bdetails%5D%5B%5D=type_article&entities%5Bauthors%5D%5Bdetails%5D%5B%5D=date_pub&entities%5Bauthors%5D%5Bdetails%5D%5B%5D=category&entities%5Bauthors%5D%5Bdetails%5D%5B%5D=sources&entities%5Bauthors%5D%5Bdetails%5D%5B%5D=social&entities%5Bauthors%5D%5Bdetails%5D%5B%5D=domain&entities%5Bauthors%5D%5Bdetails%5D%5B%5D=trend&entities%5Bauthors%5D%5Bdetails%5D%5B%5D=count_pub&entities%5Bauthors%5D%5Bentity%5D=authors&entities%5Bcategories%5D%5Bdetails%5D%5B%5D=author&entities%5Bcategories%5D%5Bdetails%5D%5B%5D=facebook&entities%5Bcategories%5D%5Bdetails%5D%5B%5D=page&entities%5Bcategories%5D%5Bdetails%5D%5B%5D=pageviews&entities%5Bcategories%5D%5Bdetails%5D%5B%5D=reference_time&entities%5Bcategories%5D%5Bdetails%5D%5B%5D=timeread_total&entities%5Bcategories%5D%5Bdetails%5D%5B%5D=url&entities%5Bcategories%5D%5Bdetails%5D%5B%5D=recirculation&entities%5Bcategories%5D%5Bdetails%5D%5B%5D=readability&entities%5Bcategories%5D%5Bdetails%5D%5B%5D=timeread&entities%5Bcategories%5D%5Bdetails%5D%5B%5D=type_article&entities%5Bcategories%5D%5Bdetails%5D%5B%5D=date_pub&entities%5Bcategories%5D%5Bdetails%5D%5B%5D=category&entities%5Bcategories%5D%5Bdetails%5D%5B%5D=sources&entities%5Bcategories%5D%5Bdetails%5D%5B%5D=social&entities%5Bcategories%5D%5Bdetails%5D%5B%5D=domain&entities%5Bcategories%5D%5Bdetails%5D%5B%5D=trend&entities%5Bcategories%5D%5Bdetails%5D%5B%5D=uniques&entities%5Bcategories%5D%5Bentity%5D=categories&entities%5Bsources%5D%5Bdetails%5D%5B%5D=author&entities%5Bsources%5D%5Bdetails%5D%5B%5D=facebook&entities%5Bsources%5D%5Bdetails%5D%5B%5D=page&entities%5Bsources%5D%5Bdetails%5D%5B%5D=pageviews&entities%5Bsources%5D%5Bdetails%5D%5B%5D=reference_time&entities%5Bsources%5D%5Bdetails%5D%5B%5D=timeread_total&entities%5Bsources%5D%5Bdetails%5D%5B%5D=url&entities%5Bsources%5D%5Bdetails%5D%5B%5D=recirculation&entities%5Bsources%5D%5Bdetails%5D%5B%5D=readability&entities%5Bsources%5D%5Bdetails%5D%5B%5D=timeread&entities%5Bsources%5D%5Bdetails%5D%5B%5D=type_article&entities%5Bsources%5D%5Bdetails%5D%5B%5D=date_pub&entities%5Bsources%5D%5Bdetails%5D%5B%5D=category&entities%5Bsources%5D%5Bdetails%5D%5B%5D=sources&entities%5Bsources%5D%5Bdetails%5D%5B%5D=social&entities%5Bsources%5D%5Bdetails%5D%5B%5D=domain&entities%5Bsources%5D%5Bdetails%5D%5B%5D=trend&entities%5Bsources%5D%5Bentity%5D=sources&entities%5Bsummary%5D%5Bentity%5D=summary&key=7f530662dee6f132d96a6f3950defc9d&options%5Bper_page%5D=2500&options%5Bperiod%5D%5Bat_from%5D=2018-01-01&options%5Bperiod%5D%5Bat_to%5D=2018-01-01&options%5Bperiod%5D%5Bname%5D=range"
        self.assertEqual(original_urlencode_output, expected_urlencode_output)

    def test_flatten(self):
        original_flatten_output = io_tech_etl_script.flatten(
            {
                "key": "7f530662dee6f132d96a6f3950defc9d",
                "entities": {
                    "categories": {"entity": "categories", "details": ["author", "facebook", "page", "pageviews", "reference_time", "timeread_total", "url", "recirculation", "readability", "timeread", "type_article", "date_pub", "category", "sources", "social", "domain", "trend", "uniques"]},
                    "articles": {"entity": "articles", "details": ["author", "facebook", "page", "pageviews", "reference_time", "timeread_total", "url", "recirculation", "readability", "timeread", "type_article", "date_pub", "category", "sources", "social", "domain", "trend"]},
                    "authors": {"entity": "authors", "details": ["author", "facebook", "page", "pageviews", "reference_time", "timeread_total", "url", "recirculation", "readability", "timeread", "type_article", "date_pub", "category", "sources", "social", "domain", "trend", "count_pub"]},
                    "sources": {"entity": "sources", "details": ["author", "facebook", "page", "pageviews", "reference_time", "timeread_total", "url", "recirculation", "readability", "timeread", "type_article", "date_pub", "category", "sources", "social", "domain", "trend"]},
                    "summary": {"entity": "summary"},
                },
                "options": {"period": {"name": "range", "at_from": "2018-01-01", "at_to": "2018-01-01"}, "per_page": 2500},
            }
        )
        expected_flatten_output = [
            ["entities", "articles", "details", ["author", "facebook", "page", "pageviews", "reference_time", "timeread_total", "url", "recirculation", "readability", "timeread", "type_article", "date_pub", "category", "sources", "social", "domain", "trend"]],
            ["entities", "articles", "entity", "articles"],
            ["entities", "authors", "details", ["author", "facebook", "page", "pageviews", "reference_time", "timeread_total", "url", "recirculation", "readability", "timeread", "type_article", "date_pub", "category", "sources", "social", "domain", "trend", "count_pub"]],
            ["entities", "authors", "entity", "authors"],
            ["entities", "categories", "details", ["author", "facebook", "page", "pageviews", "reference_time", "timeread_total", "url", "recirculation", "readability", "timeread", "type_article", "date_pub", "category", "sources", "social", "domain", "trend", "uniques"]],
            ["entities", "categories", "entity", "categories"],
            ["entities", "sources", "details", ["author", "facebook", "page", "pageviews", "reference_time", "timeread_total", "url", "recirculation", "readability", "timeread", "type_article", "date_pub", "category", "sources", "social", "domain", "trend"]],
            ["entities", "sources", "entity", "sources"],
            ["entities", "summary", "entity", "summary"],
            ["key", "7f530662dee6f132d96a6f3950defc9d"],
            ["options", "per_page", 2500],
            ["options", "period", "at_from", "2018-01-01"],
            ["options", "period", "at_to", "2018-01-01"],
            ["options", "period", "name", "range"],
        ]
        self.assertEqual(original_flatten_output, expected_flatten_output)

    def test_parametrize(self):
        original_parametrize_output = io_tech_etl_script.parametrize(
            ["entities", "articles", "details", ["author", "facebook", "page", "pageviews", "reference_time", "timeread_total", "url", "recirculation", "readability", "timeread", "type_article", "date_pub", "category", "sources", "social", "domain", "trend"]]
        )
        expected_parametrize_output = "entities[articles][details][['author', 'facebook', 'page', 'pageviews', 'reference_time', 'timeread_total', 'url', 'recirculation', 'readability', 'timeread', 'type_article', 'date_pub', 'category', 'sources', 'social', 'domain', 'trend']]"
        self.assertEqual(original_parametrize_output, expected_parametrize_output)

    def test_io_tech_api_data_collection(self):
        brand_vertical_mapping_url = "s3://srmg-datalake/brand_vertical_mapping/brand_vertical_mapping_v2.json"
        brand_vertical_df = io_tech_etl_script.get_brand_vertical_mapping(brand_vertical_mapping_url)
        self.assertIsNotNone(brand_vertical_df)
        
        start_date = "2022-01-01"
        end_date = "2022-01-01"
        date_range_arr = pd.date_range(start_date, end_date, freq="D")
        secret_keys_length = 1
        secret_keys = ["7f530662dee6f132d96a6f3950defc9d"]
        end_point = ["6A6q0G6fFuuNzvHu46pbm0Iq8buSpaUw"]
        io_tech_s3_url = "s3://srmg-datalake-test/io_technologies/"
        data_list, date_length, secret_keys_length = io_tech_etl_script.get_api_response(date_range_arr, secret_keys, end_point)
        print(data_list)
        self.assertIsNotNone(data_list)
        #io_tech_etl_script.save_data_frame_to_s3(data_list, date_length, secret_keys_length, io_tech_s3_url, brand_vertical_df)


if __name__ == "__main__":
    test.main()
