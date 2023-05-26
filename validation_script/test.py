import master_validation
import unittest
import pandas as pd
import boto3
from datetime import datetime as dt

secret = boto3.client("ssm")

user_id = secret.get_parameter(Name="srmg_socialbakers_secret_param_user_id", WithDecryption=True)["Parameter"]["Value"]
password = secret.get_parameter(Name="srmg_socialbakers_secret_param_password", WithDecryption=True)["Parameter"]["Value"]


class master_validation_test(unittest.TestCase):
    def test_dup_check(self):
        test_df = pd.DataFrame({"name": ["aru", "aru"], "age": [1, 1]}, index=[1, 2])
        resulted_df = master_validation.dupe_check(test_df)
        expected_df = pd.DataFrame({"name": ["aru"], "age": [1]}, index=[2])
        pd.testing.assert_frame_equal(expected_df, resulted_df)

    def test_get_brand_vertical_mapping(self):
        original_brand_vertical_output = master_validation.get_brand_vertical_mapping("s3://srmg-datalake/social_media_metrics/social_bakers/brand_vertical_mapping.json")
        expected_brand_vertical_output_0 = 57
        expected_brand_vertical_output_1 = 7
        self.assertEqual(original_brand_vertical_output.shape[0], expected_brand_vertical_output_0)
        self.assertEqual(original_brand_vertical_output.shape[1], expected_brand_vertical_output_1)

    def test_get_api_facebook_response(self):
        real_count = master_validation.get_api_response(
            user_id,
            password,
            1,
            dt.strptime("2022-01-01", "%Y-%m-%d").date(),
            dt.strptime("2022-01-01", "%Y-%m-%d").date(),
            dt.strptime("2022-01-02", "%Y-%m-%d").date(),
            "https://api.socialbakers.com/3/",
            ["1430891407210428"],
            ["created_time"],
            100,
            [{"field": "created_time", "order": "asc"}],
        )
        expected_count = 18
        self.assertEqual(expected_count, real_count)

    def test_get_api_instagram_response(self):
        real_count = master_validation.get_api_response(
            user_id,
            password,
            2,
            dt.strptime("2022-01-01", "%Y-%m-%d").date(),
            dt.strptime("2022-01-01", "%Y-%m-%d").date(),
            dt.strptime("2022-01-02", "%Y-%m-%d").date(),
            "https://api.socialbakers.com/3/",
            ["17841404482027591"],
            ["created_time"],
            100,
            [{"field": "created_time", "order": "asc"}],
        )
        expected_count = 2
        self.assertEqual(expected_count, real_count)

    def test_get_api_youtube_response(self):
        real_count = master_validation.get_api_response(
            user_id,
            password,
            3,
            dt.strptime("2022-01-01", "%Y-%m-%d").date(),
            dt.strptime("2022-01-01", "%Y-%m-%d").date(),
            dt.strptime("2022-01-02", "%Y-%m-%d").date(),
            "https://api.socialbakers.com/3/",
            ["UC6JgJJE7M8Vtx6NktbASSiA"],
            ["created_time"],
            100,
            [{"field": "created_time", "order": "asc"}],
        )
        expected_count = 0
        self.assertEqual(expected_count, real_count)

    def test_get_api_linkedin_response(self):
        real_count = master_validation.get_api_response(
            user_id,
            password,
            4,
            dt.strptime("2022-01-01", "%Y-%m-%d").date(),
            dt.strptime("2022-01-01", "%Y-%m-%d").date(),
            dt.strptime("2022-01-02", "%Y-%m-%d").date(),
            "https://api.socialbakers.com/3/",
            ["urn:li:organization:3222493"],
            ["created_time"],
            100,
            [{"field": "created_time", "order": "asc"}],
        )
        expected_count = 8
        self.assertEqual(expected_count, real_count)

    def test_extract_date_validation(self):
        test_df = pd.DataFrame(
            {
                "created_time": [dt.strptime("2022-01-01", "%Y-%m-%d"), dt.strptime("2022-01-02", "%Y-%m-%d"), dt.strptime("2022-01-03", "%Y-%m-%d")],
                "date": [dt.strptime("2022-01-01", "%Y-%m-%d"), dt.strptime("2022-01-02", "%Y-%m-%d"), dt.strptime("2022-01-03", "%Y-%m-%d")],
                "year": [2022, 2022, 2022],
                "month": ["Jan", "Jan", "Jan"],
            }
        )
        result, resulted_df = master_validation.extract_date_validate(test_df)
        self.assertEqual(result, "None")
        expected_df = pd.DataFrame()
        pd.testing.assert_frame_equal(expected_df, resulted_df)

    def test_get_cloud_data_facebook_row(self):
        resulted_df = master_validation.get_cloud_data(2022, dt.strptime("2022-01-01", "%Y-%m-%d").date(), dt.strptime("2022-01-03", "%Y-%m-%d").date(), 1, "s3://srmg-datalake/social_media_metrics/social_bakers/post-level-metrics/")
        row_len_resulted_df = resulted_df.shape[0]
        row_len_expected_df = 1721
        self.assertEqual(row_len_expected_df, row_len_resulted_df)

    def test_get_cloud_data_facebook_column(self):
        resulted_df = master_validation.get_cloud_data(2022, dt.strptime("2022-01-01", "%Y-%m-%d").date(), dt.strptime("2022-01-03", "%Y-%m-%d").date(), 1, "s3://srmg-datalake/social_media_metrics/social_bakers/post-level-metrics/")
        col_len_resulted_df = resulted_df.shape[1]
        col_len_expected_df = 65
        self.assertEqual(col_len_expected_df, col_len_resulted_df)

    def test_get_cloud_data_instagram_row(self):
        resulted_df = master_validation.get_cloud_data(2022, dt.strptime("2022-01-01", "%Y-%m-%d").date(), dt.strptime("2022-01-03", "%Y-%m-%d").date(), 2, "s3://srmg-datalake/social_media_metrics/social_bakers/post-level-metrics/")
        row_len_resulted_df = resulted_df.shape[0]
        row_len_expected_df = 362
        self.assertEqual(row_len_expected_df, row_len_resulted_df)

    def test_get_cloud_data_instagram_column(self):
        resulted_df = master_validation.get_cloud_data(2022, dt.strptime("2022-01-01", "%Y-%m-%d").date(), dt.strptime("2022-01-03", "%Y-%m-%d").date(), 2, "s3://srmg-datalake/social_media_metrics/social_bakers/post-level-metrics/")
        col_len_resulted_df = resulted_df.shape[1]
        col_len_expected_df = 46
        self.assertEqual(col_len_expected_df, col_len_resulted_df)

    def test_get_cloud_data_youtube_row(self):
        resulted_df = master_validation.get_cloud_data(2022, dt.strptime("2022-01-01", "%Y-%m-%d").date(), dt.strptime("2022-01-03", "%Y-%m-%d").date(), 3, "s3://srmg-datalake/social_media_metrics/social_bakers/post-level-metrics/")
        row_len_resulted_df = resulted_df.shape[0]
        row_len_expected_df = 7
        self.assertEqual(row_len_expected_df, row_len_resulted_df)

    def test_get_cloud_data_youtube_column(self):
        resulted_df = master_validation.get_cloud_data(2022, dt.strptime("2022-01-01", "%Y-%m-%d").date(), dt.strptime("2022-01-03", "%Y-%m-%d").date(), 3, "s3://srmg-datalake/social_media_metrics/social_bakers/post-level-metrics/")
        col_len_resulted_df = resulted_df.shape[1]
        col_len_expected_df = 28
        self.assertEqual(col_len_expected_df, col_len_resulted_df)

    def test_get_cloud_data_linkedin_row(self):
        resulted_df = master_validation.get_cloud_data(2022, dt.strptime("2022-01-01", "%Y-%m-%d").date(), dt.strptime("2022-01-03", "%Y-%m-%d").date(), 4, "s3://srmg-datalake/social_media_metrics/social_bakers/post-level-metrics/")
        row_len_resulted_df = resulted_df.shape[0]
        row_len_expected_df = 17
        self.assertEqual(row_len_expected_df, row_len_resulted_df)

    def test_get_cloud_data_linkedin_column(self):
        resulted_df = master_validation.get_cloud_data(2022, dt.strptime("2022-01-01", "%Y-%m-%d").date(), dt.strptime("2022-01-03", "%Y-%m-%d").date(), 4, "s3://srmg-datalake/social_media_metrics/social_bakers/post-level-metrics/")
        col_len_resulted_df = resulted_df.shape[1]
        col_len_expected_df = 24
        self.assertEqual(col_len_expected_df, col_len_resulted_df)

    def test_year_limit_validation(self):
        test_df = pd.DataFrame({"date": [dt.strptime("2022-01-01", "%Y-%m-%d"), dt.strptime("2022-01-02", "%Y-%m-%d"), dt.strptime("2022-01-03", "%Y-%m-%d")], "post_id": ["1", "2", "3"]})
        resulted_df = master_validation.year_limit_validate(2022, dt.strptime("2022-01-01", "%Y-%m-%d"), dt.strptime("2022-01-03", "%Y-%m-%d"), test_df)
        expected_df = pd.DataFrame()
        pd.testing.assert_frame_equal(expected_df, resulted_df)

    def test_duplicate_s3_data(self):
        test_df = pd.DataFrame({"ga_date": ["aru", "arumugam"], "age": [1, 2]}, index=[1, 2])
        resulted_df = master_validation.duplicate_s3_data(test_df, "fake_url")
        self.assertEqual(False, resulted_df)

    def test_read_config(self):
        [user_id, password, user_choice, start_date, end_date, limit, post_url, post_api_url, post_level_metrics_sort, profile_ids, post_level_metrics, brand_vertical_mapping_url] = master_validation.read_gen_config()

        expected_post_url = "s3://srmg-datalake/social_media_metrics/social_bakers/post-level-metrics/"

        self.assertEqual(post_url, expected_post_url)

    def test_social_bakers_validation(self):
        result = master_validation.social_bakers_validation()
        self.assertEqual("success", result)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
