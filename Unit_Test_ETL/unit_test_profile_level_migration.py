import unittest as test
import profile_level_metrics_collector
from datetime import datetime as dt
import boto3

secret = boto3.client("ssm")

user_id = secret.get_parameter(Name="srmg_socialbakers_secret_param_user_id", WithDecryption=True)["Parameter"]["Value"]
password = secret.get_parameter(Name="srmg_socialbakers_secret_param_password", WithDecryption=True)["Parameter"]["Value"]


class test_profile_migration(test.TestCase):
    def test_read_gen_config(self):
        original_read_gen_config_output = profile_level_migration_sample.read_gen_config()
        expected_read_gen_config_output = [
            "Njc1OTQ5XzE4NTM0NTlfMTU4MjczMzAwODc1NV8wYTliOGUyZWI4MzBhMmJhMmYzZDJhODEwOGY0MWFiNg==",
            "ad7159bb6aff5802360c209280ff9ee1",
            1,
            dt.strptime("2018-01-01", "%Y-%m-%d").date(),
            dt.strptime("2018-01-01", "%Y-%m-%d").date(),
            [2018],
            "95",
            "s3://srmg-datalake-test/test_2/social_bakers/post-level-metrics/",
            "s3://srmg-datalake/social_media_metrics/social_bakers/brand_vertical_mapping.json",
            [{"field": "created_time", "order": "asc"}],
        ]
        self.assertEqual(original_read_gen_config_output, expected_read_gen_config_output)

    def test_read_sm_config(self):
        original_sm_config_output = profile_level_migration_sample.read_sm_config(1)
        expected_sm_config_output = (
            [
                "1430891407210428",
                "221581034584506",
                "113791238657176",
                "114884335231354",
                "163677042373",
                "1674770609433599",
                "7382473689",
                "10250877124",
                "108426177368030",
                "120209602702665",
                "502955846753690",
                "1113776355382748",
                "1454248191455492",
                "207063552693995",
                "198984916805241",
                "145726335465699",
                "64040652712",
                "126548377386804",
                "143017562431177",
                "115308694215",
                "1167599016588010",
                "303651683481268",
                "1951056435194177",
                "336214887203607",
                "276135229843412",
                "1067067579997367",
                "149486508564346",
                "281980642874",
                "156979601031358",
                "352403315134298",
                "104555581941770",
                "113723777176236",
                "106210075240636",
                "629097450488980",
                "114291218610934",
                "1450169428329951",
                "137192489702943",
                "264907087015052",
                "652020488173369",
            ],
            [
                "attachments",
                "author",
                "authorId",
                "comments",
                "comments_sentiment",
                "content",
                "content_type",
                "created_time",
                "deleted",
                "grade",
                "hidden",
                "id",
                "interactions",
                "interactions_per_1k_fans",
                "media_type",
                "origin",
                "page",
                "post_attribution",
                "post_labels",
                "profileId",
                "published",
                "reactions",
                "reactions_by_type",
                "sentiment",
                "shares",
                "spam",
                "universal_video_id",
                "url",
                "video",
                "insights_engaged_users",
                "insights_engagements",
                "insights_impressions",
                "insights_impressions_by_post_attribution",
                "insights_impressions_engagement_rate",
                "insights_interactions",
                "insights_interactions_by_interaction_type",
                "insights_negative_feedback_unique",
                "insights_post_clicks",
                "insights_post_clicks_by_clicks_type",
                "insights_post_clicks_unique",
                "insights_reach",
                "insights_reach_by_post_attribution",
                "insights_reach_engagement_rate",
                "insights_reactions",
                "insights_reactions_by_type",
                "insights_video_view_time",
                "insights_video_view_time_average",
                "insights_video_view_time_by_country",
                "insights_video_view_time_by_distribution",
                "insights_video_view_time_by_gender_age",
                "insights_video_view_time_by_post_attribution",
                "insights_video_views",
                "insights_video_views_10s",
                "insights_video_views_10s_by_play_type",
                "insights_video_views_10s_by_post_attribution",
                "insights_video_views_10s_by_sound",
                "insights_video_views_10s_unique",
                "insights_video_views_30s",
                "insights_video_views_30s_by_play_type",
                "insights_video_views_30s_by_post_attribution",
                "insights_video_views_30s_unique",
                "insights_video_views_average_completion",
                "insights_video_views_by_play_type",
                "insights_video_views_by_post_attribution",
                "insights_video_views_by_sound",
                "insights_video_views_complete",
                "insights_video_views_complete_by_post_attribution",
                "insights_video_views_complete_unique",
                "insights_video_views_complete_unique_by_post_attribution",
                "insights_video_views_distribution",
                "insights_video_views_unique",
                "insights_video_views_unique_by_post_attribution",
            ],
        )
        self.assertEqual(original_sm_config_output, expected_sm_config_output)

    def test_get_brand_vertical_mapping(self):
        original_brand_vertical_output = profile_level_migration_sample.get_brand_vertical_mapping("s3://srmg-datalake/social_media_metrics/social_bakers/brand_vertical_mapping.json")
        expected_brand_vertical_output_0 = 57
        expected_brand_vertical_output_1 = 7
        self.assertEqual(original_brand_vertical_output.shape[0], expected_brand_vertical_output_0)
        self.assertEqual(original_brand_vertical_output.shape[1], expected_brand_vertical_output_1)
        return original_brand_vertical_output

    def test_date_limit_handler(self):
        original_start_date_output, original_end_date_output = profile_level_migration_sample.date_limit_handler(dt.strptime("2018-01-01", "%Y-%m-%d").date(), dt.strptime("2018-01-01", "%Y-%m-%d").date(), 1)
        expected_start_date_output = [dt.strptime("2018-01-01", "%Y-%m-%d").date()]
        expected_end_date_output = [dt.strptime("2018-01-01", "%Y-%m-%d").date()]
        self.assertEqual(original_start_date_output, expected_start_date_output)
        self.assertEqual(original_end_date_output, expected_end_date_output)

    def test_get_api_response(self):
        original_profile_api_response = profile_level_migration_sample.get_api_response(
            user_id,
            password,
            1,
            [dt.strptime("2018-01-01", "%Y-%m-%d").date()],
            [dt.strptime("2018-01-01", "%Y-%m-%d").date()],
            ["113791238657176"],
            [
                "attachments",
                "author",
                "authorId",
                "comments",
                "comments_sentiment",
                "content",
                "content_type",
                "created_time",
                "deleted",
                "grade",
                "hidden",
                "id",
                "interactions",
                "interactions_per_1k_fans",
                "media_type",
                "origin",
                "page",
                "post_attribution",
                "post_labels",
                "profileId",
                "published",
                "reactions",
                "reactions_by_type",
                "sentiment",
                "shares",
                "spam",
                "universal_video_id",
                "url",
                "video",
                "insights_engaged_users",
                "insights_engagements",
                "insights_impressions",
                "insights_impressions_by_post_attribution",
                "insights_impressions_engagement_rate",
                "insights_interactions",
                "insights_interactions_by_interaction_type",
                "insights_negative_feedback_unique",
                "insights_post_clicks",
                "insights_post_clicks_by_clicks_type",
                "insights_post_clicks_unique",
                "insights_reach",
                "insights_reach_by_post_attribution",
                "insights_reach_engagement_rate",
                "insights_reactions",
                "insights_reactions_by_type",
                "insights_video_view_time",
                "insights_video_view_time_average",
                "insights_video_view_time_by_country",
                "insights_video_view_time_by_distribution",
                "insights_video_view_time_by_gender_age",
                "insights_video_view_time_by_post_attribution",
                "insights_video_views",
                "insights_video_views_10s",
                "insights_video_views_10s_by_play_type",
                "insights_video_views_10s_by_post_attribution",
                "insights_video_views_10s_by_sound",
                "insights_video_views_10s_unique",
                "insights_video_views_30s",
                "insights_video_views_30s_by_play_type",
                "insights_video_views_30s_by_post_attribution",
                "insights_video_views_30s_unique",
                "insights_video_views_average_completion",
                "insights_video_views_by_play_type",
                "insights_video_views_by_post_attribution",
                "insights_video_views_by_sound",
                "insights_video_views_complete",
                "insights_video_views_complete_by_post_attribution",
                "insights_video_views_complete_unique",
                "insights_video_views_complete_unique_by_post_attribution",
                "insights_video_views_distribution",
                "insights_video_views_unique",
                "insights_video_views_unique_by_post_attribution",
            ],
            95,
            [{"field": "created_time", "order": "asc"}],
        )
        expected_profile_api_response = 29
        self.assertEqual(len(original_profile_api_response), expected_profile_api_response)
        return original_profile_api_response

    def test_post_level_metrics_fetcher(self):
        profile_api_response = self.test_get_api_response()
        original_post_level_fetcher_output = profile_level_migration_sample.post_level_metrics_fetcher(1, profile_api_response, "s3://srmg-datalake-test/test_2/social_bakers/post-level-metrics/")
        expected_post_level_fetcher_output = 29
        self.assertEqual(len(original_post_level_fetcher_output), expected_post_level_fetcher_output)
        return original_post_level_fetcher_output

    def test_beautify_post_metrics(self):
        brand_vertical_df = self.test_get_brand_vertical_mapping()
        post_metrics_response_df = self.test_post_level_metrics_fetcher()

        original_beautify_post_metrics_output = profile_level_migration_sample.beautify_post_metrics(1, brand_vertical_df, post_metrics_response_df)

        expected_beautify_post_metrics_output_0 = 29
        expected_beautify_post_metrics_output_1 = 46

        self.assertEqual(original_beautify_post_metrics_output.shape[0], expected_beautify_post_metrics_output_0)
        self.assertEqual(original_beautify_post_metrics_output.shape[1], expected_beautify_post_metrics_output_1)

    def test_main(self):
        original_main_func_output = profile_level_migration_sample.main()
        expected_main_func_output = 1
        self.assertEqual(original_main_func_output, expected_main_func_output)


if __name__ == "__main__":
    test.main()
