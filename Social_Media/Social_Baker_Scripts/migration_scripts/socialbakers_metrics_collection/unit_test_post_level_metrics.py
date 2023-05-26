import unittest as test
import post_level_metrics_collector
from datetime import datetime as dt
import boto3

secret = boto3.client("ssm","eu-west-1")

user_id = secret.get_parameter(Name="srmg_socialbakers_secret_param_user_id", WithDecryption=True)["Parameter"]["Value"]
password = secret.get_parameter(Name="srmg_socialbakers_secret_param_password", WithDecryption=True)["Parameter"]["Value"]


class test_post_level_metrics(test.TestCase):
    def test_read_gen_config(self):
        [user_id, password, user_choice, start_date, end_date, yrs_in_req_period, limit, post_url, brand_vertical_mapping_url, post_level_metrics_sort] = post_level_metrics_collector.read_gen_config()
    
        self.assertEqual(user_choice, 1)
        self.assertEqual(post_url, "s3://srmg-datalake/test_2/social_bakers/post-level-metrics/")

    def test_get_brand_vertical_mapping(self):
        brand_vertical_df = post_level_metrics_collector.get_brand_vertical_mapping("s3://srmg-datalake/social_media_metrics/social_bakers/brand_vertical_mapping.json")
    
        self.assertIsNotNone(brand_vertical_df)

    def test_read_sm_config(self):
        global PARAM_CHOICE

        PARAM_CHOICE = ["Facebook", "Instagram", "Youtube", "LinkedIn", "Twitter"]
        [profile_ids, post_level_metrics ] = post_level_metrics_collector.read_sm_config(1)
    
        expected_post_level_metrics = ["attachments", "author","authorId","comments","comments_sentiment","content","content_type","created_time","deleted"
                ,"grade","hidden","id","interactions","interactions_per_1k_fans","media_type","origin","page","post_attribution"
                ,"post_labels","profileId","published","reactions","reactions_by_type","sentiment","shares","spam","universal_video_id","url","video"
                ,"insights_engaged_users","insights_engagements","insights_impressions","insights_impressions_by_post_attribution"
                ,"insights_impressions_engagement_rate","insights_interactions","insights_interactions_by_interaction_type"
                ,"insights_negative_feedback_unique","insights_post_clicks","insights_post_clicks_by_clicks_type","insights_post_clicks_unique"
                ,"insights_reach","insights_reach_by_post_attribution","insights_reach_engagement_rate","insights_reactions"
                ,"insights_reactions_by_type","insights_video_view_time","insights_video_view_time_average","insights_video_view_time_by_country"
                ,"insights_video_view_time_by_distribution","insights_video_view_time_by_gender_age","insights_video_view_time_by_post_attribution"
                ,"insights_video_views","insights_video_views_10s","insights_video_views_10s_by_play_type","insights_video_views_10s_by_post_attribution"
                ,"insights_video_views_10s_by_sound","insights_video_views_10s_unique","insights_video_views_30s"
                ,"insights_video_views_30s_by_play_type","insights_video_views_30s_by_post_attribution","insights_video_views_30s_unique"
                ,"insights_video_views_average_completion","insights_video_views_by_play_type","insights_video_views_by_post_attribution"
                ,"insights_video_views_by_sound","insights_video_views_complete","insights_video_views_complete_by_post_attribution"
                ,"insights_video_views_complete_unique","insights_video_views_complete_unique_by_post_attribution"
                ,"insights_video_views_distribution","insights_video_views_unique","insights_video_views_unique_by_post_attribution"]

        self.assertIsNotNone(profile_ids)    
        self.assertIsNotNone(post_level_metrics)    
        self.assertEqual(post_level_metrics,expected_post_level_metrics)    

    def test_date_limit_handler(self):
        original_start_date_output, original_end_date_output = post_level_metrics_collector.date_limit_handler(dt.strptime("2018-01-01", "%Y-%m-%d").date(), dt.strptime("2018-01-01", "%Y-%m-%d").date(), 1,1)
        expected_start_date_output = [dt.strptime("2018-01-01", "%Y-%m-%d").date()]
        expected_end_date_output = [dt.strptime("2018-01-01", "%Y-%m-%d").date()]
        self.assertEqual(original_start_date_output, expected_start_date_output)
        self.assertEqual(original_end_date_output, expected_end_date_output)    

        original_start_date_output, original_end_date_output = post_level_metrics_collector.date_limit_handler(dt.strptime("2016-01-01", "%Y-%m-%d").date(), dt.strptime("2018-01-01", "%Y-%m-%d").date(), 1,1)

        self.assertIsNotNone(original_start_date_output)
        self.assertIsNotNone(original_end_date_output)   
        original_start_date_output, original_end_date_output = post_level_metrics_collector.date_limit_handler(dt.strptime("2016-01-01", "%Y-%m-%d").date(), dt.strptime("2018-01-01", "%Y-%m-%d").date(), 1,2)

        self.assertIsNotNone(original_start_date_output)
        self.assertIsNotNone(original_end_date_output)     

    def test_get_api_response(self):
        original_post_api_response = post_level_metrics_collector.get_api_response(
            user_id,
            password,
            1,
            [dt.strptime("2018-01-01", "%Y-%m-%d").date()],
            [dt.strptime("2018-01-01", "%Y-%m-%d").date()],
            ["1430891407210428"],
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
            1,
        )
        self.assertIsNotNone(original_post_api_response)

if __name__ == "__main__":
    test.main()
