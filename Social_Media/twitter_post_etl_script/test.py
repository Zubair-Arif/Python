import twitter_post
import unittest
import pandas as pd
import sys

class twitter_post_metrics(unittest.TestCase):

    def test_read_config(self):
        [bearer_token, actual_CONSUMER_KEY, actual_CONSUMER_SECRET, actual_ACCESS_TOKEN, actual_ACCESS_SECRET, actual_profile_ids, actual_brand_vertical_mapping_url,dict_datatype,s3_path] = twitter_post.read_config()
        expected_CONSUMER_KEY = "zS2dho2nhu7ishB91yn6xpVAw"
        self.assertEqual(actual_CONSUMER_KEY,expected_CONSUMER_KEY)
        expected_CONSUMER_SECRET="AsUdqFbGAIT0NRFmAnz9Uf1eABlaVHZ6JujZwKeefsDg6zs4BP"
        self.assertEqual(actual_CONSUMER_SECRET,expected_CONSUMER_SECRET)
        expected_ACCESS_TOKEN="1569658977616809984-ntKSGR8uhuKmwANNlpOsE6Hwthapkh"
        self.assertEqual(actual_ACCESS_TOKEN,expected_ACCESS_TOKEN)
        expected_ACCESS_SECRET="c5qI44nhR3Dnc8kHhC8elTRJCfdOJEqLMaYgPD1Ek0eYi"
        self.assertEqual(actual_ACCESS_SECRET,expected_ACCESS_SECRET)
        expected_profile_ids=["169108336"]
        self.assertEqual(actual_profile_ids,expected_profile_ids)
        return bearer_token, actual_CONSUMER_KEY, actual_CONSUMER_SECRET, actual_ACCESS_TOKEN, actual_ACCESS_SECRET, actual_profile_ids, actual_brand_vertical_mapping_url,dict_datatype,s3_path

    def test_brand_vertical_mapping(self):
        [bearer_token, actual_CONSUMER_KEY, actual_CONSUMER_SECRET, actual_ACCESS_TOKEN, actual_ACCESS_SECRET, actual_profile_ids, brand_vertical_mapping_url,dict_datatype,s3_path]=self.test_read_config()
        self.actual_df=twitter_post.brand_vertical_mapping(brand_vertical_mapping_url)
        actual_response= self.actual_df.shape[0]
        expected_response=62
        self.assertEqual(actual_response,expected_response)

    def test_process_tweepy_data(self):
        bearer_token, CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET, profile_ids, brand_vertical_mapping_url, dict_datatype, s3_path = self.test_read_config()
        df = twitter_post.process_tweepy_data(bearer_token, CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET, profile_ids,brand_vertical_mapping_url, dict_datatype, s3_path)
        print(df)
        actual_response=df.shape[1]
        expected_response=3
        self.assertEqual(actual_response,expected_response)

if __name__ == "__main__":
    unittest.main(warnings=ignore)
