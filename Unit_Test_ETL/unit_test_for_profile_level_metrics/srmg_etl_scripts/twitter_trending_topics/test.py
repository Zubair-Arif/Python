import twitter_trending_topics
import unittest
import pandas as pd
class twitter_trending_tests(unittest.TestCase):

    def test_twitter_read(self):
        self.actual_response = twitter_trending_topics.twitter_read("zS2dho2nhu7ishB91yn6xpVAw","AsUdqFbGAIT0NRFmAnz9Uf1eABlaVHZ6JujZwKeefsDg6zs4BP","1569658977616809984-ntKSGR8uhuKmwANNlpOsE6Hwthapkh","c5qI44nhR3Dnc8kHhC8elTRJCfdOJEqLMaYgPD1Ek0eYi")
        self.actual_response_str = str(self.actual_response)[:25]
        self.assertEqual(self.actual_response_str,"<tweepy.api.API object at")
        return  self.actual_response

    def test_normalize_response_rows(self):
        self.actual_response = self.test_twitter_read()
        df,df_bulk = twitter_trending_topics.normalize_response(self.actual_response,[23424938,23424738,23424802,23424922,23424860,23424848,23424969], ["KSA","UAE","Egypt","Pakistan","Jordan","India","Turkey"])
        df_row_len = df.shape[0]
        df_bulk_row_len = df_bulk.shape[0]
        self.assertEqual(df_row_len, 50)
        self.assertEqual(df_bulk_row_len,50)
        return df,df_bulk

    def test_normalize_response_columns(self):
        self.actual_response = self.test_twitter_read()
        df, df_bulk = twitter_trending_topics.normalize_response(self.actual_response, [23424938,23424738,23424802,23424922,23424860,23424848,23424969], ["KSA","UAE","Egypt","Pakistan","Jordan","India","Turkey"])
        df_column_len = df.shape[1]
        df_bulk_column_len = df_bulk.shape[1]
        self.assertEqual(df_column_len, 9)
        self.assertEqual(df_bulk_column_len, 9)

    def test_twitter_write_files(self):
        self.df,self.df_bulk= self.test_normalize_response_rows()
        result = twitter_trending_topics.twitter_write_files(self.df,"KSA")
        self.assertEqual(result, "written")

    def test_twitter_write_bulk_files(self):
        self.df,self.df_bulk = self.test_normalize_response_rows()
        result = twitter_trending_topics.twitter_bulk_files(self.df_bulk)
        self.assertEqual(result, "written")
if __name__=="__main__":
    unittest.main(warnings="ignore")
