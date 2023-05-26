import awswrangler as wr
from datetime import date as d, datetime as dt, timedelta
import pandas as pd

def get_cloud_data(date):

    df_s3_data= wr.s3.read_parquet(
        path="s3://srmg-datalake/test_2/social_bakers/post-level-metrics/processed/facebook_post/" + str(
            date) + ".parquet")

    return df_s3_data

def main():

    start_date = dt.strptime("2016-01-01", "%Y-%m-%d").date()
    end_date = d.today() - timedelta(days=1)

    df_s3_data_initial=get_cloud_data(2015)

    start_yr = start_date.year
    end_yr = end_date.year

    for i in range(start_yr, end_yr + 1):
        
        df_s3_data = get_cloud_data(2021)
        
        print(i,":")

        for j in range(df_s3_data.shape[1]):

            if(df_s3_data_initial.columns[j] != df_s3_data.columns[j]):
                print(i,"has a mismatch in the column",df_s3_data.columns[j])
    
        # df_s3_month = df_s3_data.groupby(df_s3_data.created_time.dt.month)['post_id'].count()
        # print(df_s3_month)

        # if(temp.shape[0]>0):
        #     print("2015 and", i , "length of the columns are not equal")

if __name__ == "__main__":
    main()

