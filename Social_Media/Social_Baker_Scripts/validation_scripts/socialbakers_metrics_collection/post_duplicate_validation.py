import awswrangler as wr
import json
import boto3
import pandas as pd
import configparser as cp
from datetime import date as d
s3 = boto3.client('s3')

def read_gen_config():

    config = cp.ConfigParser()
    config.read('config.ini')
    user_choice = int(config['Default']['user_choice'])
    post_url = json.loads(config['Default']['post_url'])

    return user_choice, post_url

def get_cloud_data(p_date,p_user_choice,p_post_url):

    year = p_date
    user_choice = p_user_choice-1
    post_url = p_post_url
    s3_url_smp_part = {0: "facebook_post/"
        , 1: "instagram_post/"
        , 2: "youtube_post/"
        , 3: "linkedin_post/"
        , 4: "twitter_post/"
                        }
    try:
        df_s3_data= wr.s3.read_parquet(
            path= post_url+ s3_url_smp_part[user_choice]+str(year)+".parquet")
    except Exception as e:
        print("Exception",e,"has occured while reading the s3 file")

        return 1,pd.DataFrame()

    return 2,df_s3_data

def dupe_check(p_df):

    df_s3_data=p_df

    if df_s3_data[df_s3_data.duplicated()].shape[0] > 0:
        return_val= True
        dup_df = df_s3_data[df_s3_data.duplicated()]
    else:
        return_val= False
        dup_df = pd.DataFrame()

    return return_val,dup_df

def main():

    user_choice,post_url=read_gen_config()

    start_year = 2015
    current_date = d.today()
    end_year = int(current_date.year)

    for i in range(start_year,end_year+1):
        flag,df_s3_data = get_cloud_data(i,user_choice,post_url)

        if flag==2:
            flag1,df_dub_s3_data = dupe_check(df_s3_data)

            if flag1 == False:
                print("No Duplicates in ",i)
            else:
                print("Duplicates found in ",i)
                print(df_dub_s3_data)
        else:
            pass

    return None

if __name__ == "__main__":
    main()