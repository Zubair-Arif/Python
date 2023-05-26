import pandas as pd
import json
import configparser as cp
import awswrangler as wr
from datetime import date as d


def read_gen_config():
    config = cp.ConfigParser()
    config.read('config.ini')
    brand_vertical_mapping_config = config['Default']['brand_vertical_mapping']
    post_url = json.loads(config['Default']['post_url'])
    user_choice = int(config["Default"]['user_choice'])
    return brand_vertical_mapping_config,post_url,user_choice

def get_cloud_data(p_year, p_post_url,p_user_choice):
    
    year = p_year
    post_url = p_post_url
    user_choice = p_user_choice -1
    s3_url_smp_part = {0: "facebook_post/"
                    , 1: "instagram_post/"
                    , 2: "youtube_post/"
                    , 3: "linkedin_post/"
                    , 4: "twitter_post/"
                     }
    try:
        df_s3 = wr.s3.read_parquet(
            path=post_url+s3_url_smp_part[user_choice] + str(year) + ".parquet")
    except Exception as e:
        print("Exception ", e, " has occured while reading the s3 file")
        return False,pd.DataFrame


    return True,df_s3


def validate_brand_vertical_mapping(p_brand_vertical_mapping_config,p_df_s3,p_user_choice):
    brand_vertical_mapping_config = p_brand_vertical_mapping_config
    df_s3 = p_df_s3
    user_choice = p_user_choice-1
    df_brand_vertical_mapping_config = pd.DataFrame(json.loads(brand_vertical_mapping_config))
    profile_id= {0: 'facebook_profile_id'
                    , 1: 'instagram_prfoile_id'
                    , 2: 'youtube_profile_id'
                    , 3: 'linkedin_profile_id'
                    , 4: 'twitter_profile_id'
                     }
    df_brand_vertical_mapping_config[profile_id[user_choice]] = df_brand_vertical_mapping_config[profile_id[user_choice]].astype("Int64")
    df_brand_vertical_mapping_config = df_brand_vertical_mapping_config.astype('string')
    df_s3_null = df_s3.loc[(df_s3['profile_id'].isnull()) | (df_s3['post_id'].isnull()) | (df_s3['brand'].isnull()) | (df_s3['vertical'].isnull())]
    if df_s3_null.shape[0] == 0:
        df_s3_brand_vertical = df_s3[[ "profile_id", "brand",'vertical']].drop_duplicates().reset_index(drop=True)
        df_brand_vertical_config =  df_brand_vertical_mapping_config[[profile_id[user_choice], "brand",'vertical']].dropna().drop_duplicates().reset_index(drop = True)
        df_brand_vertical_config.rename(columns = {profile_id[user_choice]:'profile_id'}, inplace = True)
        df_brand_vertical = pd.merge(df_brand_vertical_config, df_s3_brand_vertical, how='inner' )
        if df_brand_vertical.shape[0] == df_s3_brand_vertical.shape[0] :
            df_diff = pd.DataFrame()
            return 1,df_diff
        else:
            df_diff = pd.concat([df_brand_vertical,df_s3_brand_vertical]).drop_duplicates(keep=False)
            return 2,df_diff
    else:
        return 3,df_s3_null


def main():
    brand_vertical_mapping_config,post_url,user_choice=read_gen_config()
    start_year = 2020
    today = d.today()
    end_year = int(today.year)


    for year in range(start_year,end_year):
    
        flag1,df_s3 = get_cloud_data(year,post_url,user_choice)
        if flag1:
            flag,df_diff = validate_brand_vertical_mapping(brand_vertical_mapping_config,df_s3,user_choice)
            if flag == 1:
                print("brand vertical mapping matching for the year :",year)       
            elif flag == 2:
                print("brand vertical mapping not matching for the year :",year)
                print(df_diff)
            else : 
                print("Null values are there brand vertical mapping for the year :",year)
                print(df_diff)
        else:
            pass
    print("brand vertical mapping validation completed")
    



    return 1


if __name__=="__main__":
    main()