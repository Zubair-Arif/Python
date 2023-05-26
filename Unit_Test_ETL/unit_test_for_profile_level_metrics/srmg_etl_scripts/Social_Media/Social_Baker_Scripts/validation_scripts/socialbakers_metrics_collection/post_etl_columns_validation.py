from datetime import date as d, datetime as dt, timedelta
import configparser as cp
import awswrangler as wr

def read_config():
    config = cp.ConfigParser()
    config.read('config.ini')
    user_choice = config['Default']['user_choice']
    post_url = config['Default']['post_url']
    return user_choice,post_url

def get_cloud_data(p_year,p_user_choice,p_post_url):
    
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
        df_s3_data= wr.s3.read_parquet(path=post_url+s3_url_smp_part[user_choice]+ str(year) + ".parquet")
        return df_s3_data

    except:
        print("No data Found")


def extract_date_validate(p_user_choice,p_post_url):
    user_choice=int(p_user_choice)
    post_url=p_post_url
    start_date= dt.strptime('2015-01-01','%Y-%m-%d').date()
    end_date=d.today()-timedelta(days=1)

    start_yr=start_date.year
    end_yr=end_date.year
   
    for i in range(start_yr,end_yr+1):

        df_s3_data = get_cloud_data(i,user_choice,post_url)
        print(i,"Date Matching : ")

        try:

            if(df_s3_data[df_s3_data.created_time.dt.year != df_s3_data.year].shape[0]>0):
                print("There is a mismatch in the year")
                print(df_s3_data[df_s3_data.created_time.dt.year != df_s3_data.year]['post_id'])

            elif(df_s3_data[df_s3_data.created_time.dt.strftime('%b') != df_s3_data.month].shape[0]>0):
                print("There is a mismatch in the month")
                print(df_s3_data[df_s3_data.created_time.dt.month != df_s3_data.month]['post_id'])

            elif(df_s3_data[df_s3_data.created_time.dt.date != df_s3_data.date].shape[0]>0):
                print("There is a mismatch in the date")
                print(df_s3_data[df_s3_data.created_time.dt.date != df_s3_data.date]['post_id'])

            else:
                print("All Dates are Matching")
        
        except:
            print("No such Files Found")


def main():
    user_choice,post_url=read_config()
    extract_date_validate(user_choice,post_url)
    
if __name__ == "__main__":
    main()
