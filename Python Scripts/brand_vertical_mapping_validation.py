import pandas as pd
import json
import configparser as cp
import calendar
import boto3
s3 = boto3.client('s3')


def read_gen_config():
    config = cp.ConfigParser()
    config.read('config.ini')
    brand_vertical_mapping = config['Default']['brand_vertical_mapping']
    return brand_vertical_mapping



def main():
    brand_vertical_mapping=read_gen_config()
    df_brand_vertical = pd.DataFrame(json.loads(brand_vertical_mapping))
    #print(df_brand_vertical.to_string())
    df_brand_vertical_check = df_brand_vertical.groupby(df_brand_vertical.brand)['brand'].count().reset_index(name='count_sec_target')
    df_mismatched = df_brand_vertical_check.loc[(df_brand_vertical_check['count_sec_target']>1)]
    print(df_mismatched)



    return 1


if __name__=="__main__":
    main()