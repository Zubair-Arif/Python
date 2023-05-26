import awswrangler as wr
import pandas as pd

df_s3_data = pd.DataFrame()
df_s3_data = wr.s3.read_parquet(path="s3://srmg-datalake/marketing_data/snapchat/source=adaccounts_stats_daily/0.parquet")
for i in range(len(df_s3_data)):
    print(df_s3_data["total_reach"][i])

