# import great_expectations as ge
import pandas as pd
import awswrangler as wr

# sales = pd.read_csv("C:/Users/Praveen/Downloads/Duplicate data - 6577f1d4-c07d-492e-ba23-c0c327f2e184.csv")
# print(sales)

# df = ge.read_csv("s3://srmg-gold-layer/dbt/tables/501444f9-05ef-4a40-834e-cc7ee58b18b3/20230320_120015_00071_k9c3p_18a1ec32-5155-4612-8571-247ef59fc8d6")
# print(df)
# uniqueness = df.expect_compound_columns_to_be_unique(column_list=["ga_users", "ga_exitRate", "_airbyte_ab_id"])
# print(uniqueness["success"])

df_s3_data_1 = wr.s3.read_parquet("s3://srmg-datalake-test/google_analytics_data/UA/source=hourly-pipeline/vertical=News/brand=Urdu_News/year=2023/0.parquet")
print(df_s3_data_1)
df_s3_data_1.to_csv("C:/Users/Praveen/Documents/GA_airbyte_test/ga_hourly_pipeline.csv", header=True)
# df_s3_data_2 = wr.s3.read_parquet("s3://srmg-gold-layer/dbt/tables/501444f9-05ef-4a40-834e-cc7ee58b18b3/20230320_120015_00071_k9c3p_43d88925-4c8c-4770-8559-6e394a813d7e")
# print(len(df_s3_data_2))
# df_s3_data_2.to_csv("C:/Users/Praveen/Documents/GA_airbyte_test/df_s3_data_2.csv", header=False)
# df_s3_data_3 = wr.s3.read_parquet("s3://srmg-gold-layer/dbt/tables/501444f9-05ef-4a40-834e-cc7ee58b18b3/20230320_120015_00071_k9c3p_a7008ca3-887f-4c17-b37d-1026cbed4fef")
# print(len(df_s3_data_3))
# df_s3_data_3.to_csv("C:/Users/Praveen/Documents/GA_airbyte_test/df_s3_data_3.csv", header=False)
# df_s3_data_4 = wr.s3.read_parquet("s3://srmg-gold-layer/dbt/tables/501444f9-05ef-4a40-834e-cc7ee58b18b3/20230320_120015_00071_k9c3p_c5ad5e95-d039-4923-832a-b0049fd45e0f")
# print(len(df_s3_data_4))
# df_s3_data_4.to_csv("C:/Users/Praveen/Documents/GA_airbyte_test/df_s3_data_4.csv", header=False)
# uniqueness = df_s3_data.expect_column_values_to_be_unique(column="_airbyte_ab_id")
# print(uniqueness["success"])
