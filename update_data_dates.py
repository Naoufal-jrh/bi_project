import pandas as pd

# Read the original Excel file
data = pd.read_excel("./data/global_superstore.xlsx", sheet_name="Orders")



# Make the data more recent
data["Order Date"] = (data["Order Date"] + pd.DateOffset(years=9) - pd.DateOffset(months=2)).dt.date
data["Ship Date"] = (data["Ship Date"] + pd.DateOffset(years=9) - pd.DateOffset(months=2)).dt.date

print(data.head().to_dict())

# # Select the first 100 rows
# sample_data = data.head(100)

# # Save the sample data to a new Excel file
# sample_data.to_excel("./airflow/dags/data/test_sales_data.xlsx", sheet_name="Orders", index=False)

# print("Sample data saved successfully!")





# import pandas as pd
# from snowflake.connector.pandas_tools import write_pandas
# import snowflake.connector

# # extract the historical data from excel
# data = pd.read_excel("./data/global_superstore.xlsx", sheet_name="Orders")

# # make the data recent by updating the dates in the DataFrame
# data["Order Date"] = (data["Order Date"] + pd.DateOffset(years=9) - pd.DateOffset(months=2)).dt.date
# data["Ship Date"] = (data["Ship Date"] + pd.DateOffset(years=9) - pd.DateOffset(months=2)).dt.date


# # save the data to a new excel file
# data.to_excel("./data/global_superstore_updated.xlsx", sheet_name="Orders", index=False)

# print("Data updated successfully!")