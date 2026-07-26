import pandas as pd

# Use raw string (r prefix) to handle backslashes correctly
file_path = r"D:\GAURAV\My Learnings\Grow Data Skills DE\Project 1\My Fintech project\Output\Gold\dim_accounts\part-00000-d71a995c-f00e-448a-bcf2-dd3e614f2a04-c000.snappy.parquet"

# Read the parquet file into a pandas DataFrame
df = pd.read_parquet(file_path)

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
# Display the first few rows of the DataFrame
print(df)