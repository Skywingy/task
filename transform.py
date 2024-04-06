import pandas as pd

# Mounting dataset to pandas
df = pd.read_csv('datasets/original/SampleDataset.csv')

# Data Quality Check

# Missing_values
missing_values = df.isnull().sum()

# Completely blank
rows_missing_all_columns = df[df.isnull().all(axis=1)]

# Count how many rows missing all values
missing_all_column_values = len(rows_missing_all_columns)

# Only some values missing
rows_missing_some_columns = df[df.isnull().any(axis=1) & ~df.isnull().all(axis=1)]

# Count how many rows missing some values
missing_some_column_values = len(rows_missing_some_columns)

# Count how many duplicated rows
duplicates = df.duplicated().sum()


data_types = df.dtypes

# Removes all rows that columns completely blank - cleaning process
df = df.dropna(how='all')

# Transforming datatypes
df['Date'] = pd.to_datetime(df['Date'])

df['Value'] = df['Value'].astype(float)
df['Factor'] = df['Factor'].astype(int)

df.reset_index(drop=True, inplace=True)


# Add a new column representing the index number in the first column
df.insert(0, 'Index', df.index)


# Extracting duplicated rows after indexing in order to report with row number

duplicated_mask = df.duplicated(subset=df.columns.difference(['Index']))

# Filter the DataFrame to include only duplicated rows
duplicated_rows = df[duplicated_mask]

duplicated_rows.to_csv('datasets/performed/Duplicated.csv', index=False)

# Saving cleaned dataset to cleaned.csv
df_cleaned = df[~duplicated_mask]

df_cleaned.to_csv('datasets/performed/Cleaned.csv', index=False)

print("Success!")