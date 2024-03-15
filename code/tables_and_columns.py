import pandas as pd
# Requires optional dependency openpyxl
import os
from basic_parameters import excel_data_dir, output_dir
from database_tools import push_to_db

#gather files in master_sheets

# Where the xlsx files are:
#rootdir = excel_data_dir

# Write csv data here
csv_outdir = output_dir + 'csv/'

treat_as_text = ('VARCHAR', 'LONGTEXT', 'DATE')

start_workdir = os.getcwd()
os.chdir(csv_outdir)
print('Writing metadata to', csv_outdir)

dfs = pd.read_excel(excel_data_dir + 'tables_and_columns.xlsx', sheet_name=None)

# Metadata
df_spec_master = dfs['metadata']

df_spec_master.to_csv(csv_outdir + 'tables_and_column_master.csv', index=False)
print('Pushing metadata to DB')
push_to_db(df_spec_master, table_name='tables_and_columns_master')

# Create new column that is a tuple of 'table_name' and 'column_name'
df_spec_master['fullcolname'] = list(zip(df_spec_master['table_name'], df_spec_master['column_name']))


# Now set the new tuple column as index
df_spec_master.set_index('fullcolname', inplace=True)


# Content data
df_spec_content = dfs['content_data']
df_spec_content['fullcolname'] = list(zip(df_spec_content['table_name'], df_spec_content['column_name']))
df_spec_content.set_index('fullcolname', inplace=True)
df_spec_master.to_csv(csv_outdir + 'tables_and_columns_content.csv', index=False)
push_to_db(df_spec_master, table_name='tables_and_columns_content')


os.chdir(start_workdir)