#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Main script for processing content data.
Assumes that master data already exists on DB with the necessary keys
"""

import pandas as pd
# Requires optional dependency openpyxl
import os
import re
import sys
import atexit

from basic_parameters import excel_data_dir, output_dir
from basic_parameters import strict_fkeys, strict_pkeys
from database_tools import push_to_db, set_primary_key, set_foreign_key, enforce_not_null, enforce_dtypes, treat_as_text
from tools_data import remove_unnamed_cols, nums_to_ints, create_and_move_to_outdir, find_newest_path
from tools_data import remove_help_cols, find_empty_rows_in_csv, int_regex_pattern
from tables_and_columns import df_spec_content

# How to look for text_id in file name
text_id_regex_pattern = r'_(\d+)\.xlsx'

# Where the xlsx files are:
rootdir = find_newest_path(excel_data_dir) + 'Content data'

# Write csv data etc. here
outdir = output_dir + 'csv/content_data/'

print('Processing content data from', rootdir)

start_workdir = os.getcwd()
def exit_stuff():
    os.chdir(start_workdir)
atexit.register(exit_stuff)
create_and_move_to_outdir(outdir)
print('Writing to', outdir)

# Get Excel file names
dir_list = []
#iterates through all the folders and gathers file names
for subdir, dirs, files in os.walk(rootdir):
    for file in files:
        # if the file is excel, add it
        if (re.findall(r"\.xlsx", file) != []):# & (re.findall("_help\.xlsx", file) == []):
            dir_list.append(os.path.join(subdir, file))
        else: print("Will not process: " + os.path.join(subdir, file))

if dir_list == []:
    print('No content sheets found in', rootdir, '. Exiting.')
    sys.exit()
# File name should start with four digits, but three occur
# dir_list = [f for f in dir_list if re.findall('\d?\d\d\d', os.path.basename(f)) != []]


# Check whether a column is defined
def column_declared_content(table_name, c, report = False):
    if (table_name, c) in df_spec_content.index:
        out = True
    else:
        if (report) & (c != 'text_id'):
            print('Warning: Column', c, 'not specified for content table', table_name)
        out = False
    return out
def set_not_null_cols(table_name, df_spec=df_spec_content):
    df_tab = df_spec[df_spec.table_name==table_name]
    cols_list = list(df_tab.column_name[df_tab.null=='no'])
    enforce_not_null(table_name, cols_list)
    
table_list = set(df_spec_content.table_name)
    


# Gather data from Excel files in a dict
content_dict = {}
for file in dir_list:
    print('\nReading data from', file)
    numbers = re.findall(text_id_regex_pattern, str(file))
    try:
        text_id = numbers[-1]
    except IndexError:
        text_id = '0000'
        print('No text id in file name', file + '. Using default id 0000.')
    print (text_id + " processing")
    dfs = pd.read_excel(file, sheet_name=None)
    # Remove rows containing all empty data
    # See pandas guide for what is considered missing data
    dfs = {key: df.dropna(how = 'all') for key, df in dfs.items()}
    print('Done reading from', file)
    content_dict[file] = {'text_id': text_id, 'data': dfs}

"""
Result is content_dict of form:
{filename1: { text_id: str
              data: {table_name1: df
                     table_name2: df
                     }
              }
}
"""

# Catch problems with content table names
print('Checking validity of table names.')
name_lists = [it['data'].keys() for f, it in content_dict.items()]
names = [name for namelist in name_lists for name in namelist]
for table_name in names:
    for name in dfs.keys():
        if name not in table_list:
            print(name, 'is not a specified content table name.')

# Concatenate:
# Empty lists
print('Concatenating tables from each source.')
conc_data = {}
for table_name in names:
    conc_data[table_name] = []
# List of dfs for each table, insert text ids etc
for f, content in content_dict.items():
    text_id = content['text_id']
    dfs = content['data']
    # Remove rows containing all empty data
    # See pandas guide for what is considered missing data
    dfs = {key: df.dropna(how = 'all') for key, df in dfs.items()}
    for table_name, df in dfs.items():
        if len(df.columns) > 40:
            print('Warning: Table', table_name, "in", f, "has suspiciously many columns.")
            print(df.columns)
        df['text_id'] = int(text_id)
        conc_data[table_name].append(df)
# Concatenate dfs
for table_name, dfs in conc_data.items():
    conc_data[table_name] = pd.concat(dfs) 
        

# Loop over the tables and write to DB etc
tables_processed = []
for table_name, dftmp in conc_data.items():
    print('\nProcessing sheet named ', table_name)
    if table_name not in table_list:
        print('No table specified with name '+ table_name + '. Skipping sheet with this name.')
        continue
    dftmp = remove_unnamed_cols(dftmp)
    print('Removing help columns')
    df = remove_help_cols(dftmp, table_name, df_specification=df_spec_content)
    # Process column names. Make lower case:
    #df.columns = df.columns.str.lower()
    #df.columns = df.columns.str.replace('\W', '__', regex=True)

    # Data type handling in Pandas
    for c in df.columns:
        if column_declared_content(table_name, c, report=True):
            if (df_spec_content.data_type[(table_name, c)]=='INT')  |  (re.findall(int_regex_pattern, c) != []):
                df[c] = pd.to_numeric(df[c], errors='coerce')
                try:                  
                    df[c] = df[c].astype(pd.Int64Dtype())
                except TypeError:
                    print('Warning: Unable to convert', c, 'from numeric to integer')
            if df_spec_content.data_type[(table_name, c)] in treat_as_text:
                try:
                    df[c] = df[c].fillna('').astype(str)
                except TypeError:
                    df[c] = df[c].astype(str)

    # Convert all numeric types to integer
    df = nums_to_ints(df)

    # To DB
    print('Pushing to DB.')
    n = push_to_db(df, table_name)
    print("Created DB table", table_name, "with", n,  "rows (count reported by sqlalchemy).")

    # Data constraints
    enforce_dtypes(table_name, df_spec=df_spec_content)
    set_not_null_cols(table_name, df_spec=df_spec_content)

    # To csv
    csv_file_name = outdir+'{}.csv'.format(table_name)
    df.to_csv(csv_file_name, 
              index=False, encoding='utf-8')
    # Check for empty rows
    find_empty_rows_in_csv(csv_file_name)

    
    # Some logging
    f = open(outdir + "output.txt", "a")
    f.write(file + ";")
    f.write(table_name + ";")
    f.write(str(list(df)) + "\n")
    f.close()
    tables_processed.append(table_name)
print('The following content data tables were processed:')
print(tables_processed)

if strict_pkeys:
    print('\nSetting primary keys in content data')
    for table_name, c in df_spec_content.index:
        if column_declared_content(table_name, c):
            if df_spec_content.key[(table_name, c)] == 'primary':
                set_primary_key(table_name, c)
else:
    print('Warning: Not setting primary keys.')

if strict_fkeys:
    print('\nSetting foreign keys in content data')
    # Assuming primary keys in master data are already set
    for table_name in table_list:
        set_foreign_key(table_name, 'text_id')
    for table_name, c in df_spec_content.index:
        if column_declared_content(table_name, c):
            if df_spec_content.key[(table_name, c)] == 'foreign':
                set_foreign_key(table_name, c)
else:
    print('Warning: Not setting foreign keys.')

os.chdir(start_workdir)