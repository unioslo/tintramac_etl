#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
# Requires optional dependency openpyxl
import os
import re
import sys
import atexit


from basic_parameters import excel_data_dir, output_dir
from database_tools import push_to_db, set_primary_key, set_foreign_key, enforce_not_null
from tools_data import remove_unnamed_cols, nums_to_ints, move_to_outdir, find_newest_path
from tools_data import remove_help_cols, int_regex_pattern, find_empty_rows_in_csv
from tables_and_columns import df_spec_master, treat_as_text

#gather files in master_sheets

# Where the xlsx files are:
rootdir = find_newest_path(excel_data_dir) + 'Master Data'
if not os.path.exists(rootdir):
    rootdir = find_newest_path(excel_data_dir) + 'Metadata'


# Write csv data here
outdir = output_dir + 'csv/master_data/'


print('Processing master data from', rootdir)
start_workdir = os.getcwd()
def exit_stuff():
    os.chdir(start_workdir)
atexit.register(exit_stuff)
move_to_outdir(outdir)
print('Writing to', outdir)

# Get excel file names
dir_list = []
#iterates through all the folders and gathers file names
for subdir, dirs, files in os.walk(rootdir):
    for file in files:
        # if the file is excel, add it
        if (re.findall(r"\.xlsx", file) != []):
            dir_list.append(os.path.join(subdir, file))
        else: print("Did not process: " + os.path.join(subdir, file))

if dir_list == []:
    print('No master sheets found in', rootdir, '. Exiting.')
    sys.exit()
    
def column_declared(table_name, c, report = False):
    if (table_name, c) in df_spec_master.index:
        out = True
    else:
        print('Warning: Column', c, 'not specified for '+ table_name +'.')
        out = False
    return out

def set_not_null_cols(table_name, df_spec=df_spec_master):
    df_tab = df_spec[df_spec.table_name==table_name]
    cols_list = list(df_tab.column_name[df_tab.null=='no'])
    enforce_not_null(table_name, cols_list)

table_list = set(df_spec_master.table_name)

# Create empty tables, may be overwritten with actual data
for table_name in table_list:
    columns = df_spec_master.column_name[df_spec_master.table_name==table_name]
    df = pd.DataFrame(columns=columns)
    push_to_db(df, table_name)


tables_processed = []
# take the xlsx file and push data from each sheet in the workbook
for file in dir_list:
    print('\n*****Processing file', file)
    dfs = pd.read_excel(file, sheet_name=None)
    # Remove rows containing all empty data
    # See pandas guide for what is considered missing data
    dfs = {key: df.dropna(how = 'all') for key, df in dfs.items()}
    
    for table_name in dfs.keys():
        print('\nProcessing sheet named', table_name)
        if re.search(r'_help\s*$', table_name):
            continue
        if table_name not in table_list:
            print('No table specified with name '+ table_name + '. Skipping sheet with this name.')
            continue
        
        dftmp = dfs[table_name]
        dftmp = remove_unnamed_cols(dftmp)
        df = remove_help_cols(dftmp, table_name, df_spec_master, ignorable=[])
        # Process column names. Make lower case:
        #df.columns = df.columns.str.lower()
        #df.columns = df.columns.str.replace('\W', '__', regex=True)
        if len(df.columns) > 40:
            print('Table', table_name, "in", file, "has suspiciously many columns.")
            print(df.columns)

        # Data types etc
        for c in df.columns:
            if column_declared(table_name, c, report=True):
                if (df_spec_master.data_type[(table_name, c)]=='INT') | (re.findall(int_regex_pattern, c) != []):
                    df[c] = pd.to_numeric(df[c], errors='coerce')
                    try:                  
                        df[c] = df[c].astype(pd.Int64Dtype())
                    except TypeError:
                        print('Warning: Unable to convert', c, 'from numeric to integer')
                if df_spec_master.data_type[(table_name, c)] in treat_as_text:
                    df[c] = df[c].astype(str)

        # Convert all numeric types to integer
        df = nums_to_ints(df)


        # To DB
        print('Pushing table to DB')
        n = push_to_db(df, table_name)
        print("Created DB table", table_name, "with", n,  "rows (count reported by sqlalchemy).")

        print('Enforcing non-null property')
        set_not_null_cols(table_name, df_spec=df_spec_master)
            
        # To csv
        csv_file_name = outdir+'{}.csv'.format(table_name)
        df.to_csv(csv_file_name, 
                  index=False, encoding='utf-8')
        # Check for empty rows
        find_empty_rows_in_csv(csv_file_name)


        # Some logging
        f = open(outdir + "output.txt", "a")
        f.write(file + "; ")
        f.write(str(n) + '; ')
        f.write(table_name + "; ")
        f.write(str(df.dtypes).replace('\n', ', ').replace(' '*5, ' ') + "\n")
        f.close()
        tables_processed.append(table_name)

print('\nThe following master data tables were found and processed:')
print(sorted(tables_processed))

print('Setting primary keys in master data')
for table_name, c in df_spec_master.index:
    if column_declared(table_name, c ):
        if df_spec_master.key[(table_name, c)] == 'primary':
            set_primary_key(table_name, c)

print('Setting foreign keys in master data')
for table_name, c in df_spec_master.index:
    if column_declared(table_name, c ):
        if df_spec_master.key[(table_name, c)] == 'foreign':
            set_foreign_key(table_name, c)


    

