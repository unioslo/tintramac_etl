# -*- coding: utf-8 -*-
"""
Functionality for processing TInTraMac data
"""
import re
import os
import glob
import pandas as pd

# Column names fitting pattern always transformed to integer
int_regex_pattern = r'_id$'

def create_dir_if_nec(dir):
    if not os.path.exists(dir):
        os.makedirs(dir)

def create_and_move_to_outdir(dir):
    if not os.path.exists(dir):
        os.makedirs(dir)
    os.chdir(dir)

def find_newest_path(rootdir):
    # Return the newest subfolder based on date in folder name
    date_pattern = r'^\d\d\d\d\.\d\d.\d\d$'
    start_workdir = os.getcwd()
    os.chdir(rootdir)
    all_names = glob.glob('*')
    dated_folders = [name for name in all_names if re.findall(date_pattern, name) != []]
    if dated_folders == []:
        out = rootdir + '/'
    else:
        out = rootdir +'/' + sorted(dated_folders)[-1] + '/'
    os.chdir(start_workdir)
    return out

"""
def conv_to_text(df, table_name, df_spec):
    df_tab = df_spec[df_spec.table_name==table_name]
    df_tab = df_tab[df_tab.help_column != 'yes']
    df_tab = df_tab[df_tab.data_type == treat_as_text]
    df_copy = df.copy()
    for col in df_tab.column_name:
        df_copy[col] = df_copy[col].fillna('').astype(str)
    return df_copy
"""

# Strip whitespace
def strip_white(df):
    df.columns = [n.strip() for n in df.columns]
    # Iterate over all columns and strip whitespace from string columns
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].str.strip()
    return df

def nums_to_ints(df):
    """
    Numeric types should be integer
    Exceptions can  hardcoded here
    """
    dfnew=df.copy(deep=True)
    hardcoded_exceptions = []
    for c in df:
        if c in hardcoded_exceptions:
            pass
        elif pd.api.types.is_numeric_dtype(df[c]):
            try:
                dfnew[c] = df[c].astype(pd.Int64Dtype(), errors='raise')
            except TypeError:
                print('Unable to convert', c, 'from numeric to integer.')
    return dfnew

def times2strings(df):
    # Change all datetime types to strings. 
    # In place!!!
    # 'NaT' --> ''
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            print('Resetting column', col, 'from type', df[col].dtype, 'to text.')
            df[col] = df[col].astype(str)
            df[col] = df[col].replace('NaT', '')
    return df

def remove_unnamed_cols(df):
    patt = r'^Unnamed: \d+$'   
    unnameds = [c for c in df.columns if re.findall(patt, c) != []]
    return df.drop(unnameds, axis=1, inplace= False)

def remove_help_cols(df, table_name, df_specification, ignorable = ('text_id')):
    for col in df.columns:
        try:
            if (df_specification.help_column[(table_name, col)]=='yes'):
                df = df.drop([col], axis=1, inplace=False)
        except KeyError:
            if col not in ignorable:
                pass
                #print('Warning: Column', col, 'not specified for table', table_name)
    return df

def find_empty_rows_in_csv(filename):
    # So far only for monitoring, it might also find empty lines within quotes
    with open(filename, 'r', errors='ignore') as f:
        csvdata = f.readlines()
    newdata = []
    for line in csvdata:
        if (re.findall(r'^[\s,]+$', line)!=[]) & (re.findall(r',', line) != []):
            print('Empty line')
        else:
            newdata.append(line)
    #with open(filename, 'w') as f:
    #    f.writelines(newdata)

# def hardcode_colnames(df):
#     hcs = {'Coptic': '_c_optic'}
#     return df.rename(columns=hcs)
    
            
