#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 21 16:28:05 2024

@author: knutwa
"""

import re
import atexit
import psycopg2
from sqlalchemy import create_engine, text, exc
from basic_parameters import database_name, database_user
from basic_parameters import strict_dtypes, strict_fkeys, strict_nulls, strict_pkeys


conn = psycopg2.connect(
   database = database_name, 
   user = database_user,
   host = "127.0.0.1", 
   port = "5432"
)

if conn.status == psycopg2.extensions.STATUS_READY:
    print(f'Python connected to database {database_name} successfully.')
else:
    print(f'Python failed to connect to database {database_name}.')

if database_user:
    engine = create_engine('postgresql://' + database_user + '@localhost:5432/' + database_name)
else:
    engine = create_engine('postgresql://localhost:5432/' + database_name)

def close_db():
    if conn is not None:
        conn.close()
        print("Python database connection closed.")

atexit.register(close_db)

def run_sql(sql_command):
    with conn.cursor() as cursor:
        #cursor = conn.cursor()
        try:
            cursor.execute(sql_command)
            conn.commit()
        except psycopg2.DatabaseError as error:
            print(f"Database error: {error}")
            conn.rollback()  # Roll back the transaction
        finally:
            cursor.close()

def push_to_db(df, table_name):#, if_exists='replace'):
    # Use DROP ... CASCADE to drop tables with foreign keys pointing to `table_name`
    sql_drop_query = f"DROP TABLE IF EXISTS {table_name} CASCADE;"
    run_sql(sql_drop_query)
    n = df.to_sql(table_name, engine, index=False, if_exists='replace')
    return n

dtype_mapping = {'LONGTEXT': 'text', 
            'VARCHAR': 'text',
            'DATE': 'text',
            'INT': 'integer'}
def enforce_dtypes(table_name, df_spec):
    if strict_dtypes:
        print("Enforcing data types")
        # Specify data types in existing table
        df = df_spec[df_spec.table_name==table_name]
        changes = {row.column_name: row.data_type for index, row in df.iterrows()}
        # Find Postgres type, drop if not found
        for col, dtype in changes.items():
            if dtype in dtype_mapping.keys():
                changes[col] = dtype_mapping[dtype]
            else:
                changes.pop(col)
        # Perform SQL
        for col_name, new_type in changes.items():
            with engine.connect() as connection:
                try:
                    query = text(f"ALTER TABLE {table_name} ALTER COLUMN {col_name} SET DATA TYPE {new_type} USING {col_name}::{new_type}")    
                    connection.execute(query)
                except exc.SQLAlchemyError as e:
                        print(f'Warning: database error when enforcing data type for {col_name}. Column not in data?')

def set_primary_key(table_name, column_name):
    if strict_pkeys:
        alter_command = f"ALTER TABLE {table_name} ADD PRIMARY KEY ({column_name})"
        run_sql(alter_command)
    
def __set_foreign_key(foreign_table, column_name, parent_table, constraint_name=None):
    # If you don't provide a specific constraint name, the system will generate one.
    fk_constraint_name = constraint_name if constraint_name else f"{foreign_table}_{column_name}_fkey"

    fk_command = f"""
    ALTER TABLE {foreign_table}
    ADD CONSTRAINT {fk_constraint_name}
    FOREIGN KEY ({column_name})
    REFERENCES {parent_table} ({column_name});
    """
    run_sql(fk_command)

# Setting foreign keys:
def set_foreign_key(table_name, c):
    if strict_fkeys & strict_pkeys:
        parent_table = re.sub('_id', '', c)
        if ping_table(parent_table):
            if ping_column(parent_table, c):
                __set_foreign_key(table_name, c, parent_table)
            else:
                print(f'No parent column {c} in {parent_table} for foreign key {c}')
        else:
            print(f'No parent table {parent_table} found for foreign key {c}')

def enforce_not_null(table_name, columns):
    if strict_nulls:
        print("Enforcing non-null property")
        with engine.connect() as enconn:
            for column in columns:
                stmt = text(f"ALTER TABLE {table_name} ALTER COLUMN {column} SET NOT NULL")
                try:
                    enconn.execute(stmt)  # Use the connection to execute
                    enconn.commit()  # Commit the changes
                    # print(f'NOT NULL constraint enforced on column {column}.')
                except exc.SQLAlchemyError as e:
                    print(f'Database error, unable to "set not null" for column {column} in {table_name}')
                    enconn.rollback() 
                    # print(f'Database error: {str(e)}')


def ping_table(dt):
    # Create a cursor object
    cur = conn.cursor()
    # Your SQL query to get the 'tab' table
    sql_query = f'SELECT * FROM {dt};'
    # Execute the query
    try:
        cur.execute(sql_query)
        conn.commit()
        out = True
    except psycopg2.DatabaseError:
        conn.rollback()
        out = False
    # Close the cursor and the connection
    cur.close()
    return out

def ping_column(dt, col):
    # Create a cursor object
    cur = conn.cursor()
    # Your SQL query to get the 'col' column from the 'tab' table
    sql_query = f'SELECT {col} FROM {dt};'
    # Execute the query
    try:
        cur.execute(sql_query)
        conn.commit()
        out = True
    except psycopg2.DatabaseError:
        conn.rollback()  # Roll back the transaction
        out = False
    # Close the cursor and the connection
    cur.close()
    return out
