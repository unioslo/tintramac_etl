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
from basic_parameters import database_name

conn = psycopg2.connect(
   database = database_name, 
   host = "127.0.0.1", 
   port = "5432"
)
if conn.status == psycopg2.extensions.STATUS_READY:
    print(f'Python connected to database {database_name} successfully.')
else:
    print(f'Python failed to connect to database {database_name}.')

engine = create_engine('postgresql://localhost:5432/' + database_name)

def close_db():
    if conn is not None:
        conn.close()
        print("Python database connection closed.")
        
atexit.register(close_db)

def run_sql(sql_command):
    cursor = conn.cursor()
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

"""
def apply_dtypes(table_name, df_spec):
    df = df_spec[df_spec.table_name==table_name]
    changes = {row.column_name: row.data_type for index, row in df.iterrows()}
    recoding = {'LONGTEXT': 'text'}
    for col, dtype in changes.items():
        if dtype in recoding.keys():
            changes[col] = recoding[dtype]
    for col_name, new_type in changes.items():
        with engine.connect() as connection:
            query = text(f"ALTER TABLE {table_name} ALTER COLUMN {col_name} TYPE {new_type}")
            connection.execute(query)
"""

def set_primary_key(table_name, column_name):
    #print('Setting primary key', column_name, 'in', table_name)
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
    parent_table = re.sub('_id', '', c)
    #print('Foreign key', c, 'in table', table_name)
    if ping_table(parent_table):
        if ping_column(parent_table, c):
            __set_foreign_key(table_name, c, parent_table)
        else:
            print(f'No parent column {c} in {parent_table} for foreign key {c}')
    else:
        print(f'No parent table {parent_table} found for foreign key {c}')

def enforce_not_null(table_name, columns):
    #try:
        with engine.connect() as conn:
            for column in columns:
                stmt = text(f"ALTER TABLE {table_name} ALTER COLUMN {column} SET NOT NULL")
                try:
                    conn.execute(stmt)  # Use the connection to execute
                    conn.commit()  # Commit the changes
                    # print(f'NOT NULL constraint enforced on column {column}.')
                except exc.SQLAlchemyError as e:
                    print(f'Database error, unable to "set not null" for column', column)
                    # print(f'Database error: {str(e)}')


def ping_table(dt):
    # Create a cursor object
    cur = conn.cursor()
    # Your SQL query to get the 'col' column from the 'tab' table
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
