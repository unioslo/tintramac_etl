#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 21 16:28:05 2024

@author: knutwa
"""

import re
import atexit
from getpass import getpass
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine, text, exc
from basic_parameters import database_name, database_user, hostname, ask_for_password
from basic_parameters import strict_dtypes, strict_fkeys, strict_nulls, strict_pkeys

dtype_mapping = {'LONGTEXT': 'text', 
                'VARCHAR': 'text',
                'DATE': 'text',
                'INT': 'integer'}
treat_as_text = [key for key, val in dtype_mapping.items() if val=='text']

if hostname:
    if database_user:
        print()
        db_passw = getpass(prompt="Enter your database password:")
    else:
        db_passw = None
else:
    if ask_for_password:
        print()
        db_passw = getpass(prompt="Enter your database password:")
    else:
        db_passw = None

# Connect to local Postgres DB
if hostname:
    host = hostname
else:
    host = "127.0.0.1"
conn = psycopg2.connect(
   database = database_name, 
   user = database_user,
   password = db_passw,
   host = host, 
   port = "5432"
)
if conn.status == psycopg2.extensions.STATUS_READY:
    print(f'Python connected to database {database_name} successfully.')
else:
    print(f'Python failed to connect to database {database_name}.')

# Also connect with SQLalchemy
if hostname:
    if database_user:
        engine = create_engine('postgresql://' + database_user  + ':' + db_passw + '@' + hostname + ':5432/' + database_name)
    else:
        engine = create_engine('postgresql://' + hostname + ':5432/' + database_name)
else:
    if database_user:
        engine = create_engine('postgresql://' + database_user + '@localhost:5432/' + database_name)
    else:
        engine = create_engine('postgresql://localhost:5432/' + database_name)

# Ensure database connection closure
def close_db():
    if conn is not None:
        conn.close()
        print("Python database connection closed.")
atexit.register(close_db)

# Run arbitrary SQL command
def run_sql(sql_command, params=None):
    with conn.cursor() as cursor:
        #cursor = conn.cursor()
        try:
            cursor.execute(sql_command, params)
            conn.commit()
        except psycopg2.DatabaseError as error:
            print(f"Database error: {error}")
            conn.rollback()  # Roll back the transaction
        finally:
            cursor.close()

# pd.Dataframe to DB
def push_to_db(df, table_name):#, if_exists='replace'):
    # Use DROP ... CASCADE to drop tables with foreign keys pointing to `table_name`
    sql_drop_query = f"DROP TABLE IF EXISTS {table_name} CASCADE;"
    sql_drop_query = sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(sql.Identifier(table_name))
    run_sql(sql_drop_query)
    n = df.to_sql(table_name, engine, index=False, if_exists='replace')
    return n

def enforce_dtypes(table_name, df_spec, verbose=True):
    if strict_dtypes:
        if verbose:
            print("Enforcing data types for", table_name)
        # Specify data types in existing table
        df = df_spec[df_spec.table_name==table_name]
        df = df[df.help_column!='yes']
        changes = {row.column_name: row.data_type for index, row in df.iterrows()}
        # Find Postgres type, first filter out any unlisted types
        changes = {col: dtype for col, dtype in changes.items() if dtype in dtype_mapping.keys()}
        for col, dtype in changes.items():
            if dtype in dtype_mapping.keys():
                changes[col] = dtype_mapping[dtype]
        # Perform SQL
        for col_name, new_type in changes.items():
            alter_command = sql.SQL(
                "ALTER TABLE {} ALTER COLUMN {} SET DATA TYPE {} USING {}::{}"
            ).format(
                sql.Identifier(table_name),
                sql.Identifier(col_name),
                sql.SQL(new_type),
                sql.Identifier(col_name),
                sql.SQL(new_type)
            )
            run_sql(alter_command)
            #run_sql(f"ALTER TABLE {table_name} ALTER COLUMN {col_name} SET DATA TYPE {new_type} USING {col_name}::{new_type}")
            # # Using sqlalchemy here sometimes does not execute properly:
            # with engine.connect() as connection:
            #     try:
            #         query = text(f"ALTER TABLE {table_name} ALTER COLUMN {col_name} SET DATA TYPE {new_type} USING {col_name}::{new_type}")    
            #         connection.execute(query)
            #         print(query)
            #     except exc.SQLAlchemyError as e:
            #         connection.rollback()
            #         print(f'Warning: database error. Can not enforce data type for {table_name}:{col_name}. Column not in data?')

def set_primary_key(table_name, column_name, column_name2=None, strict_pkeys=True):
    if strict_pkeys:
        if column_name2:
            alter_command = sql.SQL(
                "ALTER TABLE {} ADD PRIMARY KEY ({} ,{})"
            ).format(
                sql.Identifier(table_name),
                sql.Identifier(column_name),
                sql.Identifier(column_name2)
            )
        else:
            alter_command = sql.SQL(
                "ALTER TABLE {} ADD PRIMARY KEY ({})"
            ).format(
                sql.Identifier(table_name),
                sql.Identifier(column_name)
            )
        run_sql(alter_command)

def __set_foreign_key(foreign_table, column_name, parent_table, constraint_name=None):
    # If you don't provide a specific constraint name, the system will generate one.
    fk_constraint_name = sql.Identifier(constraint_name if constraint_name else f"{foreign_table}_{column_name}_fkey")
    fk_command = sql.SQL("""
        ALTER TABLE {foreign_table}
        ADD CONSTRAINT {constraint_name}
        FOREIGN KEY ({column_name})
        REFERENCES {parent_table} ({parent_column});
        """).format(
        foreign_table=sql.Identifier(foreign_table),
        constraint_name=fk_constraint_name,
        column_name=sql.Identifier(column_name),
        parent_table=sql.Identifier(parent_table),
        parent_column=sql.Identifier(column_name)
    )
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

def enforce_not_null(table_name, columns, strict_nulls=strict_nulls):
    if strict_nulls:
        print("Enforcing non-null property")
        with conn.cursor() as cursor:
            for column in columns:
                # Create the ALTER TABLE command using psycopg2's SQL composition utilities
                stmt = sql.SQL("ALTER TABLE {} ALTER COLUMN {} SET NOT NULL").format(
                    sql.Identifier(table_name),
                    sql.Identifier(column)
                )
                try:
                    cursor.execute(stmt)  # Use the cursor to execute
                    conn.commit()  # Commit the changes
                    # Uncomment the following line to get a confirmation for each column
                    # print(f'NOT NULL constraint enforced on column {column}.')
                except psycopg2.DatabaseError as e:
                    print(f'Database error, unable to "set not null" for column {column} in {table_name}')
                    conn.rollback()
                    # Uncomment the following line to see the detailed error
                    # print(f'Database error: {str(e)}')

# Check if table exists in DB
def ping_table(dt):
    # Create a cursor object
    cur = conn.cursor()
    # Your SQL query to get the 'tab' table
    sql_query = sql.SQL('SELECT * FROM {}').format(sql.Identifier(dt))
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

# Check if column exists in DB
def ping_column(dt, col):
    # Create a cursor object
    cur = conn.cursor()
    # Your SQL query to get the 'col' column from the 'tab' table
    sql_query = sql.SQL('SELECT {} FROM {}').format(sql.Identifier(col), sql.Identifier(dt))
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
