#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os

# Source data folder, i.e. location of Excel files
excel_data_dir = '/Users/knutwa/OneDrive/Case Study Testing Data/'

# csv ouput etc goes here:
output_dir = '/Users/knutwa/OneDrive/tintramac_etl/data/case_study'

# The name of the database (the name that PostgreSQL recognises)
database_name = "tintramac_test_data"

# Set `database_user = None` to use default username
database_user = None

# Set to True if your database requires a password
ask_for_password = False

# Options to switch off constraints on database. Keep True unless you know what you're doing.
strict_pkeys = False # Set primary keys
strict_fkeys = True # Set foreign keys. Only runs when strict_pkeys is True
strict_nulls = False # Enforce non-null property where applicable
strict_dtypes = False # Enforce data types

# You can declare alternative paths like this:
# if not os.path.exists(excel_data_dir):
#    excel_data_dir = 'Data entry template/'

# Connect to an external database by setting hostname to the host URL
# When hostname is set, a username and password is always expected
hostname = None
# hostname = "dbpg-uio-prod02.uio.no"