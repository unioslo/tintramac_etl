# tintramac_etl

## Installation and first test

You need Conda and PostgreSQL. We have tested with version 4.9.2 of Conda and version 14 of PostgreSQL.

### Installation of Python code

First change working directory to the directory in which you want to install the package, e.g.
```
cd /Users/knutwa/OneDrive
```
Replace `/Users/knutwa/OneDrive` with the desired path on your machine. 

Clone the code from github:
```
git clone https://github.com/unioslo/tintramac_etl.git
```
Create a conda virtual environment that contains the dependencies:
```
cd tintramac_etl/code
conda create -f environment.yml
```

### Setting up a database

Start PostgreSQL and create a database. The database should not be password protected. On a Mac, with PostgreSQL installed by brew:
```
brew services start postgresql
createdb tintramac_test
```
This creates an empty database named `tintramac_test`. 


### Configure the code

In `basic_parameters.py`, change the `database_name` to variable to the one we created above. The line should now say:
```
database_name = "tintramac_test"
```
Change `excel_data_dir` to the path containg the Excel data files, and `output_dir` to where you want .csv-files and reports to be stored. 

If `output_dir` does not already exist, you must create it.

### First test

Activate the conda environment that you created:
```
conda activate tintramac_etl_env
```
Change working directory to where the codes are e.g.
```
cd /Users/knutwa/OneDrive/tintramac_etl/code
```

Process the master data, then the content data with
```
python proc_master_data.py
python proc_content_data.py
```
The order matters. The content data processing will assume that the master tables are already properly installed.

The python code reports errors to screen. These point to potential problems with the data. Read them carefully, make changes to the data, and try rerunning.

When rerunning the code, the database tables are deleted and rewritten from scratch. However, in some cases old tables may remain.
