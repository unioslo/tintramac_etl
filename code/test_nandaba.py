import psycopg2
from getpass import getpass
from sqlalchemy import create_engine, text, exc

database_name = "tf_apocrypha_utv"

# Set `database_user = None` to use default username
database_user = "tf_apocrypha_utv_user"

hostname = "dbpg-uio-utv02.uio.no"
host=hostname

pw = getpass(prompt="Enter your database password:")

conn = psycopg2.connect(
   database = database_name, 
   user = database_user,
   host = host, 
   port = "5432",
   password = pw
)