"""
Goal: This script contains the mysq db connector and database actions that can
        be reused across the project.
Created by: Nishesh Kalakheti
Created on: 2nd Feb, 2022
"""

from    sqlalchemy import create_engine
from    .log import logger
import  configparser
import  pandas as pd

try:
    logger.info('Reading config parameters')

    config = configparser.ConfigParser()
    config.read('config.ini')

    username = config['mysql']['username']
    password = config['mysql']['password']
    host = config['mysql']['host']


    logger.info('Reading config parameters completed')

except Exception as e:
    logger.error('Error -> ' + str(e), exc_info=True)
    raise

def mysql_db_connector(func):
    """
        Goal: This function is used to create MySQL database connection and
        perform read and write operations.

        Paramter: func <type: function>
            The outer function mysql_db_connector takes function as a parameter

        Return: with_connection_ <type: function>
    """
    def with_connection_(*args,**kwargs):
        """
            The with_connection_ function can access the func from the outer
            function and execute those function(which are usually read and write
            operations).

        """
        conn = None
        try:

            #Database connection string to connect to database
            db_connection_str = f'mysql+pymysql://{username}:{password}@{host}/'\
                                'TEALBOOK_ASSESSMENT'

            # Creates the engine to connect to the MySQL database
            engine = create_engine(db_connection_str)
            conn = engine.connect()

            logger.info("Database connection success")

            #Executes the function that is passed to the outer function.
            rv = func(conn, *args,**kwargs)
        except Exception as e:
            logger.error("Database connection error" + str(e))
            raise
        finally:
            #Close the connection after the function is executed or during errors
            if conn:
                conn.close()
        return rv
    return with_connection_

@mysql_db_connector
def read_from_database( conn, sql_query):
    """
    Goal: This function is used to read data from MySQL database.
    Paramter: sql_query <type: string>
                The sql_query is executed against the database to read the data from.

    Return: df <type: pandas.core.frame.DataFrame>
                The pandas dataframe that consists the data from the database
    """

    #Execte sql query against the database
    df = pd.read_sql(sql_query, conn);
    return df

@mysql_db_connector
def write_to_database( conn, df, table_name, database_name, load_type, chunksize = 1000):
    """
    Goal: This function is used to write data to MySQL database.
    Paramter: df <type: pandas.core.frame.DataFrame>
                The data that we wish to write to the database.

            table_name <type: string>
                Name of the table where we want to load the data.

            database_name <type: string>
                Database which contains table provided.

            load_type <type: string>
                This determines how we want to load the data into the table.
                Possible values are append, replace, fail and truncate_and_load

            chunksize <type: int>
                How many records do we want to insert at a time


    Return: df <type: pandas.core.frame.DataFrame>
                The pandas dataframe that consists the data from the database

    """

    #Pandas has 'Replace', 'Append' and 'Fail' technique to load the data into
    #database. Using replace is going to change the data type of the fields.
    #Since we don't want the fields datatype to change, we truncate the table
    #and load the data using 'append' technique.
    if load_type == 'truncate_and_load':

        #Truncate and load for dim tables can be dangerous in case we have system
        #generated primary keys.

        conn.execute(f'SET FOREIGN_KEY_CHECKS = 0');
        conn.execute(f'TRUNCATE TABLE {database_name}.{table_name};')
        load_type = 'append'

    #Writes the dataframe to the database.
    df.to_sql(name = table_name, schema = database_name,  con = conn, \
                if_exists = load_type, method = 'multi', chunksize = chunksize, index = False)
    conn.execute(f'SET FOREIGN_KEY_CHECKS = 1');


@mysql_db_connector
def run_commands_on_database(conn, sql_query):
    """
    Goal: This function is used to run SQL commands on the database
    Paramter: sql_query <type: string>
                The sql_query is executed against the database

    """
    #cursor = conn.cursor()
    conn.execute(sql_query, multi = True)
