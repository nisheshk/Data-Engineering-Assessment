"""
Goal: This script contains the mysq db connector and database actions that can
        be reused across the project.
Created by: Nishesh Kalakheti
Created on: 2nd Feb, 2022
"""

from sqlalchemy import create_engine
from log import logging
import pandas as pd


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
        try:
            #Database connection string to connect to database
            db_connection_str = 'mysql+pymysql://root:test@localhost/'\
                                'TEALBOOK_ASSESSMENT'

            # Creates the engine to connect to the MySQL database
            engine = create_engine(db_connection_str)
            conn = engine.connect()

            logging.info("Database connection success")

            #Executes the function that is passed to the outer function.
            rv = func(conn, *args,**kwargs)
        except Exception:
            logging.error("Database connection error" + str(e))
            raise
        finally:
            #Close the connection after the function is executed or during errors
            conn.close()
        return rv
    return with_connection_

@mysql_db_connector
def read_from_mysql( conn, sql_query):
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
def write_to_mysql( conn, df, table_name, database_name, if_exists):
    """
    Goal: This function is used to write data to MySQL database.
    Paramter: df <type: pandas.core.frame.DataFrame>
                The data that we wish to write to the database.

            table_name <type: string>
                Name of the table where we want to load the data.

            database_name <type: string>
                Database which contains table provided.

            if_exists <type: string>
                This is used to define what needs to be done if the table already exists
                in the database. Possible values are 'append', 'replace' and 'fail'


    Return: df <type: pandas.core.frame.DataFrame>
                The pandas dataframe that consists the data from the database

    """

    #Writes the dataframe to the database.
    df.to_sql(name = table_name, schema = database_name,  con = conn, if_exists = if_exists, method = 'multi', chunksize = 2000, index = False)
