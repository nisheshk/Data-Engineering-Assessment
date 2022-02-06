"""
Goal: This script loads the dim_date dimension.

We don't need to run this job everyday. We can provide the end_date(in the
config file) to be far in the future and run the job just one time. We know that
this job is not going to be incremental. The load technique currently is set as
'Truncate and Load' in the config file.

Created by: Nishesh Kalakheti
Created on: 5th Feb, 2022
"""

from    datetime import date, timedelta
from    utils.log import logging
from    utils.database_connection import *
import  configparser
import  os
import  pandas as pd




#Read config file parameters
try:
    logging.info('Reading config parameters')

    config = configparser.ConfigParser()
    config.read('config.ini')

    start_year = int(config['dim_date']['start_year'])
    end_year = int(config['dim_date']['end_year'])
    table_name = config['dim_date']['table_name']
    database_name = config['dim_date']['database_name']
    load_type = config['dim_date']['load_type']


    logging.info('Reading config parameters completed')

except Exception as e:
    logging.error('Error -> ' + str(e), exc_info=True)
    raise


def create_dim_date_df():
    """
    Goal: This function is used to create pandas dataframe that can be loaded to
            dim_date table.

    Return: df <type: pandas.core.frame.DataFrame>
                The pandas dataframe that contains dim_date data

    """
    try:
        logging.info('Function create_dim_date_df() running ')

        #Creates dictionary based on the columns of dim_date table
        dim_date = {'date_id_sk':  [], 'date':[], 'year':[], 'quarter':[], \
                    'month':[], 'day':[], 'month_name':[] ,'is_weekend':[],
                    'transformation_name':[], 'transformation_dt': []}
        #Creates datetime.date fields
        start_date = date(start_year, 1, 1)
        end_date = date(end_year, 12, 31)

        #Finds the difference in days between start and end date
        delta = end_date - start_date

        for i in range(delta.days + 1):

            #Gets next date
            date1 = start_date + timedelta(days=i)

            #Gets next date year,  month, quarter, day and month name
            year = date1.year
            month = date1.month

            if month <= 3:
                quarter = 1
            elif month <= 6:
                quarter = 2
            elif month <= 9:
                quarter = 3
            else:
                quarter = 4

            day = date1.day
            month_name = date1.strftime("%b")

            #Create surrogate key for our dim_date table.
            #If date = 2021/01/01 then date_id_sk = 20210101
            date_id_sk = (((year * 100 ) + month ) * 100 ) + day

            #Loads the data into the dictionary dim_date
            dim_date['date_id_sk'].append(date_id_sk)
            dim_date['date'].append(date1)
            dim_date['year'].append(year)
            dim_date['quarter'].append(quarter)
            dim_date['month'].append(month)
            dim_date['day'].append(day)
            dim_date['month_name'].append(month_name)

            #os.path.basename(__file__) gives the file name.
            dim_date['transformation_name'].append(os.path.basename(__file__))
            dim_date['transformation_dt'].append(pd.Timestamp.now())

            #Weekday 5 and 6 represents Sat and Sun repectively.
            if date1.weekday() == 5 or date1.weekday() == 6:
                dim_date['is_weekend'].append(1)
            else:
                dim_date['is_weekend'].append(0)

        #Creates pandas dataframe from the dictionary dim_date
        df_dim_date = pd.DataFrame(dim_date)

        return df_dim_date

    except Exception as e:
        logging.error('Error -> ' + str(e), exc_info=True)
        raise e



if __name__ == '__main__':
    df_dim_date = create_dim_date_df()

    #Load the df_dim_date dataframe into the database
    write_to_database( df_dim_date, table_name, database_name, \
                            load_type = load_type)
