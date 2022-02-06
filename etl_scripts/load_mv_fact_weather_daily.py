"""
Goal: This script creates  materialized view for fact_weather_city table. The
        purpose of creating materialized view is to join the foreign key with
        the dim tables to get more descriptive attributes.

        In this case we jusut have date as a dimension. So we join with date_id
        to get the date field.

Created by: Nishesh Kalakheti
Created on: 6th Feb, 2022
"""

import  configparser
import  os
import  pandas as pd
from    utils.database_connection import *
from    utils.log import logging
from    datetime import date

#Read config file parameters
try:
    logging.info('Reading config parameters')

    config = configparser.ConfigParser()
    config.read('config.ini')

    table_name = config['mv_fact_weather_daily']['table_name']
    database_name = config['mv_fact_weather_daily']['database_name']

    fact_wthr_daily_database_name = config['mv_fact_weather_daily']\
                                ['fact_wthr_daily_database_name']
    fact_wthr_daily_table_name = config['mv_fact_weather_daily']\
                                ['fact_wthr_daily_table_name']

    dim_date_table_name = config['mv_fact_weather_daily']\
                                        ['dim_date_table_name']
    dim_date_database_name = config['mv_fact_weather_daily']\
                                        ['dim_date_database_name']

    logging.info('Reading config parameters completed')

except Exception as e:
    logging.error('Error -> ' + str(e), exc_info=True)
    raise

if __name__ == '__main__':
    try:

        #Reads data from fact_weather_daily table
        fact_wthr_daily_query = f"SELECT *  FROM \
                                        {fact_wthr_daily_database_name}.\
                                        {fact_wthr_daily_table_name}"
        df_mv_fact_wthr_daily = read_from_database(fact_wthr_daily_query)


        #Reads data from dim_date
        dim_date_query = f"SELECT date_id_sk, date   FROM \
                        {dim_date_database_name}.{dim_date_table_name};"
        df_dim_date = read_from_database(dim_date_query)

        #Get date for the corresponding date_id
        df_mv_fact_wthr_daily = pd.merge(df_mv_fact_wthr_daily, df_dim_date, \
                                    left_on = ['date_id'],\
                                    right_on = ['date_id_sk'],
                                    how = 'left')
        #Adds transformation name as the script name and current timestamp as the
        #transformation_dt
        df_mv_fact_wthr_daily['transformation_name'] = os.path.basename(__file__)
        df_mv_fact_wthr_daily['transformation_dt'] = pd.Timestamp.now()


        select_cols = ['date', 'mean_daily_temp', 'median_daily_temp', 'min_daily_temp',
           'max_daily_temp', 'transformation_name', 'transformation_dt']

        #df_mv_fact_wthr_daily[select_cols].head()

        #Write the data into the database
        write_to_database( df_mv_fact_wthr_daily[select_cols], table_name,\
                        database_name, 'truncate_and_load')

    except Exception as e:
        logging.error('Error -> ' + str(e), exc_info=True)
        raise
