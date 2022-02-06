"""
Goal: This script loads the daily weather stats that Canadian people experience.

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

    table_name = config['fact_weather_daily']['table_name']
    database_name = config['fact_weather_daily']['database_name']

    fact_wthr_city_daily_table_name = config['fact_weather_daily']\
                                ['fact_wthr_city_daily_table_name']
    fact_wthr_city_daily_database_name = config['fact_weather_daily']\
                                ['fact_wthr_city_daily_database_name']

    logging.info('Reading config parameters completed')

except Exception as e:
    logging.error('Error -> ' + str(e), exc_info=True)
    raise

try:

    #Reads data from fact_weather_city_daily
    fact_wthr_city_daily_query = f"SELECT date_id, mean_temperature  FROM \
                                    {fact_wthr_city_daily_database_name}.\
                                    {fact_wthr_city_daily_table_name}"
    df_fact_wthr_city_daily = read_from_database(fact_wthr_city_daily_query)

    #Groups the data by date_id and the aggregation operation is mean, median,
    #min and max
    df_fact_wthr_daily = df_fact_wthr_city_daily.groupby('date_id')\
                                .agg({'mean_temperature':['mean', 'median', \
                                'min', 'max']}).reset_index().round(2)

    #Rename the columns
    df_fact_wthr_daily.columns = ['date_id', 'mean_daily_temp', \
                        'median_daily_temp', 'min_daily_temp', 'max_daily_temp']

    #Adds transformation name as the script name and current timestamp as the
    #transformation_dt
    df_fact_wthr_daily['transformation_name'] = os.path.basename(__file__)
    df_fact_wthr_daily['transformation_dt'] = pd.Timestamp.now()

    #Write the data into the database.
    write_to_database( df_fact_wthr_daily, table_name, database_name, \
                    'truncate_and_load')


except Exception as e:
    print (e)
