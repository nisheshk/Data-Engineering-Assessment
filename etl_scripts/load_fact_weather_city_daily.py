"""
Goal: This script loads the daily tempeerature of Canadian cities in the table
fact_weather_city_daily

Created by: Nishesh Kalakheti
Created on: 5th Feb, 2022
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

    dim_wthr_stn_table_name = config['fact_weather_city_daily']\
                                    ['dim_wthr_stn_table_name']
    dim_wthr_stn_database_name = config['fact_weather_city_daily']\
                                    ['dim_wthr_stn_database_name']

    dim_date_table_name = config['fact_weather_city_daily']\
                                    ['dim_date_table_name']
    dim_date_database_name = config['fact_weather_city_daily']\
                                    ['dim_date_database_name']

    database_name = config['fact_weather_city_daily']['database_name']
    table_name = config['fact_weather_city_daily']['table_name']

    logging.info('Reading config parameters completed')

except Exception as e:
    logging.error('Error -> ' + str(e), exc_info=True)
    raise

if __name__ == '__main__':

    try:

        #Read the climate dataset
        df_climate = pd.read_csv('../source_data/climate.csv')

        #Rename the columns with lower case
        df_climate_columns = df_climate.columns
        climate_columns_replace = {i:i.lower() for i in df_climate_columns}
        df_climate = df_climate.rename(columns=climate_columns_replace)

        #Read the data from dim_weather_station and dim_date
        dim_weather_stn_query = f"SELECT station_id_sk as station_id, station_id_bk\
                        FROM {dim_wthr_stn_database_name}.{dim_wthr_stn_table_name};"
        dim_date_query = f"SELECT date_id_sk as date_id, date   FROM \
                {dim_date_database_name}.{dim_date_table_name};"

        df_dim_weather_stn = read_from_database(dim_weather_stn_query)
        df_dim_date = read_from_database(dim_date_query)

        #Join the climate df with business key(station_id_bk) to get the surrogate
        #key which is station_id_sk
        df_fact_wthr_cty_dy = pd.merge(df_climate, df_dim_weather_stn, how = 'left',\
                    right_on = ['station_id_bk'], left_on = ['climate_identifier'])

        #Join with dim_date table to get the date_id
        df_fact_wthr_cty_dy['date'] = pd.to_datetime(df_fact_wthr_cty_dy['local_date']).dt.date
        df_fact_wthr_cty_dy = pd.merge(df_fact_wthr_cty_dy, df_dim_date, on = ['date'],\
                                how = 'left')

        #Adds transformation name as the script name and current timestamp as the
        #transformation_dt
        df_fact_wthr_cty_dy['transformation_name'] = os.path.basename(__file__)
        df_fact_wthr_cty_dy['transformation_dt'] = pd.Timestamp.now()

        select_cols = ['station_id', 'date_id', 'mean_temperature', 'mean_temperature_flag',\
                'min_temperature', 'min_temperature_flag', 'max_temperature',
                'max_temperature_flag','total_precipitation', 'total_precipitation_flag',
                'total_rain','total_rain_flag', 'total_snow', 'total_snow_flag',
                'snow_on_ground','snow_on_ground_flag', 'direction_max_gust',
                'direction_max_gust_flag','speed_max_gust', 'speed_max_gust_flag',
                'cooling_degree_days','cooling_degree_days_flag', 'heating_degree_days',
               'heating_degree_days_flag', 'min_rel_humidity', 'min_rel_humidity_flag',
               'max_rel_humidity', 'max_rel_humidity_flag', 'transformation_name', \
               'transformation_dt']

        #Write the data into the database
        write_to_database( df_fact_wthr_cty_dy[select_cols], table_name,\
                            database_name, 'truncate_and_load')

    except Exception as e:
        logging.error('Error -> ' + str(e), exc_info=True)
        raise
