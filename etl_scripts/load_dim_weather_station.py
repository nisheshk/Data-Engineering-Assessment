"""
Goal: This script loads the dim_weather_station dimension.

Created by: Nishesh Kalakheti
Created on: 5th Feb, 2022
"""

import  configparser
import  numpy as np
import  os
import  pandas as pd
from    datetime import datetime
from    utils.database_connection import *
from    utils.log import logging

#Read config file parameters
try:
    logging.info('Reading config parameters')

    config = configparser.ConfigParser()
    config.read('config.ini')

    dim_location_table_name = config['dim_weather_station']['dim_location_table_name']
    dim_locatin_database_name = config['dim_weather_station']['dim_location_database_name']

    target_table_name = config['dim_weather_station']['table_name']
    target_database_name = config['dim_weather_station']['database_name']

    logging.info('Reading config parameters completed')

except Exception as e:
    logging.error('Error -> ' + str(e), exc_info=True)
    raise

def find_station_city(df_climate):
    """
    Goal: This function is used to find the closest city from the station

    Return: df_climate <type: pandas.core.frame.DataFrame>
                The pandas dataframe that contains climate data

    """

    #Gets data from the dim_location table
    query = f"""

    SELECT location_id_sk as location_id, longitude as city_longitude,
    latitude as city_latitude, province_code  FROM {dim_locatin_database_name}.\
    {dim_location_table_name}; """
    dim_location = read_from_database(query)

    #Joins the climate df with the dim_location table on province
    #After the join operation, we are going to have all the cities of that province
    #associated with the climate station of that province.
    df_climate = pd.merge(df_climate, dim_location, how = 'left', on = ['province_code'])

    #Out of all the cities in that province, calc the average station to city distance.
    #For this we subtract city's abs longitude with station abs longitude and city's
    #abs latitude with the station abs laitude and then average the abs difference.
    df_climate['avg_station_to_city_proximity'] = \
            (abs(abs(df_climate['longitude']) - abs(df_climate['city_longitude'])) +
            abs(abs(df_climate['latitude']) - abs(df_climate['city_latitude']))) / 2


    #Groups by 'longitude', 'latitude', 'station_name', 'station_id_bk', 'province_code'
    #THis means a station will be now associated with just one city that's closest to it.
    #I am assuming here that we are going to have just one value of
    #'avg_station_to_city_proximity'

    #Using transform may not be the fastest because it is non-vectorized function.
    #But I didn't wanna spend much time trying to figure out the alternative.
    df_climate = df_climate[df_climate['avg_station_to_city_proximity'] == \
            df_climate.groupby(['longitude', 'latitude', 'station_name', 'station_id_bk',
           'province_code'])['avg_station_to_city_proximity'].transform(min)]

    return df_climate

if __name__ == "__main__":
    try:

        #Read only the required fields from the dataset.
        cols = ['lng', 'lat', 'STATION_NAME', 'CLIMATE_IDENTIFIER',
               'PROVINCE_CODE']
        df_climate = pd.read_csv('../source_data/climate.csv', usecols = cols)

        #Creates unique combination of the cols mentioned above.
        df_climate = df_climate.drop_duplicates()


        #Renames the column names.
        df_climate.rename(columns={'STATION_NAME': 'station_name', \
                                    'CLIMATE_IDENTIFIER': 'station_id_bk', \
                                    'PROVINCE_CODE': 'province_code',\
                                    'lng': 'longitude', 'lat': 'latitude'},inplace = True)

        #Finds the city for a station. This is how I related two datasets.
        df_climate = find_station_city(df_climate)

        #Gets todays date
        todays_date = datetime.today().strftime('%Y-%m-%d')

        #I am assuming that the some of the attributes of this table can change over
        #time. So, I have used SCD (Slowly Changing Dimension) type 2 technique.
        #For now I have set the valid_from to have the default value of 2021-01-01
        #However, when the attribute changes for a station(Eg: A weather station moved
        #to a new place), then we replace the valid_to date with the day before when
        #the change took place. Next, we add new record into the dimension table
        #with valid_from as the date when the attributes was changed, valid_to field
        #set to be null. Also set the  latest_record_flag to 'Y' for the new record,
        #while changing this field value to 'N' for the previous record.

        df_climate['valid_from'] = '2021-01-01'
        df_climate['valid_to'] = np.nan
        df_climate['latest_record_flag'] = 'Y'

        #Adds transformation name as the script name and current timestamp as the
        #transformation_dt
        df_climate['transformation_name'] = os.path.basename(__file__)
        df_climate['transformation_dt'] = pd.Timestamp.now()

        df_climate = df_climate.rename(columns={'location_id': 'station_location_id'})
        select_columns = ['station_id_bk', 'station_name',  'station_location_id', \
                        'longitude', 'latitude', 'valid_from', 'valid_to', \
                        'latest_record_flag', 'transformation_name', 'transformation_dt']

        #Load the data into the database.
        write_to_database( df_climate[select_columns], target_table_name,\
                            target_database_name, 'truncate_and_load')

    except Exception as e:
        logging.error('Error -> ' + str(e), exc_info=True)
        raise
