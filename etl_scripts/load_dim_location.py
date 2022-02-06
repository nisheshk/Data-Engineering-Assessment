"""
Goal: This script loads the dim_location dimension.

Created by: Nishesh Kalakheti
Created on: 5th Feb, 2022
"""

import  configparser
import  os
import  pandas as pd
from    utils.database_connection import *
from    utils.log import logging

#Read config file parameters
try:
    logging.info('Reading config parameters')

    config = configparser.ConfigParser()
    config.read('config.ini')

    table_name = config['dim_location']['table_name']
    database_name = config['dim_location']['database_name']

    logging.info('Reading config parameters completed')

except Exception as e:
    logging.error('Error -> ' + str(e), exc_info=True)
    raise

if __name__ == '__main__':
    try:
        df_city = pd.read_csv('../source_data/cities.csv')

        #Renames columns with smthng more descriptive.
        df_city = df_city.rename(columns={'admin': 'province', 'lat': 'latitude', \
                    'lng': 'longitude'})

        #Drop the duplicates if any
        df_city = df_city[['city', 'latitude', 'longitude', 'country', 'iso2', \
                    'province', 'capital']].drop_duplicates()

        #Create a mapping for the province and its code
        canada_province = {'province': ['Newfoundland and Labrador', 'Prince Edward Island',\
                                        'Nova Scotia', 'New Brunswick', 'Québec', 'Ontario',\
                                        'Manitoba' , 'Saskatchewan' , 'Alberta', 'British Columbia',\
                                         'Yukon', 'Northwest Territories' , 'Nunavut'],
                           'province_code': ['NL', 'PE', 'NS', 'NB', 'QC', 'ON', 'MB', \
                                                'SK', 'AB', 'BC', 'YT', 'NT', 'NU']}

        #Conver the canada_province dictionary into a dataframe
        df_province_code = pd.DataFrame(canada_province)

        #Join df_city with df_province_code dictionary to get the province code.
        df_city = pd.merge(df_city, df_province_code, how = 'left', on = 'province')

        df_city['transformation_name'] = os.path.basename(__file__)
        df_city['transformation_dt'] = pd.Timestamp.now()

        #Load the data into database using truncate and load
        write_to_database( df_city, table_name, database_name, 'truncate_and_load')

    except Exception as e:
        logging.error('Error -> ' + str(e), exc_info=True)
        raise
