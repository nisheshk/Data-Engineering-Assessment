"""
Goal: This script loads the fact_country_demography table.
The cities.csv dataset had population field which is measured during census. As
this field is changeable, we could either treat it as a SCD type 1 or store the
history of the population in the fact table. Here I am storing the history in
the fact table.

I am assuming that we get the population stats file once at the end of every year.


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

    table_name = config['fact_country_demo']['table_name']
    database_name = config['fact_country_demo']['database_name']

    dim_location_table_name = config['fact_country_demo']['dim_location_table_name']
    dim_location_database_name = config['fact_country_demo']['dim_location_database_name']

    logging.info('Reading config parameters completed')

except Exception as e:
    logging.error('Error -> ' + str(e), exc_info=True)
    raise

if __name__ == '__main__':
    try:
        df_city = pd.read_csv('../source_data/cities.csv')

        #Gets the current year and adds the column to df
        current_year = date.today().year
        df_city['year'] = current_year

        #Get the dim_location_data
        query = f"SELECT location_id_sk as location_id, longitude as city_longitude,\
                latitude as city_latitude, province_code  FROM \
                {dim_location_database_name}.{dim_location_table_name};"
        df_dim_location = read_from_database(query)

        #Get the location_id based on lng and lat. We could also have used city to join the data.
        df_fact_cntry_demo = pd.merge(df_city, df_dim_location, right_on = \
                                ['city_longitude', 'city_latitude'],\
                                left_on = ['lng', 'lat'], how = 'left')

        #Get the date_id which is the last day of the last month of the current year.
        df_fact_cntry_demo['date_id'] = (((current_year * 100 ) + 12 ) * 100 ) + 31

        #Adds transformation name as the script name and current timestamp as the
        #transformation_dt
        df_fact_cntry_demo['transformation_name'] = os.path.basename(__file__)
        df_fact_cntry_demo['transformation_dt'] = pd.Timestamp.now()

        select_cols = ['location_id', 'date_id', 'population', 'population_proper',
                        'transformation_name', 'transformation_dt']

        write_to_database( df_fact_cntry_demo[select_cols], table_name,\
                            database_name, 'append')

    except Exception as e:
        logging.error('Error -> ' + str(e), exc_info=True)
        raise
