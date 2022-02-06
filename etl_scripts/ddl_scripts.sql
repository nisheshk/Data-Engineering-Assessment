/*
Goal: This sql script contains the ddl scripts that can be used to create
      database and tables

Created by: Nishesh Kalakheti
Created on: 6th Feb, 2022
*/

CREATE DATABASE IF NOT EXISTS TEALBOOK_ASSESSMENT;

DROP TABLE IF EXISTS TEALBOOK_ASSESSMENT.dim_location;
CREATE TABLE  TEALBOOK_ASSESSMENT.dim_location(
	location_id_sk INT AUTO_INCREMENT PRIMARY KEY,
	country VARCHAR(70),
	province VARCHAR(70),
	iso2 CHAR(2),
	province_code CHAR(2),
	city VARCHAR(70),
	capital VARCHAR(10),
	longitude DECIMAL(11,8),
	latitude DECIMAL(10,8),
	transformation_name VARCHAR(100),
	transformation_dt TIMESTAMP
);



DROP TABLE IF EXISTS TEALBOOK_ASSESSMENT.dim_weather_station;
CREATE TABLE  TEALBOOK_ASSESSMENT.dim_weather_station(
	station_id_sk INT AUTO_INCREMENT PRIMARY KEY,
	station_id_bk VARCHAR(15),
	station_name VARCHAR(150),
	station_location_id INT,
	longitude DECIMAL(11,8),
	latitude DECIMAL(10,8),
	valid_from DATE,
	valid_to DATE,
	latest_record_flag CHAR(1),
	transformation_name VARCHAR(100),
	transformation_dt TIMESTAMP,
	FOREIGN KEY (station_location_id) REFERENCES TEALBOOK_ASSESSMENT.dim_location(location_id_sk)
);




DROP TABLE IF EXISTS TEALBOOK_ASSESSMENT.dim_date;
CREATE TABLE  TEALBOOK_ASSESSMENT.dim_date(
	date_id_sk INT PRIMARY KEY,
	date DATE,
	year INT,
	quarter INT,
	month INT,
	day INT,
	month_name CHAR(3),
	is_weekend BOOLEAN,
	transformation_name VARCHAR(100),
	transformation_dt TIMESTAMP
);


DROP TABLE IF EXISTS TEALBOOK_ASSESSMENT.fact_country_demography;
CREATE TABLE  TEALBOOK_ASSESSMENT.fact_country_demography(
	location_id INT,
	date_id INT,
	population INT,
	population_proper INT,
	transformation_name VARCHAR(100),
	transformation_dt TIMESTAMP,
	FOREIGN KEY (location_id) REFERENCES TEALBOOK_ASSESSMENT.dim_location(location_id_sk),
	FOREIGN KEY (date_id) REFERENCES TEALBOOK_ASSESSMENT.dim_date(date_id_sk)


);




DROP TABLE IF EXISTS TEALBOOK_ASSESSMENT.fact_weather_city_daily;
CREATE TABLE  TEALBOOK_ASSESSMENT.fact_weather_city_daily(
	station_id INT,
	date_id INT,
	mean_temperature DECIMAL(5,2),
	mean_temperature_flag VARCHAR(5),
	min_temperature DECIMAL(4,1),
	min_temperature_flag VARCHAR(5),
	max_temperature DECIMAL(5,2),
	max_temperature_flag VARCHAR(5),
	total_precipitation DECIMAL(5,2),
	total_precipitation_flag VARCHAR(5),
	total_rain DECIMAL(5,2),
	total_rain_flag VARCHAR(5),
	total_snow DECIMAL(5,2),
	total_snow_flag VARCHAR(5),
	snow_on_ground DECIMAL(5,2),
	snow_on_ground_flag VARCHAR(5),
	direction_max_gust DECIMAL(5,2),
	direction_max_gust_flag VARCHAR(5),
	speed_max_gust DECIMAL(5,2),
	speed_max_gust_flag VARCHAR(5),
	cooling_degree_days DECIMAL(5,2),
	cooling_degree_days_flag VARCHAR(5),
	heating_degree_days DECIMAL(5,2),
	heating_degree_days_flag VARCHAR(5),
	min_rel_humidity DECIMAL(5,2),
	min_rel_humidity_flag VARCHAR(5),
	max_rel_humidity DECIMAL(5,2),
	max_rel_humidity_flag VARCHAR(5),
	transformation_name VARCHAR(100),
	transformation_dt TIMESTAMP,
	FOREIGN KEY (station_id) REFERENCES TEALBOOK_ASSESSMENT.dim_weather_station(station_id_sk)
);



DROP TABLE IF EXISTS TEALBOOK_ASSESSMENT.fact_weather_daily;
CREATE TABLE  TEALBOOK_ASSESSMENT.fact_weather_daily(
	date_id INT,
	mean_daily_temp DECIMAL(5,2),
	median_daily_temp DECIMAL(5,2),
	min_daily_temp DECIMAL(5,2),
	max_daily_temp DECIMAL(5,2),
	transformation_name VARCHAR(100),
	transformation_dt TIMESTAMP,
	FOREIGN KEY (date_id) REFERENCES TEALBOOK_ASSESSMENT.dim_date(date_id_sk)
);

DROP TABLE IF EXISTS TEALBOOK_ASSESSMENT.mv_fact_weather_daily;
CREATE TABLE  TEALBOOK_ASSESSMENT.mv_fact_weather_daily(
	date DATE,
	mean_daily_temp DECIMAL(5,2),
	median_daily_temp DECIMAL(5,2),
	min_daily_temp DECIMAL(5,2),
	max_daily_temp DECIMAL(5,2),
	transformation_name VARCHAR(100),
	transformation_dt TIMESTAMP
);
