<h3> Data Engineering Assessment </h3>
<h4> The documents folder contains </h4><br/>
<ul>
<li>My assumptions for this project </li>
<li>My Approach on Scaling to Larger Datasets</li>
<li>Challenges working on a remote environment </li>
</ul>
<h4> The data_model folder contains </h4><br/>
<ul>
<li>Database Schema for the project </li>
</ul>
<h4> The jupyter_notebook_scripts folder contains </h4><br/>
<ul>
<li>Initial Data Analysis and Exploration notebook file </li>
</ul>
<h4> The apis folder contains </h4><br/>
<ul>
<li>Python based Rest API to provide an endpoint that returns the mean and median temperature </li>
</ul>
<h4> The etl_scripts folder contains </h4><br/>
<ul>
<li>Scripts to load dim and fact tables </li>
</ul>

<h4> Steps to run the project </h4>

<ul>
  <li>  <h5>Clone the project </li>
    <code> git clone https://github.com/nisheshk/Data-Engineering-Assessment.git </code> <br/> <br/>
  <li> <h5> To Run ETL Jobs that loads the dim and fact tables  </h5> </li>
    1. Create a virtual env using python. <br/><br/>
    2. Activate the virtual environment. In linux, <br/><br/>
      <code> source env_name/bin/activate </code><br/><br/>
    3. Go inside the etl_scripts folder. Install the packages from the requirements file using <br/><br/>
<code> pip install -r requirements.txt </code><br/><br/>
4. Run the scripts in order. <br/><br/>
<code> python load_dim_date.py  </code><br/><br/>
<code> python load_dim_location.py  </code><br/><br/>
<code> python load_dim_weather_station.py  </code><br/><br/>
<code> python load_fact_country_demography.py  </code><br/><br/>
<code> python load_fact_weather_city_daily.py  </code><br/><br/>
<code> python load_fact_weather_daily.py  </code><br/><br/>
<code> python load_mv_fact_weather_daily.py  </code><br/><br/>
5. If you have MySQL client installed, you can connect to the GCP CLoudSQL using the credentials below to look at the data:<br/><br/>
<code> host: 34.124.113.76  </code><br/>
<code> username: root  </code><br/>
<code> password: tealbook_assessment </code><br/><br/>
<li> <h5> I have used Django framework to create an API that provide an endpoint to calculate daily mean and median temperature. To run the API,   </h5> </li>
1. Create a new virtual environment and activate it.<br/>
2. Change the path to apis\weather_project\<br/>
3. Install the package using the requirements.txt file<br/><br/>
<code> pip install -r requirements.txt </code><br/><br/>
4. Run the project using<br/><br/>
<code> python manage.py runserver </code><br/><br/>
5. Send POST request to <br/><br/>
<code>http://127.0.0.1:8000/weather_station_reading/daily_reading/</code><br/>
<code>Request JSON Body Example: {"date":"2021-01-13"}</code><br/><br/>
It returns the mean and median temperature for that day.<br/><br/>
6. if you want to nagivate to the important files for this  API, <br/><br/>
<code>apis\weather_project\weather_station_readings\apis\views</code><br/>
<code>apis\weather_project\weather_station_readings\models.py</code><br/>
<code>apis\weather_project\weather_project\settings.py</code><br/>
<br/>
