"""
Goal: Contains the model for the mv_fact_weather_daily table. As this was already
        there is the database before creating the model, I used the feature of
        django to import the model from the database using (python manage.py inspectdb)

        In django models, there needs to be a primary key. So, I have added
        primary key in the date field, although it's not really defined as a
        primary key in the materialized view.
"""

from django.db import models

class MvFactWeatherDaily(models.Model):
    date = models.DateField(primary_key=True)
    mean_daily_temp = models.DecimalField(max_digits=5, decimal_places=2, blank=True, null=True)
    median_daily_temp = models.DecimalField(max_digits=5, decimal_places=2, blank=True, null=True)
    min_daily_temp = models.DecimalField(max_digits=5, decimal_places=2, blank=True, null=True)
    max_daily_temp = models.DecimalField(max_digits=5, decimal_places=2, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'mv_fact_weather_daily'
