"""
Goal: Provide API endpoint name for the weather_station_reading API
"""
from django.contrib import admin
from django.urls import path
from weather_station_readings.apis import views

urlpatterns = [
    path('daily_reading/', views.MvFactWeatherDailyView.as_view()),
]
