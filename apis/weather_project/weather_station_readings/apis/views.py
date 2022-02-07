"""
Goal: This view contains the API logic for daily weather station/

Created by: Nishesh Kalakheti
Created on: 6th Feb, 2022
"""

from    django.http import JsonResponse, HttpResponse
from    django.views import View
from    weather_station_readings.models import MvFactWeatherDaily
from    django.views.decorators.csrf import csrf_exempt
from    django.utils.decorators import method_decorator
from    django.core import serializers
import  datetime
import  json

#When using postman I had to execmpt the CSRF token as the connection was not HTTPS
@method_decorator(csrf_exempt, name='dispatch')
class MvFactWeatherDailyView(View):
    def post_req_body_validator(self, req_body):
        """
        Goal: This function is used to validate if the user provided paramters are
                valid.

        Parameter: body <type: dict>
                    This is the request body

        Return: 1/0 (type: int)
            Returns 1 if input is valid; Else 0

            An example of valid request body:  {"date":"2021-01-21"}


        """

        #Check if 'date' field exists
        if 'date' in req_body:

            #Return true if the input is 'YYYY-MM-DD'.
            format = "%Y-%m-%d"
            try:
                datetime.datetime.strptime(req_body['date'], format)
                return 1
            except ValueError:
                return 0


    def post(self, request, *args, **kwargs):

        """
        Goal: This is a post request what returns the mean, median, min and max
                temperature for a specific day.

        """

        try:
            #Converts byte string to utf-8
            body_unicode = request.body.decode('utf-8')
            req_body = json.loads(body_unicode)

            #Checks if the input is valid
            if self.post_req_body_validator(req_body):
                res_obj = MvFactWeatherDaily.objects.filter(date = req_body['date'])
                if res_obj:

                    data = list(res_obj.values())

                    #Return data in json with status code 200.
                    return JsonResponse(data, safe=False,  status=200)
            else:
                return JsonResponse({'Error': 'invalid Input'}, status=400)
        except Exception as e:

            #Retrun error if any.
            return JsonResponse({'Error': e}, status = 500)
