# Fetch data from NordPool

from __future__ import unicode_literals
import datetime
import json
import requests
import pytz
from datetime import date, timedelta
from dateutil.parser import parse as parse_dt
from pytz import timezone
    

from email.message import EmailMessage


#Converting time from CET to RIX
def convert_time(time):
    new_time = parse_dt(time)
    local_tz = pytz.timezone('Europe/Riga')
    if new_time.tzinfo is None:
        new_time = timezone('Europe/Amsterdam').localize(new_time).astimezone(local_tz)
    new_time = new_time.strftime("%d-%b-%Y %H:%M") #%d-%b-%Y (%H:%M)
    return new_time

#Changing comma to dot (float values)
def convert_to_float(value):
    try:
        return float(value.replace(',', '.').replace(" ", ""))
    except ValueError:
        return float('inf')

def parse_nord_pool_data(country_code: str):
    print("Parse Nord-Pool data")
    
    with open("./price_to_compare.txt", 'r') as file:
        compare_to = float(file.read())

    API_URL = 'http://www.nordpoolspot.com/api/marketdata/page/10' #Hourly

    end_date = date.today() + timedelta(days=1)
    parameters = {"currency": "EUR", "endDate": end_date}
    additional_data = {"Max", "Min", "Average",}

    data = requests.get(API_URL, params=parameters).json()
    data = data['data']

    parsed_data = {}
    open("./new_schedules.txt", 'w').close()
    last_update_time_np = convert_time(data['DateUpdated'])    #The last time when data was updated on Nord-Pool website
    with open("./victron_update_time.txt", 'r') as file:
        last_update_time_victron = file.read()

    if last_update_time_np == last_update_time_victron:
        print("Victron schedules are up to date")
        return None
    
    with open("./victron_update_time.txt", 'w+') as file:
        file.write(last_update_time_np)

    start_schedule_datetime = {"start_weekday": -1, "start_time": -1}
    end_schedule_datetime = {"end_weekday": -1, "end_time": -1}

    schedules_to_file = {"Schedules" : []}

    for row in data['Rows']:
                for column in row['Columns']:
                    
                    name = column['Name']
                    #Skip values not for LV
                    if name != country_code:
                        continue

                    if name not in parsed_data:
                        parsed_data[name] = { 'Values': []}
                    
                    if row['IsExtraRow']:
                        if row['Name'] in additional_data:
                            parsed_data[name][row['Name']] = convert_to_float(column['Value'])
                    else:
                        if (convert_to_float(column['Value']) <= compare_to):
                            parsed_data[name]['Values'].append({
                                'Start_Datetime': convert_time(row['StartTime']),
                                'End_Datetime': convert_time(row['EndTime']),
                                'Price': convert_to_float(column['Value']),
                            })
                            
                            # Calculate start & end datetime for setting to Victron scheduler
                            start_weekday = datetime.datetime.strptime(convert_time(row['StartTime']), "%d-%b-%Y %H:%M").weekday()
                            start_time = str(datetime.datetime.strptime(convert_time(row['StartTime']), "%d-%b-%Y %H:%M").time().hour * 3600)
                            if start_weekday == 6:
                                start_weekday = 0
                            else:
                                start_weekday = start_weekday + 1
                            if start_schedule_datetime['start_weekday'] == -1 and start_schedule_datetime['start_time'] == -1:
                                start_schedule_datetime['start_weekday'] = start_weekday
                                start_schedule_datetime['start_time'] = start_time

                            end_weekday = datetime.datetime.strptime(convert_time(row['EndTime']), "%d-%b-%Y %H:%M").weekday()
                            end_time = str(datetime.datetime.strptime(convert_time(row['EndTime']), "%d-%b-%Y %H:%M").time().hour * 3600)
                            if end_weekday == 6:
                                end_weekday = 0
                            else:
                                end_weekday = end_weekday + 1
                            if end_schedule_datetime['end_weekday'] == -1 and end_schedule_datetime['end_time'] == -1:
                                end_schedule_datetime['end_weekday'] = end_weekday
                                end_schedule_datetime['end_time'] = end_time
                                continue

                            if end_schedule_datetime["end_weekday"] == start_weekday and end_schedule_datetime["end_time"] == start_time:
                                end_schedule_datetime['end_time'] = end_time
                                continue
                            else:
                                if start_schedule_datetime['start_weekday'] != -1 and end_schedule_datetime['end_weekday'] != -1:
                                    schedules_to_file['Schedules'].append({"StartWeekday": start_schedule_datetime['start_weekday'], "StartTime": start_schedule_datetime['start_time'], "EndWeekday": end_schedule_datetime['end_weekday'], "EndTime": end_schedule_datetime['end_time']})

                                start_schedule_datetime['start_weekday'] = start_weekday
                                start_schedule_datetime['start_time'] = start_time
                                end_schedule_datetime['end_weekday'] = end_weekday
                                end_schedule_datetime['end_time'] = end_time
    if {"StartWeekday": start_schedule_datetime['start_weekday'], "StartTime": start_schedule_datetime['start_time'], "EndWeekday": end_schedule_datetime['end_weekday'], "EndTime": end_schedule_datetime['end_time']} not in schedules_to_file['Schedules']:
            schedules_to_file['Schedules'].append({"StartWeekday": start_schedule_datetime['start_weekday'], "StartTime": start_schedule_datetime['start_time'], "EndWeekday": end_schedule_datetime['end_weekday'], "EndTime": end_schedule_datetime['end_time']})
            with open("./new_schedules.txt", 'a+') as file:
                file.write(json.dumps(schedules_to_file))
    if len(parsed_data[country_code]['Values']) != 0:
        parsed_data[country_code]['NordPool_Update_Time'] = last_update_time_np
        parsed_data[country_code]['Compare_To'] = compare_to
        return parsed_data
    
    return None