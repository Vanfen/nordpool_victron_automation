import datetime
import json
import os
import ssl
from time import sleep
import paho.mqtt.client as mqtt
from dotenv import load_dotenv
from victron_schedules import Schedule

load_dotenv()

username = os.getenv('UNAME')
password = os.getenv('PASSWORD')
portal_id = os.getenv('PORTAL_ID')

flag_connected = 0
schedules = []

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client: mqtt.Client, userdata, flags, rc):
    global flag_connected
    print("Connected with result code "+str(rc))
    flag_connected = 1
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("$SYS/#")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    val = json.loads(msg.payload)
    print(msg.topic+" -> "+str(val))

def on_disconnect(client: mqtt.Client, userdata, rc):
    global flag_connected
    print("Disconnected with code " + str(rc))
    client.loop_stop()
    flag_connected = 0

def fill_schedules():
    global flag_connected

    print("Fill schedules")
    with open("./new_schedules.txt", "r") as file:
        new_schedules = file.readline()
    
    if len(new_schedules) == 0:
        print("No new schedules found.")
        return None
    
    client = mqtt.Client("FillSchedules")
    client.tls_set(cert_reqs=ssl.CERT_NONE)
    client.tls_insecure_set(True)
    client.username_pw_set(username=username, password=password)
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    client.connect("mqtt87.victronenergy.com", port=8883)

    client.loop_start()
    
    # Send ping to wake up Remote Console
    client.publish("R/%s/system/0/Serial" % portal_id)
    while 1:
        if flag_connected == 0:
            with open("./available_schedules.txt", "r") as file:
                schedule_ids = file.readlines()
            available_schedules = []
            for id in schedule_ids:
                available_schedules.append(id.strip())
            schedule_counter = 0
            schedules = json.loads(new_schedules)['Schedules']
            print(schedules)
            leftover = schedules[:]
            for schedule in schedules:
                while str(schedule_counter) not in available_schedules and schedule_counter < 6:
                    schedule_counter += 1
                if schedule_counter > 5:
                    break
                if int(schedule['EndWeekday']) - int(schedule['StartWeekday']) == 0:
                    duration = int(schedule['EndTime']) - int(schedule['StartTime'])
                else:
                    duration = 24 * 3600 - int(schedule['StartTime']) + int(schedule['EndTime'])
                day_published = client.publish("W/%s/settings/0/Settings/CGwacs/BatteryLife/Schedule/Charge/%s/Day" % (portal_id, schedule_counter), json.dumps({"value": schedule['StartWeekday']}))
                day_published.wait_for_publish()
                print("Day published = " + str(day_published.is_published()))
                duration_published = client.publish("W/%s/settings/0/Settings/CGwacs/BatteryLife/Schedule/Charge/%s/Duration" % (portal_id, schedule_counter), json.dumps({"value": duration}))
                duration_published.wait_for_publish()
                print("Duration published = " + str(duration_published.is_published()))
                start_published = client.publish("W/%s/settings/0/Settings/CGwacs/BatteryLife/Schedule/Charge/%s/Start" % (portal_id, schedule_counter), json.dumps({"value": schedule['StartTime']}))
                start_published.wait_for_publish()
                print("Start published = " + str(start_published.is_published()))
                schedule_counter += 1
                leftover.pop(0)

            if len(leftover) > 0:
                schedules_to_file = {"Schedules" : []}
                for schedule in leftover:
                    schedules_to_file['Schedules'].append(schedule)
                with open("./new_schedules.txt", 'w+') as file:
                    file.write(json.dumps(schedules_to_file))
            else:
                open("./new_schedules.txt", 'w+').close()
            
            sleep(1)
            client.disconnect()
            sleep(1)
            if not client.is_connected():
                print("Client successfully disconnected")
            break
        else:
            print("Waiting for the connection to be established...")
            sleep(1)
    
