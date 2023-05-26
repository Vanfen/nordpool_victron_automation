import json
import os
import ssl
from time import sleep
import paho.mqtt.client as mqtt
from dotenv import load_dotenv

load_dotenv()

username = os.getenv('UNAME')
password = os.getenv('PASSWORD')
portal_id = os.getenv('PORTAL_ID')

flag_connected = 0

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client: mqtt.Client, userdata, flags, rc):
    global flag_connected
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("$SYS/#")
    flag_connected = 1

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
   print(msg.topic+" "+str(msg.payload))

def on_publish(client, userdata, rc):
    print("Publish callback")
    pass

def on_disconnect(client: mqtt.Client, userdata, rc):
    global flag_connected
    print("Disconnected with code " + str(rc))
    client.loop_stop()
    flag_connected = 0

def refresh_schedules():
    global flag_connected

    print("Refresh schedules")

    client = mqtt.Client("RemoveUsed")
    client.tls_set(cert_reqs=ssl.CERT_NONE)
    client.tls_insecure_set(True)
    client.username_pw_set(username=username, password=password)
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_publish = on_publish
    client.on_disconnect = on_disconnect

    client.connect("mqtt87.victronenergy.com", port=8883)

    client.loop_start()

    with open("./schedules_to_remove.txt", 'r') as file:
        schedules = file.readlines() 
    
    while 1:
        if flag_connected == 1:
            if len(schedules) > 0:
                for sch_num in schedules:
                    sch_num = sch_num.rstrip()
                    day_published = client.publish("W/%s/settings/0/Settings/CGwacs/BatteryLife/Schedule/Charge/%s/Day" % (portal_id, sch_num), json.dumps({"value": -7}))
                    day_published.wait_for_publish()
                    print("Day published = " + str(day_published.is_published()))
                    duration_published = client.publish("W/%s/settings/0/Settings/CGwacs/BatteryLife/Schedule/Charge/%s/Duration" % (portal_id, sch_num), json.dumps({"value": 0}))
                    duration_published.wait_for_publish()
                    print("Duration published = " + str(duration_published.is_published()))
                    start_published = client.publish("W/%s/settings/0/Settings/CGwacs/BatteryLife/Schedule/Charge/%s/Start" % (portal_id, sch_num), json.dumps({"value": 0}))
                    start_published.wait_for_publish()
                    print("Start published = " + str(start_published.is_published()))
                    
            else:
                print("No schedules to remove")

            # Erase schedules_to_remove.txt
            open("./schedules_to_remove.txt", 'w').close()
            
            sleep(1)
            client.disconnect()
            sleep(1)
            if not client.is_connected():
                print("Client successfully disconnected")
            break
        else:
            print("Waiting for the connection to be established...")
            sleep(1)