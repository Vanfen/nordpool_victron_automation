import datetime
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
schedules = []

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client: mqtt.Client, userdata, flags, rc):
    global flag_connected
    print("Connected with result code "+str(rc))
    flag_connected = 1
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("$SYS/#")

    ids = [0, 1, 2, 3, 4]
    for num in ids:
        client.subscribe("N/%s/settings/0/Settings/CGwacs/BatteryLife/Schedule/Charge/%d/Day" % (portal_id, num))
        

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    val = json.loads(msg.payload)
    print(msg.topic+" -> "+str(val))
    # Parse the response message
    if "Charge" in msg.topic:
        # Get schedule id
        schedule_id = int(msg.topic[(msg.topic.find('Charge')+7):(msg.topic.find('Charge')+8)])
        if "Day" in msg.topic:
            if val['value'] == -7:
                schedules.append(schedule_id)


def on_disconnect(client: mqtt.Client, userdata, rc):
    global flag_connected
    print("Disconnected with code " + str(rc))
    client.loop_stop()
    flag_connected = 0

def write_available_schedules_to_file():
    with open("./available_schedules.txt", "w+") as file:
        for schedule_id in schedules:
            file.write(str(schedule_id) + "\n")



def search_for_available_schedules():
    global flag_connected

    print("Search for available schedules")

    client = mqtt.Client("FindAvailable")
    client.tls_set(cert_reqs=ssl.CERT_NONE)
    client.tls_insecure_set(True)
    client.username_pw_set(username=username, password=password)
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    client.connect("mqtt87.victronenergy.com", port=8883)

    client.loop_start()
    
    client.publish("R/%s/system/0/Serial" % portal_id)

    while 1:
        if flag_connected == 1:
            # Write id's of available schedules to the file
            write_available_schedules_to_file()
            sleep(1)
            client.disconnect()
            sleep(1)
            if not client.is_connected():
                print("Client successfully disconnected")
            break
        else:
            print("Waiting for the connection to be established...")
            sleep(1)
