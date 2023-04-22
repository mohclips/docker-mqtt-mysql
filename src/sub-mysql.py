#!/usr/local/bin/python3

import paho.mqtt.client as mqtt
import pymysql.cursors
import sys
import time
import json
from datetime import datetime

import login_details

# data ingest from here https://www.speaktothegeek.co.uk/2022/06/hildebrand-glow-update-local-mqtt-and-home-assistant/


db = 0
e_last =''
g_last = ''

def on_connect(client, userdata, flags, rc):
    
    global db
    
    print("MQTT Client Connected")
    client.subscribe(login_details.sensor)
    
    try:
        db = pymysql.connect(host=login_details.mysqlHost, user=login_details.mysqlUser, password=login_details.mysqlPassword, db=login_details.dbName, 
            charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
        #print("MySQL Client Connected")
    except pymysql.Error as e:
        print(e)
        sys.exit()

def convert_date(str):
    date_object = datetime.strptime(str, '%Y-%m-%dT%H:%M:%SZ')
    return date_object.strftime('%Y-%m-%d %H:%M:%S')


def on_message(client, userdata, msg):
            
    global db
    global e_last
    global g_last
    
    #print("Transmission received")

    _msg = str(msg.payload.decode("utf-8"))
    j = json.loads(_msg)

    #print(json.dumps(j))
    
    if "electricitymeter" in j:
    
        tm = convert_date(j['electricitymeter']['timestamp'])
        
        if tm == e_last:
            # skip dups
            print('e_dup')
            return
        e_last = tm
        
        e_power_val = (j['electricitymeter']['power']['value'] * 1000)
        e_import_cum = j['electricitymeter']['energy']['import']['cumulative']
        e_export_cum = j['electricitymeter']['energy']['export']['cumulative']
        e_ipport_cum_unitrate = j['electricitymeter']['energy']['import']['price']['unitrate']
        
        print("e", tm, e_power_val, e_import_cum, e_export_cum, e_ipport_cum_unitrate)
        
        # now = time.strftime('%Y-%m-%d %H:%M:%S')
        
        # write to electricity sql table

        with db.cursor() as cursor:
            try:
                sql = "INSERT INTO `electricity` (`date_time`, `e_power_val`, `e_import_cum`, `e_export_cum`, `e_ipport_cum_unitrate`) VALUES (%s, %s, %s, %s, %s)"
                cursor.execute(sql, (tm, e_power_val, e_import_cum, e_export_cum, e_ipport_cum_unitrate))
            except pymysql.Error as e:
                print(e)
                sys.exit()
            finally:
                db.commit()

        #db.close()
 
    if "gasmeter" in j:
        tm = convert_date(j['gasmeter']['timestamp'])
        
        if tm == g_last:
            # skip dups
            print('g_dup')
            return
        g_last = tm

        g_import_cum = j['gasmeter']['energy']['import']['cumulative']
        g_import_unitrate = j['gasmeter']['energy']['import']['price']['unitrate']
        g_import_standing = j['gasmeter']['energy']['import']['price']['standingcharge']
    
        print("g", tm, g_import_cum, g_import_unitrate, g_import_standing)
        
        # write to gas sql table
        with db.cursor() as cursor:
            try:
                sql = "INSERT INTO `gas` (`date_time`, `g_import_cum`, `g_import_unitrate`, `g_import_standing`) VALUES (%s, %s, %s, %s)"
                cursor.execute(sql, (tm, g_import_cum, g_import_unitrate, g_import_standing))
            except pymysql.Error as e:
                print(e)
                sys.exit()
            finally:
                db.commit()

        #db.close()
        
    return

    #{"electricitymeter": {"timestamp": "2023-01-07T23:24:14Z", "energy": {"export": {"cumulative": 0.0, "units": "kWh"}, "import": {"cumulative": 4562.327, "day": 23.924, "week": 130.407, "month": 151.909, "units": "kWh", "mpan": "1100018347997", "supplier": "Octopus Energy", "price": {"unitrate": 0.4111, "standingcharge": 0.4422}}}, "power": {"value": 0.916, "units": "kW"}}}
    #{"gasmeter": {"timestamp": "2023-01-07T23:24:10Z", "energy": {"import": {"cumulative": 7063.546, "day": 49.464, "week": 290.707, "month": 344.933, "units": "kWh", "cumulativevol": 629.514, "cumulativevolunits": "m3", "dayvol": 49.464, "weekvol": 290.707, "monthvol": 344.933, "dayweekmonthvolunits": "kWh", "mprn": "2188127003", "supplier": "---", "price": {"unitrate": 0.1024, "standingcharge": 0.2684}}}}}
    #{"software": "v1.8.13", "timestamp": "2023-01-07T23:24:15Z", "hardware": "GLOW-IHD-01-1v4-SMETS2", "wifistamac": "4C11AEADF118", "ethmac": "4C11AEADF11B", "smetsversion": "SMETS2", "eui": "70:B3:D5:21:E0:00:E7:8E", "zigbee": "1.2.5", "han": {"rssi": -47, "status": "joined", "lqi": 212}}




# Connect the MQTT Client
client = mqtt.Client("energy2mysql-1")
client.on_connect = on_connect
client.on_message = on_message
# client.username_pw_set(username=mqttUser, password=mqttPassword)
try:
    client.connect(login_details.mqttBroker, login_details.mqttBrokerPort, 60)
except:
    sys.exit("Connection to MQTT Broker failed")
# Stay connected to the MQTT Broker indefinitely
client.loop_forever()

# client.loop_start() #start the loop
# time.sleep(10) # wait
# client.loop_stop()

db.close()