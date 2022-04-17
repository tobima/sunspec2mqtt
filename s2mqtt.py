#!/bin/python3

import argparse
import configparser
import random
import time

import sunspec2.modbus.client as sunspec_client
import sunspec2

from paho.mqtt import client as mqtt_client

sunspec_host = "127.0.0.1"
sunspec_port = 502
modbus_id = 1

broker = "127.0.0.1"
broker_port = 1883
topic_prefix = "foo"

def connect_mqtt(client_id):
  def on_connect(client, userdata, flags,rc):
    if rc == 0:
      print("Connected to MQTT Broker!")
    else:
      print("Failed to connect, return code %d\n", rc)

  client = mqtt_client.Client(client_id)
  client.on_connect = on_connect
  client.connect(broker, broker_port)
  return client

def connect_sunspec():
  d = sunspec_client.SunSpecModbusClientDeviceTCP(slave_id=modbus_id, ipaddr=sunspec_host, ipport=sunspec_port, timeout=1.0)
  d.scan()
  return d

def gather_points(b,p,c):
  for key in b.points:
    if key in ['ID', 'L', 'N'] or key.endswith('_SF'):
      continue
    point = getattr(b,key)
    if point.is_impl():
      c.publish(p+"/"+key, point.cvalue)
      #print(p+"/"+key+": "+str(point.cvalue))

def gather_sunspec(d,p,c):
  for m in d.models:
    if type(m) is not str:
      continue
    model_topic=p+"/"+m
    model = d.models[m][0]
    model.read()
    gather_points(model, model_topic, c)
    for g in model.groups:
      for b in model.groups[g]:
        if b is not None:
          block_topic = model_topic+"/"+g+"-"+str(b.index)
          gather_points(b,block_topic,c)

def subscribe(client: mqtt_client):
  def on_message(client, userdata, msg):
    print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")

  client.subscribe(topic_prefix+"/+/+/set/#")
  client.on_message = on_message

def run():
  d = connect_sunspec()
  device_name=d.common[0].Mn.value+"-"+d.common[0].SN.value
  print("Found SunSpec device "+device_name)
  topic=topic_prefix+"/"+device_name
  client_id = 's2mqtt-'+device_name
  client = connect_mqtt(client_id)
  subscribe(client)
  client.loop_start()
  while True:
    try:
      gather_sunspec(d,topic,client)
    except sunspec2.modbus.modbus.ModbusClientError:
      pass
    time.sleep(5)

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='SunSpec to MQTT bridge')
  parser.add_argument('-c', help='config file')
  args = parser.parse_args()
  if args.c:
    config = configparser.ConfigParser()
    config.read(args.c)

    sunspec_host = config["sunspec"]["host"]
    sunspec_port = config["sunspec"].getint("port")
    modbus_id = config["sunspec"].getint("device_id")

    broker = config["mqtt"]["host"]
    broker_port = config["mqtt"].getint("port")
    topic_prefix = config["mqtt"]["topic"]
  run()
