#!/bin/python3

import random
import time

import sunspec2.modbus.client as sunspec_client
from paho.mqtt import client as mqtt_client

sunspec_host = "192.168.178.105"
sunspec_port = 502
modbus_id = 1

broker = "192.168.10.55"
broker_port = 1883
topic_prefix = "energy/solar"
client_id = f'python-mqtt-{random.randint(0, 1000)}'

def connect_mqtt():
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
  topic=topic_prefix+"/"+device_name
  client = connect_mqtt()
  subscribe(client)
  client.loop_start()
  while True:
    gather_sunspec(d,topic,client)
    time.sleep(5)

if __name__ == '__main__':
  run()
