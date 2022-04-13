#!/bin/python3

import random
import time

import sunspec.core.client as sunspec_client
from paho.mqtt import client as mqtt_client

sunspec_host = "192.168.178.105"
sunspec_port = 502
modbus_id = 200

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
  d = sunspec_client.SunSpecClientDevice(sunspec_client.TCP, modbus_id, ipaddr=sunspec_host, ipport=sunspec_port, timeout=1.0)
  d.read()
  return d

def gather_points(b,p,c):
  for point in b.points:
    val = getattr(b,point)
    if val is not None:
      c.publish(p+"/"+point, val)

def gather_sunspec(d,p,c):
  d.read()
  for m in d.models:
    model_topic=p+"/"+m
    model = getattr(d,m)
    gather_points(model, model_topic, c)
    if model.repeating is not None:
      for idx, b in enumerate(model.repeating):
        if b is not None:
          block_topic = model_topic+"/"+model.repeating_name+"-"+str(idx)
          gather_points(b,block_topic,c)

def subscribe(client: mqtt_client):
  def on_message(client, userdata, msg):
    print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")

  client.subscribe(topic_prefix+"/+/+/set/#")
  client.on_message = on_message

def run():
  d = connect_sunspec()
  device_name=d.common.Mn+"-"+d.common.SN
  topic=topic_prefix+"/"+device_name
  client = connect_mqtt()
  subscribe(client)
  client.loop_start()
  while True:
    gather_sunspec(d,topic,client)
    time.sleep(10)

if __name__ == '__main__':
  run()
