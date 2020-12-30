import paho.mqtt.client as mqtt
import json, time

class Mqtt(object):


  def __init__(self, broker_host, broker_user, broker_passwd, topic, on_connect=None):
    self.broker_host = broker_host
    self.broker_user = broker_user
    self.broker_passwd = broker_passwd
    self.topic = topic
    self.on_connect_attr = on_connect
    self.client = None
    self.connected = False


  def on_connect(self, mqttc, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    if rc == 0:
      self.connected = True


  def on_disconnect(self, mqttc, obj, rc):
    print('disconnected...rc=' + str(rc))
    self.connected = False


  def connect(self):
    self.client = mqtt.Client()
    self.client.username_pw_set(username=self.broker_user,password=self.broker_passwd)
    self.client.on_connect = self.on_connect if self.on_connect_attr == None else self.on_connect_attr
    self.client.on_disconnect = self.on_disconnect
    self.client.connect(self.broker_host,keepalive=10)


  def publish(self,message):
    while not self.connected:
      time.sleep(1)
    # if not self.connected:
    #   self.connect()
    self.client.publish(self.topic,message, qos=1)


  def subscribe(self):
    self.client.subscribe(self.topic, qos=1)


  def loop_forever(self):
    self.client.loop_forever(retry_first_connection=True)