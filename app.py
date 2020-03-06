from flask import Flask, request, abort, send_file, jsonify, make_response
import psycopg2
from config import config
import os
from io import StringIO
import ttn
import base64

message = "nog niks"
# ------BEGIN MQTT TTN PART------ #

app_id = "co2_sensor_stenden"
access_key = "ttn-account-v2.J5ws5KGhK9jVP5p56HfG1VyLka8PecrVTtIsam6MpWA"

def uplink_callback(msg, client):
  print("Received uplink from ", msg.dev_id)
  print(msg)
  global message
  data = base64.b64decode(msg.payload_raw).decode('ascii')
  data = data.split(',')
  humidity = data[1]
  message = temperature = data[2]
  print("deviceID: " + msg.dev_id)
  print("humidity: " + humidity)
  print("temperature: " + temperature)

handler = ttn.HandlerClient(app_id, access_key)


mqtt_client = handler.data()
mqtt_client.set_uplink_callback(uplink_callback)
mqtt_client.connect()

# ------END MQTT TTN PART------ #
app = Flask(__name__)


@app.route("/")
def landing():
    return "Server is running!"

@app.route("/TTN")
def hello():
    global message
    return message

if __name__ == '__main__':
    app.run(debug=True)
