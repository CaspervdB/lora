from flask import Flask, request, abort, send_file, jsonify, make_response
import psycopg2
from config import config
import os
from io import StringIO
import ttn

# ------BEGIN MQTT TTN PART------ #

app_id = "co2_sensor_stenden"
access_key = "ttn-account-v2.J5ws5KGhK9jVP5p56HfG1VyLka8PecrVTtIsam6MpWA"
counter = 0
def uplink_callback(msg, client):
	global counter
	print("Received uplink from ", msg.dev_id)
	print("Humidity: " + msg.payload_fields.humidity)
	print("Temperature: " + msg.payload_fields.temperature)
	print(counter)
	counter = counter + 1
	# addMeting(msg.dev_id, temperature, msg.metadata.time, humidity)

handler = ttn.HandlerClient(app_id, access_key)


mqtt_client = handler.data()
mqtt_client.set_uplink_callback(uplink_callback)
mqtt_client.connect()

# ------END MQTT TTN PART------ #

app = Flask(__name__)

@app.route("/")
def landing():
    return "Server is running!"


@app.route("/addMeting", methods=['POST'])
def addMetingFromPostRequest():
    measurement = request.get_json()
    nodeID = measurement['nodeID']
    temperature = measurement['temperature']
    datetime = measurement['datetime']
    humidity = measurement['humidity']

    addMeting(nodeID, temperature, datetime, humidity)


def addMeting(nodeID, temperature, datetime, humidity):
    sql = """INSERT INTO measurement(nodeID, temperature, datetime, humidity) VALUES(%s, %s, %s, %s) RETURNING measurementID;"""

    conn = None
    metingID = None
    try:
        params = config();
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql, (nodeID, temperature, datetime, humidity))
        metingID = cur.fetchone()[0]
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    print(metingID)
    return jsonify({'result': "succes", 'metingID': metingID})


if __name__ == '__main__':
    app.run(debug=False)
