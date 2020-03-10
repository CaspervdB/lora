from flask import Flask, request, abort, send_file, jsonify, make_response
import psycopg2
from config import config
import os
from io import StringIO
import ttn

# ------BEGIN MQTT TTN PART------ #

app_id = "co2_sensor_stenden"
access_key = "ttn-account-v2.J5ws5KGhK9jVP5p56HfG1VyLka8PecrVTtIsam6MpWA"


def uplink_callback(msg, client):
    humidity =  msg.payload_fields.humidity
    temperature = msg.payload_fields.temperature
    datetime = msg.metadata.time
    addMeting(msg.dev_id, temperature, humidity, datetime)

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

    addMeting(nodeID, temperature, humidity, datetime)

@app.route("/getallsensors", methods=['GET'])
def getallsensors():

    sql = """SELECT * FROM node;"""
    conn = None
    data = {}
    nodes_as_dict = []

    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql)
        nodes = cur.fetchall()

        for node in nodes:

            nodeID = node[0]
            description = node[1]

            node_as_dict = {
                'nodeID': nodeID,
                'description': description
            }
            nodes_as_dict.append(node_as_dict)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conn is not None:
            conn.close()
            data.update({'nodes': nodes_as_dict})
        return jsonify(data)

@app.route("/getalldatafornode", methods=['GET'])
def getallsensordata():
    nodeID = request.args.get('node')
    sql = """SELECT measurementID, temperature, humidity, datetime FROM measurement WHERE nodeID = %s;"""
    conn = None
    data = {}
    measurements_as_dict = []

    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql, nodeID)
        measurements = cur.fetchall()

        for measurement in measurements:

            measurementID = measurement[0]
            temperature = measurement[1]
            humidity = measurement[2]
            datetime = measurement[3]

            measurement_as_dict = {
                'measurementID': measurementID,
                'temperature': temperature,
                'humidity': humidity,
                'datetime': datetime
            }
            measurements_as_dict.append(measurement_as_dict)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conn is not None:
            conn.close()
            data.update({'measurements': measurements_as_dict})
        return jsonify(data)

def addMeting(nodeID, temperature, humidity, datetime):
    sql = """INSERT INTO measurement(nodeID, temperature, humidity, datetime) VALUES(%s, %s, %s, %s) RETURNING measurementID;"""

    conn = None
    metingID = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql, (nodeID, temperature, humidity, datetime))
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
