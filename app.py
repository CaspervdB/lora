from flask import Flask, request, Response, jsonify
import psycopg2
from config import config
import json
from datetime import date, datetime


def json_serial(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


app = Flask(__name__)


@app.route("/")
def landing():
    return "Server is running!"


# KLAAR! Alle informatie van de locatie inclusief de laatste bekende meting waardes
@app.route("/locations/<locationID>", methods=['GET'])
def getLocationData(locationID):
    sql = """SELECT measurementid, temperature, humidity, datetime FROM measurement WHERE nodeid = %s;"""
    conn = None
    data = {}
    measurements_as_dict = []

    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql, locationID)
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
        response = Response(response=json.dumps(data, default=json_serial), status=200, mimetype='application/json')
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response


# KLAAR! Returnt alle informatie van een locatie
@app.route("/locationInfo/<locationID>", methods=['GET'])
def getLocationInfo(locationID):
    sql = """SELECT description, locationname, capacity FROM node WHERE nodeid = %s;"""
    conn = None
    data = {}
    list = []

    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql, locationID)
        info = cur.fetchall()

        for info in info:
            description = info[0]
            locationname = info[1]
            capacity = info[2]

            info_dict = {
                'description': description,
                'locationname': locationname,
                'capacity': capacity
            }
            list.append(info_dict)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conn is not None:
            conn.close()
            data.update({'locationInfo': list})
        response = Response(response=json.dumps(data, default=json_serial), status=200, mimetype='application/json')
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response


#  KLAAR! Alle locations gesorteerd op filter inclusief de laatst bekende meting waardes.
@app.route("/locations", methods=['GET'])
def get_nodes():
    filter = request.args.get('filter')
    if (filter == "moist"):
        filt = "humidity desc;"
    elif (filter == "dry"):
        filt = "humidity asc;"
    elif (filter == "warmest"):
        filt = "temperature desc;"
    elif (filter == "coolest"):
        filt = "temperature asc;"
    elif (filter == "none" or filter == None):
        filt = "measurement.measurementid asc;"
    else:
        return Response(response='filter is not set properly!', status=400)

    sql = """select measurement.nodeid, description, temperature, humidity
                from measurement
                INNER JOIN sortnodesview
                ON measurement.nodeid = sortnodesview.nodeid
                AND measurement.measurementid = sortnodesview.sortnodesid
                Inner Join node
                on measurement.nodeid = node.nodeid
                Order by """ + filt
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
            nodes_as_dict.append({
                'nodeID': node[0],
                'description': node[1],
                'temperature': node[2],
                'humidity': node[3]
            })
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conn is not None:
            conn.close()
            data.update({'nodes': nodes_as_dict})
        response = Response(response=json.dumps(data), status=200, mimetype='application/json')
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response


# KLAAR! Alle metingen returnen.
@app.route("/measurements", methods=['GET'])
def getAllData():
    sql = """SELECT measurementID, temperature, humidity, datetime FROM measurement"""
    conn = None
    data = {}
    measurements_as_dict = []

    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql)
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
        response = Response(response=json.dumps(data, default=json_serial), status=200, mimetype='application/json')
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response


# KLAAR! Alle metingen terug van de meegeven locatie.
@app.route("/measurements/<locationID>", methods=['GET'])
def get_all_sensor_data(locationID):
    sql = """SELECT measurementid, temperature, humidity, datetime FROM measurement WHERE nodeid = %s;"""
    conn = None
    data = {}
    measurements_as_dict = []

    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql, locationID)
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
        response = Response(response=json.dumps(data, default=json_serial), status=200, mimetype='application/json')
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response


# KLAAR! Meting met meegegeven id wordt verwijderd.
@app.route("/measurements/<measurementID>", methods=['DELETE'])
def deleteMeasurement(measurementID):
    sql = """DELETE FROM measurement WHERE measurementid = %s;"""
    conn = None

    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql, measurementID)
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conn is not None:
            conn.close()
        response = Response(status=204, mimetype='application/json')
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response


# KLAAR! voeg meting toe.
@app.route("/measurement", methods=['POST'])
def get_measurement_from_post_request():
    measurement = request.get_json()
    nodeID = measurement['nodeID']
    temperature = measurement['temperature']
    datetime = measurement['datetime']
    humidity = measurement['humidity']
    measurement_id = add_measurement(nodeID, temperature, humidity, datetime)
    return Response(response={'measurement_id': measurement_id}, status=201, mimetype="application/json")


def add_measurement(nodeID, temperature, humidity, datetime):
    sql = """INSERT INTO measurement(nodeID, temperature, humidity, datetime) VALUES(%s, %s, %s, %s) RETURNING measurementID;"""

    conn = None
    measurement_id = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql, (nodeID, temperature, humidity, datetime))
        measurement_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return measurement_id


if __name__ == '__main__':
    app.run(debug=True)
