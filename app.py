import json
from datetime import date, datetime

import jsonschema
import psycopg2
from flask import Flask, request, Response
from jsonschema import validate

from config import config


def json_serial(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def validateJSON(schema, json_to_validate):
    schema_path = getSchemaPath(schema)
    with open(schema_path, 'r') as schema_file:
        schema = json.loads(schema_file.read())

    try:
        validate(instance=json_to_validate, schema=schema)
    except jsonschema.exceptions.ValidationError as err:
        print(err)
        print("Json valid: false")
        return False
    print("Json valid: true")
    return True


# Returns the path to the schema of the specified resource as a string
def getSchemaPath(resource):
	return 'schema/' + resource + '.schema.json'


# Returns the Link header string
def getLinkHeader(resource):
	schemaPath = '/' + getSchemaPath(resource)
	return '<' + schemaPath + '>; rel="describedby"; type="application/schema+json"'


app = Flask(__name__)


@app.route("/")
def landing():
    return "Server is running!"


# Return all measurements of a location
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
        response.headers["Link"] = getLinkHeader('measurements')
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response


# Return location info
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
        response.headers['Link'] = getLinkHeader('locationinfo')
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response


# Alle locations gesorteerd op filter inclusief de laatst bekende meting waardes.
@app.route("/locations", methods=['GET'])
def get_nodes():
    filter = request.args.get('filter')
    if filter == "moist":
        filt = "humidity desc;"
    elif filter == "dry":
        filt = "humidity asc;"
    elif filter == "warmest":
        filt = "temperature desc;"
    elif filter == "coolest":
        filt = "temperature asc;"
    elif filter == "none" or filter is None:
        filt = "measurement.measurementid asc;"
    else:
        return Response(response='filter is not set properly!', status=400)

    sql = """select measurement.nodeid, locationname, description, temperature, humidity
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
                'location': node[1],
                'description': node[2],
                'temperature': node[3],
                'humidity': node[4]
            })
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conn is not None:
            conn.close()
            data.update({'nodes': nodes_as_dict})
        response = Response(response=json.dumps(data), status=200, mimetype='application/json')
        response.headers['Link'] = getLinkHeader('nodes')
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response


# Alle metingen returnen.
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
        response.headers["Link"] = getLinkHeader('measurements')
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response


# Alle metingen terug van de meegeven locatie.
@app.route("/measurements/<locationID>", methods=['GET'])
def get_all_sensor_data(locationID):
    # Don't use duplicate code
    return getLocationData(locationID)


# Meting met meegegeven id wordt verwijderd.
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
        response.headers["Link"] = getLinkHeader('measurements')
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response


# Voeg meting toe.
@app.route("/measurement", methods=['POST'])
def get_measurement_from_post_request():
    measurement = request.get_json()

    if not validateJSON("measurement", measurement):
        return Response(response={'measurement_id': "Bad request"}, status=400, mimetype="application/json")

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


# Voeg locatie toe.
@app.route("/location", methods=['POST'])
def add_location():
    location = request.get_json()
    if not validateJSON("location", location):
        return Response(response={'locationInfo': "Bad request"}, status=400, mimetype="application/json")

    nodeID = location['nodeID']
    desc = location['description']
    name = location['locationname']
    capacity = location['capacity']

    loc = add_location(nodeID, desc, name, capacity)
    return Response(response={'locationInfo': loc}, status=201, mimetype="application/json")


def add_location(locID, desc, name, capacity):
    sql = """INSERT INTO node(nodeID, description, locationname, capacity) VALUES(%s, %s, %s, %s) RETURNING nodeID;"""

    conn = None
    nodeID = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql, (locID, desc, name, capacity))
        nodeID = cur.fetchone()[0]
        # conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return nodeID


if __name__ == '__main__':
    app.run(debug=True)
