from flask import Flask, request, abort, send_file, jsonify, make_response
import psycopg2
from config import config
import os
from io import StringIO
import ttn
app = Flask(__name__)


@app.route("/")
def hello():
    return "Server is running!";


if __name__ == '__main__':
    app.run(debug=True)
