import re
from flask import jsonify, request, Response
from flask import Flask
from flask.json import dump, dumps
from flask_restful import Resource, Api, reqparse
from flask_pymongo import PyMongo
import json
from bson.json_util import dumps
import pymongo
from pymongo import MongoClient
from database.db import get_all_carriers, get_all_partners, get_order_delivery


app = Flask(__name__)
app.config['MONGO_URI'] = "mongodb://localhost:27017/"

mongo = PyMongo(app)


@app.route("/", methods=['GET'])
def index():
    return "hello"


@app.route("/carriers", methods=['GET'])
def get_carriers():
    res = get_all_carriers()
    return Response(response=dumps(res, indent=2))


@app.route("/partners", methods=['GET'])
def get_partners():
    res = get_all_partners()
    return Response(response=dumps(res, indent=2))


@app.route("/orders", methods=['POST'])
def get_assigned_orders():
    order_ids = []
    order_wts = []
    req_data = request.get_json()
    for data in list(req_data):
        order_ids.append(data['order_id'])
        order_wts.append(data['order_weight'])

    results = get_order_delivery(1, order_ids, order_wts)
    return Response(response=dumps(results, indent=2))


if __name__ == "__main__":
    app.run(debug=True)
