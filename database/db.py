from enum import unique
from os import curdir
from bson.objectid import ObjectId
import pymongo
from pymongo import MongoClient
import json

from pymongo import cursor

CARRIER_PATH = "g:/Python/Projects/Grofers/database/data/carrier.json"
PARTNER_PATH = "g:/Python/Projects/Grofers/database/data/delivery_partners.json"

try:
    con = MongoClient("localhost", 27017)
    print("Connected Successfully")
except:
    print("Something went wrong")


grofers_db = con['grofers']

carriers = grofers_db['carriers']
delivery_partners = grofers_db['delivery_partners']

carriers.create_index("carrier_id", unique=True)
delivery_partners.create_index("delivery_partner_id")

file = open(CARRIER_PATH, "r")
data = json.loads(file.read())

try:
    grofers_db.carriers.insert_many(data)
except:
    print("Data Already Exist")

file = open(PARTNER_PATH, "r")
data = json.loads(file.read())

try:
    grofers_db.delivery_partners.insert_many(data)
except:
    print("Data Already Exist")


def get_all_carriers():
    res = []
    cursor = grofers_db.carriers.find()
    for x in cursor:
        res.append({"carrier_id": x['carrier_id'], "name": x['carrier'],
                    "capacity": x['capacity']})
    return res


def get_vechicle_type(carrier_id):
    cursor = grofers_db.carriers.find_one({"carrier_id": carrier_id})
    return cursor['carrier']


def get_carrier_capacity(carrier_id):
    cursor = grofers_db.carriers.find_one({"carrier_id": carrier_id})
    return cursor['capacity']


def get_all_partners():
    res = []
    cursor = grofers_db.delivery_partners.find()
    for x in cursor:
        res.append({"delivery_partner_id": x['delivery_partner_id'],
                    "order_id": x['order_id'],
                    "vechicle_type": get_vechicle_type(x['carrier_id']),
                    "vechicle_capacity": get_carrier_capacity(x['carrier_id'])})
    return res


def get_orders_assigned():
    return [10, 20]


def get_order_delivery(order_id, order_weights):
    res = []
    cursor = grofers_db.delivery_partners.find({"order_id": order_id})
    for x in cursor:
        res.append({"vechicle_type": get_vechicle_type(x['carrier_id']),
                    "delivery_partner_id": x['delivery_partner_id'],
                    "list_of_order_ids_assigned": get_orders_assigned()})
    return res


con.close()
