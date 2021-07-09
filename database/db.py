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

slot_1_carriers = [2, 3]
slot_2_carriers = [1, 2, 3]
slot_3_carriers = [1, 2, 3]
slot_4_carriers = [1]
total_trip = [1, 2, 3]
capacity_per_trip = [100, 50, 30]


def print_array(arr):
    print(arr)


def swap(arr, a, b):
    tmp = arr[a]
    arr[a] = arr[b]
    arr[b] = tmp
    return arr


def get_all_combinations(arr, n):
    if (n == len(arr)):
        output.append(arr.copy())
    else:
        for i in range(n, len(arr)):
            arr = swap(arr, n, i)
            get_all_combinations(arr, n+1)
            arr = swap(arr, n, i)


def get_vehicle_type(typeId):
    if (typeId == 1):
        return "truck"
    elif (typeId == 2):
        return "scooter"
    else:
        return "bike"


def get_slot(slotId):
    if(slotId == 1):
        return slot_1_carriers
    elif(slotId == 2):
        return slot_2_carriers
    elif(slotId == 3):
        return slot_3_carriers
    else:
        return slot_4_carriers


def generate_pickup(slot, order, total_weight):
    weight_left = 0
    j = 0
    orderId = 1
    orders = []
    carriers = get_slot(slot)
    current_carrier = carriers[j]
    current_carrier_capacity = capacity_per_trip[current_carrier - 1]
    current_carrier_trip = total_trip[current_carrier - 1]
    current_order = {"vechicle_type": get_vehicle_type(
        current_carrier), "delivery_partner_id": 1, "list_of_order_ids_assigned": []}
    # print(current_order.delivery_partner_id)
    for i in order:
        if (total_weight <= 0):
            orders[len(orders) - 1]['list_of_order_ids_assigned'].append(i)
            break
        else:
            if (i > current_carrier_capacity):
                orderId += 1
                orders.append(orderId)
                j += 1
                if (j < len(carriers)):
                    current_carrier = carriers[j]
                    total_weight -= capacity_per_trip[current_carrier - 1]
                    weight_left = capacity_per_trip[current_carrier - 1] - i
            else:
                total_weight -= current_carrier_capacity
                weight_left = current_carrier_capacity - i
                current_carrier_trip -= 1
                current_carrier_capacity -= i
                if (current_carrier_capacity <= 0 and current_carrier_trip > 0):
                    current_order = {"vechicle_type": get_vehicle_type(
                        current_carrier), "delivery_partner_id": orderId, "list_of_order_ids_assigned": []}
                    orderId += 1
                    current_carrier_capacity = capacity_per_trip[current_carrier - 1]
                    delievered_order = current_order['list_of_order_ids_assigned']
                    delievered_order.append(i)
                    current_order['list_of_order_ids_assigned'] = delievered_order
                    orders.append(current_order.copy())
                elif (current_carrier_capacity > 0 and current_carrier_trip > 0):
                    current_order = {"vechicle_type": get_vehicle_type(
                        current_carrier), "delivery_partner_id": orderId, "list_of_order_ids_assigned": []}
                    delievered_order = current_order['list_of_order_ids_assigned']
                    delievered_order.append(i)
                    current_order['list_of_order_ids_assigned'] = delievered_order
                    orders.append(current_order.copy())
                else:
                    orderId += 1
                    j += 1
                    current_carrier = carriers[j]
                    current_order = {"vechicle_type": get_vehicle_type(
                        current_carrier), "delivery_partner_id": orderId, "list_of_order_ids_assigned": []}
                    delievered_order = current_order['list_of_order_ids_assigned']
                    delievered_order.append(i)
                    current_order['list_of_order_ids_assigned'] = delievered_order
                    orders.append(current_order.copy())
                    current_carrier_capacity = capacity_per_trip[current_carrier - 1]
                    current_carrier_trip = total_trip[current_carrier - 1]
                    total_weight -= current_carrier_capacity
                    weight_left = current_carrier_capacity - i
                    current_carrier_trip -= 1
                    current_carrier_capacity -= i
    return orders


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


def get_order_delivery(slot, order_ids, order_wts):
    total_weight = sum(order_wts)

    res = generate_pickup(slot, order_ids, total_weight)
    return res


con.close()
