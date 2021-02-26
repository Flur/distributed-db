import pymongo
import pprint
from bson.json_util import dumps

client = pymongo.MongoClient("mongodb://127.0.0.1:27017")
db = client["distributed-db"]


# 1
def insert_many_products():
    db.products.insert_many([
        {
            "category" : "Phone",
            "model" : "iPhone 6",
            "producer" : "Apple",
            "price" : 600
        },
        {
            "category" : "TV",
            "model" : "SmartTV",
            "producer" : "Samsung",
            "diagonal": "100",
            "price" : 6000
        },
        {
            "category" : "Watch",
            "model" : "IWatch",
            "producer" : "Apple",
            "size": "m",
            "price" : 300,
        },
        {
            "category" : "Speaker",
            "model" : "Sound2",
            "producer" : "JBL",
            "Color": "Red",
            "price" : 100,
        },
        {
            "category" : "Laptop",
            "model" : "Super",
            "producer" : "Dell",
            "CPU": "i100",
            "GPU": "NVidia",
            "price" : 1000,
        },
    ])


# 2
def get_all_products_json():
    cursor = db.products.find({})

    list_cur = list(cursor)
    json_data = dumps(list_cur)

    pprint.pprint(json_data)


# 3
def count_items_by_category(category):
    count = db.products.count_documents({"category": category})
    pprint.pprint(count)


# 4
def count_distict_categories():
    count = len(db.products.distinct("category"))
    pprint.pprint(count)


# 5
def distinct_producers():
    count = db.products.distinct("producer")
    pprint.pprint(count)

# 6
def queries_
# get_all_products()
# count_items_by_category("Phone")
distinct_producers()
