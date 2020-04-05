import os
import csv
from pymongo import UpdateOne, MongoClient
from dotenv import load_dotenv
load_dotenv()

# mongodb connection
mongo_driver = MongoClient(os.getenv("MONGO_CONNECTION_STRING"))
hetio_db = mongo_driver.get_database(os.getenv("MONGO_DB_NAME"))
mongo_collection_node_map = {
    "Anatomy": "anatomy",
    "Disease": "diseases",
    "Gene": "genes",
    "Compound": "compounds"
}

def get_disease(disease_id):
    return list(hetio_db["diseases"].aggregate([
        {"$match":{"id":disease_id}},
        {"$limit":1},
        {"$lookup":{"from":"anatomy","localField":"localizes_ids","foreignField":"id","as":"localizes"}},
        {"$lookup":{"from":"compounds","localField":"palliates_ids","foreignField":"id","as":"palliates"}},
        {"$lookup":{"from":"compounds","localField":"treats_ids","foreignField":"id","as":"treats"}},
        {"$lookup":{"from":"genes","localField":"upregulates_ids","foreignField":"id","as":"upregulates"}},
        {"$lookup":{"from":"genes","localField":"downregulates_ids","foreignField":"id","as":"downregulates"}},
        {"$lookup":{"from":"genes","localField":"associates_ids","foreignField":"id","as":"associates"}},
        {"$project":{"_id":0,"name":1,"localizes.name":1,"associates.name":1,"treats.name":1,"upregulates.name":1,"downregulates.name":1,"palliates.name":1}}
    ]))

def reset_data():
    collections = ["diseases", "genes", "anatomy", "compounds"]
    for collection in collections:
        if collection in hetio_db.list_collection_names():
            hetio_db.drop_collection(collection)
        hetio_db.create_collection(collection)

def import_nodes():
    with open("dataset/nodes.tsv") as tsvfile:
        reader = csv.DictReader(tsvfile, dialect='excel-tab')
        parsed_rows = {
            "anatomy":[],
            "diseases":[],
            "genes":[],
            "compounds" :[]
        }
        row_count = 0
        for row in reader:
            coll = mongo_collection_node_map[row["kind"]]
            del row["kind"]
            parsed_rows[coll].append(row)
            row_count+=1
            if row_count == 1000:
                for collection in parsed_rows:
                    if len(parsed_rows[collection]) > 0:
                        hetio_db[collection].insert_many(parsed_rows[collection])
                        parsed_rows[collection] = []
                row_count = 0
        for collection in parsed_rows:
            if len(parsed_rows[collection]) > 0:
                hetio_db[collection].insert_many(parsed_rows[collection])

def generate_update(edge, source, target):
    if edge == "CpD":
        return UpdateOne({ "id" : target }, {'$push': {'palliates_ids': source }})
    elif edge == "CtD":
        return UpdateOne({ "id" : target }, {'$push': {'treats_ids': source }})
    elif edge == "DlA":
        return UpdateOne({ "id" : source }, {'$push': {'localizes_ids': target }})
    elif edge == "DuG":
        return UpdateOne({ "id" : source }, {'$push': {'upregulates_ids': target }})
    elif edge == "DdG":
        return UpdateOne({ "id" : source }, {'$push': {'downregulates_ids': target }})
    elif edge == "DaG":
        return UpdateOne({ "id" : source }, {'$push': {'associates_ids': target }})

def import_edges():
    with open("dataset/edges.tsv") as tsvfile:
        reader = csv.DictReader(tsvfile, dialect='excel-tab')
        parsed_rows = {
            "CpD":[],
            "CtD":[],
            "DlA":[],
            "DuG":[],
            "DdG":[],
            "DaG":[]
        }
        row_count = 0
        for row in reader:
            edge = row["metaedge"]
            if edge in parsed_rows:
                parsed_rows[edge].append(generate_update(edge, row["source"], row["target"]))
                row_count+=1
            if row_count == 10000:
                for edge in parsed_rows:
                    if len(parsed_rows[edge]) > 0:
                        hetio_db["diseases"].bulk_write(parsed_rows[edge])
                        parsed_rows[edge] = []
                row_count = 0
        for edge in parsed_rows:
            if len(parsed_rows[edge]) > 0:
                hetio_db["diseases"].bulk_write(parsed_rows[edge])