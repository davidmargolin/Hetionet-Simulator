import click
import os
import pprint
import shutil
import csv
from neo4j import GraphDatabase
from pymongo import UpdateOne, MongoClient
from dotenv import load_dotenv
load_dotenv()

# neo4j connection
neo4j_driver = GraphDatabase.driver(os.getenv("NEO4J_URL"), auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD")), encrypted=False)
neo4j_data_path = "{}/dataset".format(os.getenv("NEO4J_IMPORT_PATH"))

# mongodb connection
mongo_driver = MongoClient(os.getenv("MONGO_CONNECTION_STRING"))
hetio_db = mongo_driver.get_database(os.getenv("MONGO_DB_NAME"))
mongo_collection_node_map = {
    "Anatomy": "anatomy",
    "Disease": "diseases",
    "Gene": "genes",
    "Compound": "compounds"
}

def reset_data():
    # reset neo4j
    with neo4j_driver.session() as session:
        session.run(
            '''MATCH (n)
            DETACH DELETE n;'''
        ).single()

    # reset mongodb
    collections = ["diseases", "genes", "anatomy", "compounds"]
    for collection in collections:
        if collection in hetio_db.list_collection_names():
            hetio_db.drop_collection(collection)
        hetio_db.create_collection(collection)


def import_nodes_neo4j():
    shutil.copyfile("dataset/nodes.tsv", "{}/nodes.tsv".format(os.getenv("NEO4J_IMPORT_PATH")))  
    with neo4j_driver.session() as session:
        try:
            session.run(
                '''CREATE CONSTRAINT unique_ids
                    ON (n:Data)
                    ASSERT n.id IS UNIQUE;'''
            ).single()
        except:
            pass
        session.run(
            '''USING PERIODIC COMMIT
            LOAD CSV WITH HEADERS FROM 'file:///nodes.tsv' AS row
            FIELDTERMINATOR '\t'
            FOREACH(ignoreMe IN CASE WHEN trim(row.kind) = "Disease" THEN [1] ELSE [] END | MERGE (p:Data:Disease{id: row.id, name: row.name}))
            FOREACH(ignoreMe IN CASE WHEN trim(row.kind) = "Gene" THEN [1] ELSE [] END | MERGE (p:Data:Gene{id: row.id, name: row.name}))
            FOREACH(ignoreMe IN CASE WHEN trim(row.kind) = "Anatomy" THEN [1] ELSE [] END | MERGE (p:Data:Anatomy{id: row.id, name: row.name}))
            FOREACH(ignoreMe IN CASE WHEN trim(row.kind) = "Compound" THEN [1] ELSE [] END | MERGE (p:Data:Compound{id: row.id, name: row.name}));'''
        ).single()

def import_nodes_mongo():
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

def import_edges_neo4j():
    shutil.copyfile("dataset/edges.tsv", "{}/edges.tsv".format(os.getenv("NEO4J_IMPORT_PATH")))  
    with neo4j_driver.session() as session:
        session.run(
            '''USING PERIODIC COMMIT
            LOAD CSV WITH HEADERS FROM 'file:///edges.tsv' AS row
            FIELDTERMINATOR '\t'
            MATCH (s:Data{id: row.source}) 
            MATCH (t:Data{id: row.target})
            FOREACH(ignoreMe IN CASE WHEN split(row.metaedge,"")[1] = "u" THEN [1] ELSE [] END | MERGE (s)-[:UPREGULATES]->(t))
            FOREACH(ignoreMe IN CASE WHEN split(row.metaedge,"")[1] = "d" THEN [1] ELSE [] END | MERGE (s)-[:DOWNREGULATES]->(t))
            FOREACH(ignoreMe IN CASE WHEN split(row.metaedge,"")[1] = "t" THEN [1] ELSE [] END | MERGE (s)-[:TREATS]->(t))
            FOREACH(ignoreMe IN CASE WHEN split(row.metaedge,"")[1] = "p" THEN [1] ELSE [] END | MERGE (s)-[:PALLIATES]->(t))
            FOREACH(ignoreMe IN CASE WHEN split(row.metaedge,"")[1] = "b" THEN [1] ELSE [] END | MERGE (s)-[:BINDS]->(t))
            FOREACH(ignoreMe IN CASE WHEN split(row.metaedge,"")[1] = "l" THEN [1] ELSE [] END | MERGE (s)-[:LOCALIZES]->(t))
            FOREACH(ignoreMe IN CASE WHEN split(row.metaedge,"")[1] = "a" THEN [1] ELSE [] END | MERGE (s)-[:ASSOCIATES]->(t))
            FOREACH(ignoreMe IN CASE WHEN split(row.metaedge,"")[1] = "i" THEN [1] ELSE [] END | MERGE (s)-[:INTERACTS]->(t))
            FOREACH(ignoreMe IN CASE WHEN split(row.metaedge,"")[1] = "c" THEN [1] ELSE [] END | MERGE (s)-[:COVARIES]->(t))
            FOREACH(ignoreMe IN CASE WHEN split(row.metaedge,"")[1] = "e" THEN [1] ELSE [] END | MERGE (s)-[:EXPRESSES]->(t))
            FOREACH(ignoreMe IN CASE WHEN row.metaedge = "CrC" THEN [1] ELSE [] END | MERGE (s)-[:RESEMBLES]->(t))
            FOREACH(ignoreMe IN CASE WHEN row.metaedge = "Gr>G" THEN [1] ELSE [] END | MERGE (s)-[:REGULATES]->(t))
            FOREACH(ignoreMe IN CASE WHEN row.metaedge = "DrD" THEN [1] ELSE [] END | MERGE (s)-[:RESEMBLES]->(t));'''
        ).single()

def import_edges_mongo():
    with open("dataset/edges.tsv") as tsvfile:
        reader = csv.DictReader(tsvfile, dialect='excel-tab')
        parsed_rows = {
            "CpD":{
                "gen_update":lambda source, target: parsed_rows["CpD"]["updates"].append(
                    UpdateOne({ "id" : target }, {'$push': {'palliates_ids': source }})
                ),
                "updates":[]
            },
            "CtD":{
                "gen_update":lambda source, target: parsed_rows["CtD"]["updates"].append(
                    UpdateOne({ "id" : target }, {'$push': {'treats_ids': source }})
                ),
                "updates":[]
            },
            "DlA":{
                "gen_update":lambda source, target: parsed_rows["DlA"]["updates"].append(
                    UpdateOne({ "id" : source }, {'$push': {'localizes_ids': target }})
                ),
                "updates":[]
            },
            "DuG":{
                "gen_update":lambda source, target: parsed_rows["DuG"]["updates"].append(
                    UpdateOne({ "id" : source }, {'$push': {'upregulates_ids': target }})
                ),
                "updates":[]
            },
            "DdG":{
                "gen_update":lambda source, target: parsed_rows["DdG"]["updates"].append(
                    UpdateOne({ "id" : source }, {'$push': {'downregulates_ids': target }})
                ),
                "updates":[]
            },
            "DaG":{
                "gen_update":lambda source, target: parsed_rows["DaG"]["updates"].append(
                    UpdateOne({ "id" : source }, {'$push': {'associates_ids': target }})
                ),
                "updates":[]
            },
        }
        row_count = 0
        for row in reader:
            edge = row["metaedge"]
            if edge in parsed_rows:
                parsed_rows[edge]["gen_update"](row["source"], row["target"])
                row_count+=1
            if row_count == 10000:
                for edge in parsed_rows:
                    if len(parsed_rows[edge]["updates"]) > 0:
                        hetio_db["diseases"].bulk_write(parsed_rows[edge]["updates"])
                        parsed_rows[edge]["updates"] = []
                row_count = 0
        for edge in parsed_rows:
            if len(parsed_rows[edge]["updates"]) > 0:
                hetio_db["diseases"].bulk_write(parsed_rows[edge]["updates"])

@click.group()
def main():
    """
    CLI for Project 1
    """
    pass

@main.command()
def import_data():
    """
    Import a tsv file into all databases
    """
    click.echo("Importing Nodes...")
    import_nodes_mongo()
    import_nodes_neo4j()
    click.echo("Importing Edges...")
    import_edges_mongo()
    import_edges_neo4j()
    click.echo("Done")

@main.command()
def clear_data():
    """
    Resets all databases
    """
    click.echo("Clearing Data...")
    reset_data()
    click.echo("Done")

@main.command()
@click.argument('query')
def get_disease(query):
    """
    MongoDB: Drug names that can treat or palliate this disease, gene names that cause this disease, and where this disease occurs.
    """
    result = list(hetio_db["diseases"].aggregate([
        {"$match":{"id":query}},
        {"$limit":1},
        {"$lookup":{"from":"anatomy","localField":"localizes_ids","foreignField":"id","as":"localizes"}},
        {"$lookup":{"from":"compounds","localField":"palliates_ids","foreignField":"id","as":"palliates"}},
        {"$lookup":{"from":"compounds","localField":"treats_ids","foreignField":"id","as":"treats"}},
        {"$lookup":{"from":"genes","localField":"upregulates_ids","foreignField":"id","as":"upregulates"}},
        {"$lookup":{"from":"genes","localField":"downregulates_ids","foreignField":"id","as":"downregulates"}},
        {"$lookup":{"from":"genes","localField":"associates_ids","foreignField":"id","as":"associates"}},
        {"$project":{"_id":0,"name":1,"localizes.name":1,"associates.name":1,"treats.name":1,"upregulates.name":1,"downregulates.name":1,"palliates.name":1}}
    ]))
    if len(result) > 0:
        click.echo(pprint.pprint(result[0]))
    else:
        click.echo("Disease not found")

@main.command()
@click.argument('id')
def drugs_for_new_disease(id):
    """
    Neo4j: Find all drugs that can treat a new disease.
    """
    with neo4j_driver.session() as session:
        result = session.run(
                '''
                
                ;'''
        ).single().value()
    click.echo(id)


if __name__ == "__main__":
    main()