import os
import shutil
from neo4j import GraphDatabase
from dotenv import load_dotenv
load_dotenv()

# neo4j connection
neo4j_driver = GraphDatabase.driver(os.getenv("NEO4J_URL"), auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD")), encrypted=False)

def find_missing_compounds(disease_id):
    with neo4j_driver.session() as session:
        return session.run(
            '''MATCH (similar:Compound)-[:RESEMBLES]->(com:Compound)-[:TREATS]->(d:Disease{id:$disease_id})
            WHERE NOT (similar)-[:TREATS]->(d)
            and (exists((similar)-[:UPREGULATES]->(:Gene)<-[:DOWNREGULATES]-(:Anatomy)<-[:LOCALIZES]-(d))
            OR exists((similar)-[:DOWNREGULATES]->(:Gene)<-[:UPREGULATES]-(:Anatomy)<-[:LOCALIZES]-(d)))
            return (similar);''',
            disease_id=disease_id
        ).value()

def reset_data():
    # reset neo4j
    with neo4j_driver.session() as session:
        session.run(
            '''MATCH (n)
            DETACH DELETE n;'''
        ).single()

def import_nodes():
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

def import_edges():
    shutil.copyfile("dataset/edges.tsv", "{}/edges.tsv".format(os.getenv("NEO4J_IMPORT_PATH")))
    with neo4j_driver.session() as session:
        session.run(
            '''USING PERIODIC COMMIT 5000
            LOAD CSV WITH HEADERS FROM 'file:///edges.tsv' AS row
            FIELDTERMINATOR '\t'
            WITH row WHERE row.metaedge IN ["CuG","AuG","CdG","AdG","CrC","CtD","DlA"]
            MATCH (s:Data{id: row.source})
            MATCH (t:Data{id: row.target})
            FOREACH(ignoreMe IN CASE WHEN row.metaedge = "CuG" THEN [1] ELSE [] END | MERGE (s)-[:UPREGULATES]->(t))
            FOREACH(ignoreMe IN CASE WHEN row.metaedge = "AuG" THEN [1] ELSE [] END | MERGE (s)-[:UPREGULATES]->(t))
            FOREACH(ignoreMe IN CASE WHEN row.metaedge = "CdG" THEN [1] ELSE [] END | MERGE (s)-[:DOWNREGULATES]->(t))
            FOREACH(ignoreMe IN CASE WHEN row.metaedge = "AdG" THEN [1] ELSE [] END | MERGE (s)-[:DOWNREGULATES]->(t))
            FOREACH(ignoreMe IN CASE WHEN row.metaedge = "CrC" THEN [1] ELSE [] END | MERGE (s)<-[:RESEMBLES]->(t))
            FOREACH(ignoreMe IN CASE WHEN row.metaedge = "CtD" THEN [1] ELSE [] END | MERGE (s)-[:TREATS]->(t))
            FOREACH(ignoreMe IN CASE WHEN row.metaedge = "DlA" THEN [1] ELSE [] END | MERGE (s)-[:LOCALIZES]->(t));'''
        ).single()
