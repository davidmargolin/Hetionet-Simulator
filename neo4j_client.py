import os
import shutil
from neo4j import GraphDatabase
from dotenv import load_dotenv
load_dotenv()

# neo4j connection
neo4j_driver = GraphDatabase.driver(os.getenv("NEO4J_URL"), auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD")), encrypted=False)
neo4j_data_path = "{}/dataset".format(os.getenv("NEO4J_IMPORT_PATH"))

def get_compounds():
    with neo4j_driver.session() as session:
        return session.run(
                '''
                
                ;'''
        ).single().value()

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
