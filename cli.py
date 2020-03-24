import click
import csv
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
cluster = Cluster(auth_provider=auth_provider)
session = cluster.connect()

def init_keyspace():
    session.execute("CREATE KEYSPACE IF NOT EXISTS project_1 WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
    session.execute("USE project_1")

def import_nodes():
    kinds = {}
    with open("dataset/nodes.tsv") as tsvfile:
        reader = csv.DictReader(tsvfile, dialect='excel-tab')
        for row in reader:
            if row["kind"] in kinds:
                kinds[row["kind"]] += 1
            else:
                kinds[row["kind"]] = 0
                session.execute(   
                    "CREATE TABLE IF NOT EXISTS " + row["kind"] + "(id text PRIMARY KEY, name text)"
                )
            session.execute(
                "INSERT INTO " + row["kind"] + " (id, name) VALUES (%s, %s)",
                (row["id"].split(":")[-1], row["name"])
            )
    click.echo(kinds)

def import_edges():
    metaedges = {}
    with open("dataset/edges.tsv") as tsvfile:
        reader = csv.DictReader(tsvfile, dialect='excel-tab')
        for row in reader:
            if row["metaedge"] in metaedges:
                metaedges[row["metaedge"]] += 1
            else:
                metaedges[row["metaedge"]] = 0
    click.echo(metaedges)

@click.group()
def main():
    """
    CLI for Project 1
    """
    init_keyspace()
    pass

@main.command()
def import_data():
    """
    Import a tsv file into all databases
    """
    click.echo("Importing Nodes...")
    import_nodes()
    click.echo("Importing Edges...")
    import_edges()    

@main.command()
@click.argument('query')
def get_disease(query):
    """
    MongoDB: Drug names that can treat or palliate this disease, gene names that cause this disease, and where this disease occurs.
    """
    click.echo(query)

@main.command()
@click.argument('id')
def drugs_for_new_disease(id):
    """
    Cassandra: Find all drugs that can treat a new disease.
    """
    click.echo(id)


if __name__ == "__main__":
    main()