import click
import pprint
import mongo_client
import neo4j_client
from dotenv import load_dotenv
load_dotenv()

@click.group()
def main():
    """
    CLI for Project 1
    """
    pass

@main.command()
def import_data():
    """
    Import a tsv files into all databases
    """
    click.echo("Importing Nodes...")
    mongo_client.import_nodes()
    neo4j_client.import_nodes()
    click.echo("Importing Edges...")
    mongo_client.import_edges()
    neo4j_client.import_edges()
    click.echo("Done")

@main.command()
def clear_data():
    """
    Resets all databases
    """
    click.echo("Clearing Data...")
    mongo_client.reset_data()
    neo4j_client.reset_data()
    click.echo("Done")

@main.command()
@click.argument('disease_id')
def get_disease(disease_id):
    """
    MongoDB: Disease name, drug names that can treat or palliate this disease, gene names that cause this disease, and where this disease occurs.
    """
    result = mongo_client.get_disease(disease_id)
    if len(result) > 0:
        click.echo(pprint.pprint(result[0]))
    else:
        click.echo("Disease not found")

@main.command()
@click.argument('disease_id')
def drugs_for_new_disease(disease_id):
    """
    Neo4j: Find all drugs that can treat a new disease.
    """
    results = neo4j_client.find_missing_compounds(disease_id)
    if len(results) > 0:
        for drug in results:
            click.echo(drug["name"])
    else:
        click.echo("Compounds not found")


if __name__ == "__main__":
    main()