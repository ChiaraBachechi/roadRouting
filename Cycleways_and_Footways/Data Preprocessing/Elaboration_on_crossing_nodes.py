from neo4j import GraphDatabase
import argparse
import os
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import json
from shapely import wkt


"""In this file we are going to make some preprocessing on crossing mapped as nodes"""


class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def get_path(self):
        """gets the path of the neo4j instance"""

        with self.driver.session() as session:
            result = session.write_transaction(self._get_path)
            return result

    @staticmethod
    def _get_path(tx):
        result = tx.run("""
                        Call dbms.listConfig() yield name,value where name = 'dbms.directories.neo4j_home' return value;
                    """)
        return result.values()
        
    def get_import_folder_name(self):
        """gets the path of the import folder of the neo4j instance"""

        with self.driver.session() as session:
            result = session.write_transaction(self._get_import_folder_name)
            return result

    @staticmethod
    def _get_import_folder_name(tx):
        result = tx.run("""
                        Call dbms.listConfig() yield name,value where name = 'dbms.directories.import' return value;
                    """)
        return result.values()


def add_options():
    """parameters to be used in order to run the script"""

    parser = argparse.ArgumentParser(description='Data elaboration of crossing nodes.')
    parser.add_argument('--neo4jURL', '-n', dest='neo4jURL', type=str,
                        help="""Insert the address of the local neo4j instance. For example: neo4j://localhost:7687""",
                        required=True)
    parser.add_argument('--neo4juser', '-u', dest='neo4juser', type=str,
                        help="""Insert the name of the user of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--neo4jpwd', '-p', dest='neo4jpwd', type=str,
                        help="""Insert the password of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--nameFile', '-f', dest='file_name', type=str,
                        help="""Insert the name of the .json file.""",
                        required=True)
    return parser


def read_file(path):
    """read the file specified by the path"""

    f = open(path)
    crossing_nodes = json.load(f)
    df_crossing_nodes = pd.DataFrame(crossing_nodes['data'])
    df_crossing_nodes['geometry'] = df_crossing_nodes['geometry'].apply(wkt.loads)
    gdf_crossing_nodes = gpd.GeoDataFrame(df_crossing_nodes, crs='epsg:3035')
    gdf_crossing_nodes.drop('index', axis=1, inplace=True)
    return gdf_crossing_nodes


def insert_id_num(gdf_crossing_nodes):
    "Add a progressive integer id to the crossings mapped as nodes"

    l_ids = [x for x in range(gdf_crossing_nodes.shape[0])]
    gdf_crossing_nodes.insert(2, 'id_num', l_ids)



def save_gdf(gdf_crossing_nodes, path):
    """save the geopandas DataFrame in a json file"""

    gdf_crossing_nodes.to_crs(epsg=4326, inplace=True)
    df_crossing_nodes = pd.DataFrame(gdf_crossing_nodes)
    df_crossing_nodes['geometry'] = df_crossing_nodes['geometry'].astype(str)
    df_crossing_nodes.to_json(path + "crossing_nodes.json", orient='table')


def preprocessing(gdf_crossing_nodes):
    insert_id_num(gdf_crossing_nodes)
    print("Insertion of id_num : done")


def main(args=None):

    """Parsing input parameters"""
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    path = greeter.get_path()[0][0] + '\\' + greeter.get_import_folder_name()[0][0] + '\\'

    """Read the content of the json file, store it in a geodataframe and apply the preprocessing"""
    gdf_crossing_nodes = gpd.read_file(path + options.file_name, crs={'init': 'epsg:4326'}, geometry='geometry')
    preprocessing(gdf_crossing_nodes)
    save_gdf(gdf_crossing_nodes, path)

#main()

