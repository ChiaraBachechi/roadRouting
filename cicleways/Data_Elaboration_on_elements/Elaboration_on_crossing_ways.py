from neo4j import GraphDatabase
import argparse
import os
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import json
from shapely import wkt

#insert ids in crossing nodes and generate the json files with WKT geometries

class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def get_path(self):
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
    parser = argparse.ArgumentParser(description='Data Elaboration of crossing ways.')
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
                        help="""Insert the name of the .geojson file.""",
                        required=True)
    return parser


def read_file(path):
    f = open(path)
    crossing_ways = json.load(f)
    df_crossing_ways = pd.DataFrame(crossing_ways['data'])
    df_crossing_ways['geometry'] = df_crossing_ways['geometry'].apply(wkt.loads)
    gdf_crossing_ways = gpd.GeoDataFrame(df_crossing_ways, crs='epsg:3035')
    gdf_crossing_ways.drop('index', axis=1, inplace=True)
    return gdf_crossing_ways


def insert_id_num(gdf_crossing_ways):
    l_ids = [x for x in range(gdf_crossing_ways.shape[0])]
    gdf_crossing_ways.insert(2, 'id_num', l_ids)
    return gdf_crossing_ways


def save_gdf(gdf_crossing_ways, path):
    gdf_crossing_ways.to_crs(epsg=4326, inplace=True)
    df_crossing_ways = pd.DataFrame(gdf_crossing_ways)
    df_crossing_ways['geometry'] = df_crossing_ways['geometry'].astype(str)
    df_crossing_ways.to_json(path + "crossing_ways.json", orient='table')


def main(args=None):
    #find the folder where the file containing the crossings is located
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    path = greeter.get_path()[0][0] + '\\' + greeter.get_import_folder_name()[0][0] + '\\'
    
    #open the file and generate a geopandas dataframe with WKT geoemtries 
    #converting the CRS to produce measures in meters
    gdf_crossing_ways =  read_file(path + options.file_name)
    
    #inserting the id in the dataframe and save the file as a json file
    gdf_crossing_ways = insert_id_num(gdf_crossing_ways)
    print("Insertion of id_num : done")

    save_gdf(gdf_crossing_ways, path)

main()

