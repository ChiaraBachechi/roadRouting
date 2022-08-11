from neo4j import GraphDatabase
import argparse
import os
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import json
from shapely import wkt


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
    parser = argparse.ArgumentParser(description='Data elaboration of cicleways and crossing ways.')
    parser.add_argument('--neo4jURL', '-n', dest='neo4jURL', type=str,
                        help="""Insert the address of the local neo4j instance. For example: neo4j://localhost:7687""",
                        required=True)
    parser.add_argument('--neo4juser', '-u', dest='neo4juser', type=str,
                        help="""Insert the name of the user of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--neo4jpwd', '-p', dest='neo4jpwd', type=str,
                        help="""Insert the password of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--nameFileCrossings', '-fw', dest='file_name_crossings', type=str,
                        help="""Insert the name of the .json file of crossing ways.""",
                        required=True)
    parser.add_argument('--nameFileCicleways', '-fc', dest='file_name_cicleways', type=str,
                        help="""Insert the name of the .json file of cicleways.""",
                        required=True)
    return parser


def read_file(path):
    f = open(path)
    json_file = json.load(f)
    df = pd.DataFrame(json_file['data'])
    df['geometry'] = df['geometry'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(df, crs='epsg:3035')
    gdf.drop('index', axis=1, inplace=True)
    return gdf


def find_cicleways_close_to_crossing_ways(gdf_cicleways, gdf_crossing_ways):
    gdf_crossing_ways.to_crs(epsg=3035, inplace=True)

    s = gdf_cicleways['geometry']
    list_closest_lanes = []

    for index, r in gdf_crossing_ways.iterrows(): 
        
        polygon = r['geometry']
        l_dist = list(s.distance(polygon))
        l1 = []
        for i in range(len(l_dist)):
            if l_dist[i] <= 2:
                l1.append(gdf_cicleways.iloc[i].id_num)
        list_closest_lanes.append(l1)
    gdf_crossing_ways['closest_lanes'] = list_closest_lanes

    return gdf_crossing_ways



def save_gdf(gdf, path):
    gdf.to_crs(epsg=4326, inplace=True)
    df = pd.DataFrame(gdf)
    df['geometry'] = df['geometry'].astype(str)
    df.to_json(path , orient='table')




def main(args=None):
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    path = greeter.get_path()[0][0] + '\\' + greeter.get_import_folder_name()[0][0] + '\\' 

    gdf_cicleways = read_file(path + options.file_name_cicleways)
    gdf_crossing_ways = read_file(path + options.file_name_crossings)
    
    gdf_crossing_ways = find_cicleways_close_to_crossing_ways(gdf_cicleways, gdf_crossing_ways)
    print("Find crossing ways that are close or touching cicleways : done ")
    
    save_gdf(gdf_crossing_ways, path + "crossing_ways.json")



main()