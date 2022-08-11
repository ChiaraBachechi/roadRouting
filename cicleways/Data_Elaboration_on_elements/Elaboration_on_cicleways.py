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
    parser = argparse.ArgumentParser(description='Data elaboration of cicleways.')
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
    f = open(path)
    cicleways = json.load(f)
    df_cicleways = pd.DataFrame(cicleways['data'])
    df_cicleways['geometry'] = df_cicleways['geometry'].apply(wkt.loads)
    gdf_cicleways = gpd.GeoDataFrame(df_cicleways, crs='epsg:3035')
    gdf_cicleways.drop('index', axis=1, inplace=True)
    return gdf_cicleways


def insert_id_num(gdf_cicleways):
    l_ids = [x for x in range(gdf_cicleways.shape[0])]    
    gdf_cicleways.insert(2, 'id_num', l_ids)

def compute_length(gdf_cicleways):
    gdf_cicleways.to_crs(epsg=3035)
    gdf_cicleways['length'] = gdf_cicleways['geometry'].length/2


def find_touched_lanes(gdf_cicleways):
    s = gdf_cicleways['geometry']


    list_touched_lanes = []
    for index, r in gdf_cicleways.iterrows():
        polygon = r['geometry']
        l_dist = list(s.distance(polygon))
        l1 = []
        for i in range(len(l_dist)):
            if l_dist[i] == 0:
                l1.append(gdf_cicleways.iloc[i].id_num)
        list_touched_lanes.append(l1)

    gdf_cicleways['touched_lanes'] = list_touched_lanes


def find_closest_lanes(gdf_cicleways):
    list_closest_lanes = []
    for i in range(gdf_cicleways.shape[0]):
        list_closest_lanes.append([])

    gdf_cicleways['closest_lanes'] = list_closest_lanes

    s = gdf_cicleways['geometry']

    for index, r in gdf_cicleways.iterrows():    
        polygon = r['geometry']
        l_dist = list(s.distance(polygon))
    
        for i in range(len(l_dist)):
            if l_dist[i] <= 20 and l_dist[i] > 0.5:
                gdf_cicleways[gdf_cicleways['id_num'] == r.id_num]['closest_lanes'].iloc[0].append(
                                                                        (gdf_cicleways.iloc[i].id_num, l_dist[i]))

    


def save_gdf(gdf_cicleways, path):
    gdf_cicleways.to_crs(epsg=4326, inplace=True)
    df_cicleways = pd.DataFrame(gdf_cicleways)
    df_cicleways['geometry'] = df_cicleways['geometry'].astype(str)
    df_cicleways.to_json(path + "cicleways.json", orient='table')




def main(args=None):
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    path = greeter.get_path()[0][0] + '\\' + greeter.get_import_folder_name()[0][0] + '\\' 

    gdf_cicleways = read_file(path + options.file_name)
    insert_id_num(gdf_cicleways)
    print("Insertion of id_num : done")

    compute_length(gdf_cicleways)
    print("Compute the length of the cicleways : done")

    find_touched_lanes(gdf_cicleways)
    print("Find cicleways that touch each other : done")

    find_closest_lanes(gdf_cicleways)
    print("Find cicleways that are close to each other, but bot in contact: done")
    
    save_gdf(gdf_cicleways, path + "cicleways.json")



main()