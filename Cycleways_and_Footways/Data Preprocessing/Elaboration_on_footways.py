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
    parser = argparse.ArgumentParser(description='Data elaboration of footways.')
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
    footways = json.load(f)
    df_footways = pd.DataFrame(footways['data'])
    df_footways['geometry'] = df_footways['geometry'].apply(wkt.loads)
    gdf_footways = gpd.GeoDataFrame(df_footways, crs='epsg:3035')
    gdf_footways.drop('index', axis=1, inplace=True)
    return gdf_footways


def insert_id_num(gdf_footways):
    l_ids = [x for x in range(gdf_footways.shape[0])]    
    gdf_footways.insert(2, 'id_num', l_ids)


def find_touched_footways(gdf_footways):
    list_touched_footways = []
    for i in range(gdf_footways.shape[0]):
        list_touched_footways.append([])

    gdf_footways['touched_footways'] = list_touched_footways

    gdf_footways.to_crs(epsg=3035, inplace=True)

    s = gdf_footways['geometry']

    for index, r in gdf_footways.iterrows():    
        polygon = r['geometry']
        print(index)
        l = list(s.sindex.query(polygon, predicate="intersects"))
        for i in l:
            gdf_footways[gdf_footways['id_num'] == r.id_num]['touched_footways'].iloc[0].append(gdf_footways.iloc[i].id_num)
        
        l1 = list(s.sindex.query(polygon, predicate="touches"))
        for i in l1:
            gdf_footways[gdf_footways['id_num'] == r.id_num]['touched_footways'].iloc[0].append(gdf_footways.iloc[i].id_num)
    



def find_closest_footways(gdf_footways):
    list_closest_footways = []
    for i in range(gdf_footways.shape[0]):
        list_closest_footways.append([])

    gdf_footways['closest_footways'] = list_closest_footways

    s = gdf_footways['geometry']

    for index, r in gdf_footways.iterrows():    
        polygon = r['geometry']
        l_dist = list(s.distance(polygon))
    
        for i in range(len(l_dist)):
            if l_dist[i] <= 20 and l_dist[i] > 0.5:
                gdf_footways[gdf_footways['id_num'] == r.id_num]['closest_lanes'].iloc[0].append(
                                                                        (gdf_footways.iloc[i].id_num, l_dist[i]))
                                                            


def save_gdf(gdf_footways, path):
    gdf_footways.to_crs(epsg=4326, inplace=True)
    df_footways = pd.DataFrame(gdf_footways)
    df_footways['geometry'] = df_footways['geometry'].astype(str)
    df_footways.to_json(path + "footways.json", orient='table')




def main(args=None):
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    path = greeter.get_path()[0][0] + '\\' + greeter.get_import_folder_name()[0][0] + '\\' 

    gdf_footways = read_file(path + options.file_name)
    gdf_footways['length'] = gdf_footways['geometry'].length

    insert_id_num(gdf_footways)
    print("Insertion of id_num : done")

    find_touched_footways(gdf_footways)
    print("Find footways that touch each other : done")

    find_closest_footways(gdf_footways)
    print("Find footways that are close to each other, but bot in contact: done")
    
    save_gdf(gdf_footways, path + "footways.json")



main()