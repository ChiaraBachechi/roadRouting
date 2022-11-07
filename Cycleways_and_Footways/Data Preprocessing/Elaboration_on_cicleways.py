from neo4j import GraphDatabase
import argparse
import os
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import json
from shapely import wkt

"""In this file we are going to make some preprocessing in order to find
   relations between cycleways
"""


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

    parser = argparse.ArgumentParser(description='Data elaboration of cycleways.')
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
    cycleways = json.load(f)
    df_cycleways = pd.DataFrame(cycleways['data'])
    df_cycleways['geometry'] = df_cycleways['geometry'].apply(wkt.loads)
    gdf_cycleways = gpd.GeoDataFrame(df_cycleways, crs='epsg:3035')
    gdf_cycleways.drop('index', axis=1, inplace=True)
    return gdf_cycleways


def insert_id_num(gdf_cycleways):
    "Add a progressive integer id to the cycleways"

    l_ids = [x for x in range(gdf_cycleways.shape[0])]    
    gdf_cycleways.insert(2, 'id_num', l_ids)


def compute_length(gdf_cycleways):
    """Compute the length of the whole cycling path"""

    gdf_cycleways.to_crs(epsg=3035)
    gdf_cycleways['length'] = gdf_cycleways['geometry'].length/2


def compute_danger(gdf_cycleways):
    """Convert in an integer value the classification attribute"""

    d = {'lontano dal traffico' : 1, 'fisicamente protetto' : 2, 'vicino alla strada' : 3, 'vicino al traffico (L)' : 4, 
    'vicino al traffico (V)': 5}

    gdf_cycleways['classifica'].fillna('vicino alla strada', inplace=True)

    list_danger = []
    for _, r in gdf_cycleways.iterrows():
        print(r['classifica'])
        list_danger.append(d[r['classifica']])

    gdf_cycleways['pericolosità'] = list_danger


def find_touched_lanes(gdf_cycleways):
    """Find cycleways that are touching or intersecting the current one"""
    gdf_cycleways.to_crs(epsg=3035, inplace=True)

    list_touched_lanes = []
    for i in range(gdf_cycleways.shape[0]):
        list_touched_lanes.append([])

    gdf_cycleways['touched_lanes'] = list_touched_lanes

    s = gdf_cycleways['geometry']

    for index, r in gdf_cycleways.iterrows():    
        polygon = r['geometry']
        l = list(s.sindex.query(polygon, predicate="intersects"))
        for i in l:
            gdf_cycleways[gdf_cycleways['id_num'] == r.id_num]['touched_lanes'].iloc[0].append(gdf_cycleways.iloc[i].id_num)
        
        l1 = list(s.sindex.query(polygon, predicate="touches"))
        for i in l1:
            gdf_cycleways[gdf_cycleways['id_num'] == r.id_num]['touched_lanes'].iloc[0].append(gdf_cycleways.iloc[i].id_num)


def find_closest_lanes(gdf_cycleways):
    """Find cycleways that are reachable by crossing the road where the crossing is not signaled"""
    gdf_cycleways.to_crs(epsg=3035, inplace=True)

    list_closest_lanes = []
    for i in range(gdf_cycleways.shape[0]):
        list_closest_lanes.append([])

    gdf_cycleways['closest_lanes'] = list_closest_lanes

    s = gdf_cycleways['geometry']

    for index, r in gdf_cycleways.iterrows():    
        polygon = r['geometry']
        l_dist = list(s.distance(polygon))
    
        for i in range(len(l_dist)):
            if l_dist[i] <= 20 and l_dist[i] > 0:
                gdf_cycleways[gdf_cycleways['id_num'] == r.id_num]['closest_lanes'].iloc[0].append(
                                                                        (gdf_cycleways.iloc[i].id_num, l_dist[i]))

    


def save_gdf(gdf_cycleways, path):
    """save the geopandas DataFrame in a json file"""

    gdf_cycleways.to_crs(epsg=4326, inplace=True)
    df_cycleways = pd.DataFrame(gdf_cycleways)
    df_cycleways['geometry'] = df_cycleways['geometry'].astype(str)
    df_cycleways.to_json(path + "cycleways.json", orient='table')


def preprocessing(gdf_cycleways):
    insert_id_num(gdf_cycleways)
    print("Insertion of id_num : done")

    compute_length(gdf_cycleways)
    print("Compute the length of the cycleways : done")

    compute_danger(gdf_cycleways)
    print("Compute danger of the cycleways: done")

    find_touched_lanes(gdf_cycleways)
    print("Find cycleways that touch each other : done")

    find_closest_lanes(gdf_cycleways)
    print("Find cycleways that are close to each other, but not in contact: done")




def main(args=None):
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    path = greeter.get_path()[0][0] + '\\' + greeter.get_import_folder_name()[0][0] + '\\' 

    gdf_cycleways = read_file(path + options.file_name)
    preprocessing(gdf_cycleways)
    save_gdf(gdf_cycleways, path + "cycleways.json")




#main()