from neo4j import GraphDatabase
import argparse
import os
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import json
from shapely import wkt
from shapely.ops import unary_union

#exploring spatial relationships between footways and cycleways

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
    parser = argparse.ArgumentParser(description='Data elaboration of footways and cicleways.')
    parser.add_argument('--neo4jURL', '-n', dest='neo4jURL', type=str,
                        help="""Insert the address of the local neo4j instance. For example: neo4j://localhost:7687""",
                        required=True)
    parser.add_argument('--neo4juser', '-u', dest='neo4juser', type=str,
                        help="""Insert the name of the user of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--neo4jpwd', '-p', dest='neo4jpwd', type=str,
                        help="""Insert the password of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--nameFileCicleways', '-fc', dest='file_name_cicleways', type=str,
                        help="""Insert the name of the .json file containing the cicleways.""",
                        required=True)
    parser.add_argument('--nameFileFootways', '-ff', dest='file_name_footways', type=str,
                        help="""Insert the name of the .json file containing the footways.""",
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


def find_cicleways_touching_and_close_to_footways(gdf_footways, gdf_cicleways):
    #convert the CRS to measurements in meters
    gdf_footways.to_crs(epsg=3035, inplace=True)
    gdf_cicleways.to_crs(epsg=3035, inplace=True)

    s2 = gdf_cicleways['geometry']
    
    #non è chiaro cosa faccia questa parte di codice
    list_lanes = []
    for i in range(gdf_footways.shape[0]):
        list_lanes.append([])

    gdf_footways['closest_lanes'] = list_lanes
    gdf_footways['touched_lanes'] = list_lanes

    #add to closest lane attribute the cycleways that are in a radius major than 0.5m but lower than 9m
    #add to touched_lanes attribute the cycleqays that intersect the footways only in one point
    for index, r in gdf_footways.iterrows(): 
        print(index)
        polygon = r['geometry']
        l_dist = list(s2.distance(polygon))
        l1 = []
        for i in range(len(l_dist)):
        
            if l_dist[i] == 0:
                gdf_footways[gdf_footways['id_num'] == r.id_num]['touched_lanes'].iloc[0].append(gdf_cicleways.iloc[i].id_num)
            elif l_dist[i] <= 9 and l_dist[i] > 0.5:
                gdf_footways[gdf_footways['id_num'] == r.id_num]['closest_lanes'].iloc[0].append((
                                                                gdf_cicleways.iloc[i].id_num, l_dist[i]))

    return gdf_footways

def compute_distance(geom, footway, gdf_cicleways):
    dist = 0
    l_dist = []
    l_cicleways = []
    l_ids = list(footway['closest_lanes'])
    s2 = gpd.GeoSeries(geom)
    dist += geom.length
    print("Distanza di partenza : " + str(dist))
    for idx in l_ids:
        polygon = gdf_cicleways[gdf_cicleways['id_num'] == idx]['geometry'].iloc[0]
        l_dist.append(list(s2.distance(polygon)))
        
    
    print(l_dist)
    for i in range(2):
        idx = l_dist.index(min(l_dist))
        l_cicleways.append(l_ids[idx])
        dist += l_dist[idx][0]
        l_dist.remove(min(l_dist))
        l_ids.remove(l_ids[idx])
        
    print(footway['closest_lanes'])
    if dist - s2.iloc[0].length > 10:
        return 
    
    t1 = (int(l_cicleways[1]), dist, footway['id_num'])
    gdf_cicleways[gdf_cicleways['id_num'] == int(l_cicleways[0])]['triple'].iloc[0].append(t1)
    
    t2 = (int(l_cicleways[0]), dist, footway['id_num'])
    gdf_cicleways[gdf_cicleways['id_num'] == int(l_cicleways[1])]['triple'].iloc[0].append(t2)

    
    return


def compute_footways_as_crossings(gdf_footways, gdf_cicleways):
    #covert reference system to obtain measurements as meters
    gdf_footways.to_crs(epsg=3035, inplace=True)
    gdf_cicleways.to_crs(epsg=3035, inplace=True)
    
    
    list_triple = []
    for i in range(gdf_cicleways.shape[0]):
        list_triple.append([])

    gdf_cicleways['triple'] = list_triple
    #find the footways that acts like crossing
    for index, footway in gdf_footways.iterrows():
        print(index)
        print(footway['closest_lanes'])
        #if the number of cycleways that are located nearby 
        #are less than 2 the footway cannot be a crossing
        if len(footway['closest_lanes']) < 2:
            continue
        l_polygons = []
        s = footway['geometry']
        #if two between the cycleways locaed close to the footway are located in a radius of 2.6 meters 
        #from one to the other the footway is a crossing
        for idx in footway['closest_lanes']:
            polygon = gdf_cicleways[gdf_cicleways['id_num'] == idx]['geometry'].buffer(2.6).iloc[0]
            l_polygons.append(polygon)
        
        polygons=unary_union(l_polygons)
        s = s.difference(polygons)
        
        if s.is_empty:
            continue
        #compute the distance that correspond to the footway
        if s.geom_type == 'MultiLineString' or s.geom_type == 'GeometryCollection': 
            for geom in s.geoms:
                compute_distance(geom, footway, gdf_cicleways)
        else:
            compute_distance(s, footway, gdf_cicleways)
            





def save_gdf(gdf, path):
    gdf.to_crs(epsg=4326, inplace=True)
    df = pd.DataFrame(gdf)
    df['geometry'] = df['geometry'].astype(str)
    df.to_json(path , orient='table')




def main(args=None):
    #find the folder where the files containing the cycleways and the footways are located
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    path = greeter.get_path()[0][0] + '\\' + greeter.get_import_folder_name()[0][0] + '\\' 
    
    #read the json files of cycleways and footways generated by previous elaboration
    gdf_footways = read_file(path + options.file_name_footways)
    gdf_cicleways = read_file(path + options.file_name_cicleways)

    #find for each footway the cycleways that intersect it only in one point or are close to it
    gdf_footways = find_cicleways_touching_and_close_to_footways(gdf_footways, gdf_cicleways)
    print("Find the cicleways that touch the footways : done")
    
    #identify the footways that act like crossings beetween two cycleways
    compute_footways_as_crossings(gdf_footways, gdf_cicleways)
    print("Compute the footways that also act as crossings between cicleways : done")

    save_gdf(gdf_footways, path + "footways.json")
    save_gdf(gdf_cicleways, path + "cicleways.json")



main()
