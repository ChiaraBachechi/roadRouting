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


"""In this file we are going to make some preprocessing in order to find
   relations between cycling paths and pedestrian paths
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

    parser = argparse.ArgumentParser(description='Data elaboration of footways and cycleways.')
    parser.add_argument('--neo4jURL', '-n', dest='neo4jURL', type=str,
                        help="""Insert the address of the local neo4j instance. For example: neo4j://localhost:7687""",
                        required=True)
    parser.add_argument('--neo4juser', '-u', dest='neo4juser', type=str,
                        help="""Insert the name of the user of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--neo4jpwd', '-p', dest='neo4jpwd', type=str,
                        help="""Insert the password of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--nameFilecycleways', '-fc', dest='file_name_cycleways', type=str,
                        help="""Insert the name of the .json file containing the cycleways.""",
                        required=True)
    parser.add_argument('--nameFileFootways', '-ff', dest='file_name_footways', type=str,
                        help="""Insert the name of the .json file containing the footways.""",
                        required=True)
    return parser


def read_file(path):
    """read the file specified by the path"""

    f = open(path)
    json_file = json.load(f)
    df = pd.DataFrame(json_file['data'])
    df['geometry'] = df['geometry'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(df, crs='epsg:3035')
    gdf.drop('index', axis=1, inplace=True)
    return gdf


def find_cycleways_touching_and_close_to_footways(gdf_footways, gdf_cycleways):
    """Find cycleways and footways that touch or intersect each other or are reachable by crossing
       the road where the crossing is not signaled.
    """

    gdf_footways.to_crs(epsg=3035, inplace=True)
    gdf_cycleways.to_crs(epsg=3035, inplace=True)

    s2 = gdf_cycleways['geometry']

    list_lanes = []
    for i in range(gdf_footways.shape[0]):
        list_lanes.append([])

    gdf_footways['closest_lanes'] = list_lanes
    gdf_footways['touched_lanes'] = list_lanes


    for index, r in gdf_footways.iterrows(): 
        polygon = r['geometry']
        l_dist = list(s2.distance(polygon))
        for i in range(len(l_dist)):
        
            if l_dist[i] == 0:
                gdf_footways[gdf_footways['id_num'] == r.id_num]['touched_lanes'].iloc[0].append(gdf_cycleways.iloc[i].id_num)
            elif l_dist[i] <= 9 and l_dist[i] > 0:
                gdf_footways[gdf_footways['id_num'] == r.id_num]['closest_lanes'].iloc[0].append((
                                                                gdf_cycleways.iloc[i].id_num, l_dist[i]))

    
"""
def compute_distance(geom, footway, gdf_cycleways):
    dist = 0
    l_dist = []
    l_cycleways = []
    l_ids = list(footway['closest_lanes'])
    s2 = gpd.GeoSeries(geom)
    dist += geom.length
    print("Distanza di partenza : " + str(dist))
    for idx in l_ids:
        polygon = gdf_cycleways[gdf_cycleways['id_num'] == idx]['geometry'].iloc[0]
        l_dist.append(list(s2.distance(polygon)))
        
    
    for i in range(2):
        idx = l_dist.index(min(l_dist))
        l_cycleways.append(l_ids[idx])
        dist += l_dist[idx][0]
        l_dist.remove(min(l_dist))
        l_ids.remove(l_ids[idx])
        
    if dist - s2.iloc[0].length > 10:
        return 
    
    t1 = (int(l_cycleways[1]), dist, footway['id_num'])
    gdf_cycleways[gdf_cycleways['id_num'] == int(l_cycleways[0])]['triple'].iloc[0].append(t1)
    
    t2 = (int(l_cycleways[0]), dist, footway['id_num'])
    gdf_cycleways[gdf_cycleways['id_num'] == int(l_cycleways[1])]['triple'].iloc[0].append(t2)



def compute_footways_as_crossings(gdf_footways, gdf_cycleways):
    gdf_footways.to_crs(epsg=3035, inplace=True)
    gdf_cycleways.to_crs(epsg=3035, inplace=True)

    list_triple = []
    for i in range(gdf_cycleways.shape[0]):
        list_triple.append([])

    gdf_cycleways['triple'] = list_triple
    
    for index, footway in gdf_footways.iterrows():

        if len(footway['touched_lanes']) < 2:
            continue
        l_polygons = []
        s = footway['geometry']
        
        for idx in footway['touched_lanes']:
            polygon = gdf_cycleways[gdf_cycleways['id_num'] == idx]['geometry'].buffer(2.6).iloc[0]
            l_polygons.append(polygon)
        
        polygons=unary_union(l_polygons)
        s = s.difference(polygons)
        
        if s.is_empty:
            continue
            
        if s.geom_type == 'MultiLineString' or s.geom_type == 'GeometryCollection': 
            for geom in s.geoms:
                compute_distance(geom, footway, gdf_cycleways)
        else:
            compute_distance(s, footway, gdf_cycleways)
"""





def save_gdf(gdf, path):
    """save the geopandas DataFrame in a json file"""

    gdf.to_crs(epsg=4326, inplace=True)
    df = pd.DataFrame(gdf)
    df['geometry'] = df['geometry'].astype(str)
    df.to_json(path, orient='table')




def main(args=None):

    """Parsing input parameters"""
    argParser = add_options()
    options = argParser.parse_args(args=args)
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    path = greeter.get_path()[0][0] + '\\' + greeter.get_import_folder_name()[0][0] + '\\'

    """Read the content of the json file, store it in a geodataframe and apply the preprocessing"""
    gdf_footways = read_file(path + options.file_name_footways)
    gdf_cycleways = read_file(path + options.file_name_cycleways)

    find_cycleways_touching_and_close_to_footways(gdf_footways, gdf_cycleways)
    print("Find the cycleways that touch the footways : done")

    #compute_footways_as_crossings(gdf_footways, gdf_cycleways)
    #print("Compute the footways that also act as crossings between cycleways : done")

    save_gdf(gdf_footways, path + "footways.json")
    save_gdf(gdf_cycleways, path + "cycleways.json")



if __name__ == "__main__":
    main()