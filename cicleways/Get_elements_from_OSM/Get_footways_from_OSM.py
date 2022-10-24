from neo4j import GraphDatabase
import json
import argparse
import os
import geopandas as gpd
import pandas as pd
import requests

#this code import from OSM the data regarding footways 
#and generates a filein the path of the graph database 
#(but do not generates nodes in the database)

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
    parser = argparse.ArgumentParser(description='Insertion of CROSSING NODES in the graph.')
    parser.add_argument('--neo4jURL', '-n', dest='neo4jURL', type=str,
                        help="""Insert the address of the local neo4j instance. For example: neo4j://localhost:7687""",
                        required=True)
    parser.add_argument('--neo4juser', '-u', dest='neo4juser', type=str,
                        help="""Insert the name of the user of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--neo4jpwd', '-p', dest='neo4jpwd', type=str,
                        help="""Insert the password of the local neo4j instance.""",
                        required=True)
    parser.add_argument('--latitude', '-x', dest='lat', type=float,
                        help="""Insert latitude of city center""",
                        required=True)
    parser.add_argument('--longitude', '-y', dest='lon', type=float,
                        help="""Insert longitude of city center""",
                        required=True)
    parser.add_argument('--distance', '-d', dest='dist', type=float,
                        help="""Insert distance (in meters) of the area to be cover""",
                        required=True)
    return parser


def elem_to_feature(elem):
   return {
        "geometry": {
                "type": "LineString",
                "coordinates": [[d["lon"], d["lat"]] for d in elem["geometry"]]
        },
        "properties": elem["tags"] ,
    }


def save_gdf(gdf, path):
    gdf.to_crs(epsg=4326, inplace=True)
    df = pd.DataFrame(gdf)
    df['geometry'] = df['geometry'].astype(str)
    df.to_json(path + "footways.json", orient='table')


def main(args=None):
    argParser = add_options()
    options = argParser.parse_args(args=args)
    dist = options.dist
    lon = options.lon
    lat = options.lat
    greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
    url = 'http://overpass-api.de/api/interpreter'
    query = f"""[out:json][timeout:1000];
                            (
                            way(around:{dist},{lat},{lon})[highway="footway"]->.all;
                            way(around:{dist},{lat},{lon})[highway="path"]->.all;
                            way(around:{dist},{lat},{lon})[highway="pedestrian"]->.all;
                            way(around:{dist},{lat},{lon})[footway="sidewalk"]->.all;
                            way(around:{dist},{lat},{lon})[foot="yes"]->.all; 
                            way(around:{dist},{lat},{lon})[foot="designated"]->.all;
                            );
                            out geom;
                           """
    

    result = requests.get(url, params={'data': query})
    data = result.json()['elements']
    features = [elem_to_feature(elem) for elem in data]
    gdf = gpd.GeoDataFrame.from_features(features)
    list_ids = ["way/"+str(elem["id"]) for elem in data]
    gdf.insert(0, 'id', list_ids)    

    path = greeter.get_path()[0][0] + '\\' + greeter.get_import_folder_name()[0][0] + '\\'
    
    save_gdf(gdf, path)
    print("Storing footways: done")
    return 0

main()
