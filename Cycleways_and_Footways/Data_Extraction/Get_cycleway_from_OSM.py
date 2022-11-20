#!/usr/bin/env python
# coding: utf-8

from neo4j import GraphDatabase
import json
import argparse
import os
import geopandas as gpd
import numpy as np
import pandas as pd
import requests
from Tools import *

"""Extract cycleways and roads where bicycles are allowed from OSM"""

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

	parser = argparse.ArgumentParser(description='Generation of the cycleway.json file in the input folder of neo4j from OSM data.')
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
						help="""Insert distance (in meters) of the area to be covered""",
						required=True)
	return parser
		
def elem_to_feature(elem, geomType):
	"""Convert the element in a json format"""

	if geomType == "LineString":
		return {
			"geometry": {
					"type": geomType,
					"coordinates": [[d["lon"], d["lat"]] for d in elem["geometry"]]
			},
			"properties": elem["tags"] ,
		}

	return {
		"geometry": {
			"type": geomType,
			"coordinates": [elem["lon"], elem["lat"]]
		},
		"properties": elem["tags"],
	}


# In[48]:


def classification(row):
	if(row['highway'] == 'track' or row['highway'] == 'path' or row['highway'] == 'footway' or row['highway'] == 'steps' or row['highway'] == 'pedestrian' ):
		return 'fisicamente protetto'
	if(row['highway'] == 'cycleway' or row['cycleway'] == 'track'):
		return 'fisicamente protetto'
	if(row['cycleway'] == 'lane'):
		return 'fisicamente protetto in sede stradale'
	if(row['maxspeed'] <= 30):
		return 'vicino al traffico (L)'
	else:
		if(row['maxspeed'] > 30):
			return 'vicino al traffico (V)'
		else:
			if(row['highway'] == 'residential' or row['highway'] == 'service' or row['highway'] == 'unclassified'):
				return 'vicino al traffico (L)'
			else:
				return 'vicino al traffico (V)'
		

def createQueryCycleways(dist, lat, lon):
	"""Create the query to fetch the data of interest"""

	query = f"""[out:json][timeout:1000];
								(
								way(around:{dist},{lat},{lon})[highway="cycleway"]->.all;
								way(around:{dist},{lat},{lon})[bicycle];
								way(around:{dist},{lat},{lon})[cycleway];
								);
								out geom;
							   """
	return query




def main(args=None):
	"""Parsing of input parameters"""
	argParser = add_options()
	options = argParser.parse_args(args=args)
	dist = options.dist
	lon = options.lon
	lat = options.lat
	greeter = App(options.neo4jURL, options.neo4juser, options.neo4jpwd)
	"""Employ overpass API to get data regarding cycleways"""
	url = 'http://overpass-api.de/api/interpreter'
	query = createQueryCycleways(dist, lat, lon)
	result = requests.get(url, params={'data': query})
	data = result.json()['elements']
	"""generating a geodataframe with line geometry"""
	features = [elem_to_feature(elem, "LineString") for elem in data]
	gdf = gpd.GeoDataFrame.from_features(features, crs=4326)
	"""inserting ID in the geodataframe"""
	list_ids = ["way/"+str(elem["id"]) for elem in data]
	gdf.insert(0, 'id', list_ids)
	path = greeter.get_path()[0][0] + '\\' + greeter.get_import_folder_name()[0][0] + '\\'
	"""filtering the roads where bicycles are not allowed"""
	gdf = gdf[gdf['bicycle'] != 'no']
	"""inserting ID_E for support witht he version employing also data from the ER geoportal"""
	gdf['ID_E'] = np.NaN
	df1 = gdf[['id','ID_E','highway','bicycle','foot','lanes','cycleway','segregated','maxspeed','geometry']]
	df1['maxspeed'] = df1['maxspeed'].astype(float)
	"""performing classification based on the tag values of OSM data"""
	df1['classifica'] = df1.apply(classification,axis = 1)
	"""Save the GeoDataframe in a json file"""
	save_gdf(df1, path, "cycleways.json")
	print("Storing cycleways: done")
	
if __name__ == "__main__":
	main()





