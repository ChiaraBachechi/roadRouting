import pandas as pd
import geopandas as gpd

"""This file contains some functions useful to the data extraction process"""

def save_gdf(gdf, path, filename):
    """save the GeoPandas Dataframe in a json file"""

    gdf.to_crs(epsg=4326, inplace=True)
    df = pd.DataFrame(gdf)
    df['geometry'] = df['geometry'].astype(str)
    df.to_json(path + filename, orient='table')



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