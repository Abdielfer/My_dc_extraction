'''
Modified on May 18 : Ability to give a geopackge or geojson for filter of asset 
'''
import pathlib
import sys
import geopandas as gpd
import json
root = pathlib.Path(__file__).parents[2]

if str(root) not in sys.path:
    sys.path.insert(0,str(root))

import ccmeo_datacube.extract as dce
def test_it():
    
    dex = dce.DatacubeExtract()
    collections_asset = dex.collection_str_to_df('hrdem-lidar:dtm')
    
    # df = dce.asset_urls(collection_asset = collections_asset,
    #                     bbox = '1775625.8392919037,-61408.370247168554,1896969.1451886918,86894.29517093688',
    #                     bbox_crs = 'EPSG:3979')
    
    # geom = gpd.read_file(r'C:\Users\ccrevier\Documents\Datacube\Temp\dev_dc_extract\issue\128\bbox_output_3979.geojson')
    geom = gpd.read_file(r'C:\Users\ccrevier\Documents\Datacube\Temp\dev_dc_extract\issue\128\splitted.gpkg')

    df_from_geopackage = dce.asset_urls(collection_asset = collections_asset,
                        geom = geom)
    print(df_from_geopackage)

    # print(df.columns)
    # print(df.item_resolution)
    # print(df)

if __name__ == '__main__':
    test_it()
