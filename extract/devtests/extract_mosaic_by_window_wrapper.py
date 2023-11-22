# -*- coding: utf-8 -*-
"""
Created on Fri Apr 14 07:55:33 2023

@author: ccrevier

Code to test the fix used by Julien from CHN with a job file on HPC to validate if the scenario
extract + rastrerio.merge is better than the warped_mosaic used atm for big extent

Test that calls the extract_cog() and is run inside CLI with: 
    python -m memory_profiler test_extract_mosaic_chn.py
"""
import argparse
import pathlib
import sys
import rasterio
import os
from rasterio.shutil import copy as rscopy
import numpy


root = pathlib.Path(__file__).parents[2]
if str(root) not in sys.path:
    sys.path.insert(0,str(root))
    
import ccmeo_datacube.extract as dce    
import ccmeo_datacube.extract_cog as exc

@dce.print_time
def extract_mosaic_by_window(collections:str,
                            bbox:str,
                            bbox_crs:str,
                            resolution:int=None,
                            method:str='nearest',
                            out_crs:str=None,
                            out_dir:str=None,
                            suffix:str=None,
                            datetime_filter:str=None,
                            resolution_filter:str=None,
                            overviews:bool=False,
                            debug:bool=False,
                            mosaic:bool=False, 
                            orderby:str='date', 
                            desc:bool=True):
    #wrapper, resemble au extract_cog, mais juste pour les mosaic et le by window
    dex = dce.DatacubeExtract()
    #Create the empty list of output files
    out_files = []

    collections_asset = dex.collection_str_to_df(collections)
    
    # A pandas.DataFrame with url,collection_id,item_datetime,item_epsg from all asked collections
    df = dce.asset_urls(collections_asset, 
                        bbox, 
                        bbox_crs, 
                        resolution_filter=resolution_filter)
    
    # Get the list of all the input file resolutions from all collections
    list_resolutions = df.item_resolution.unique().tolist()
    
    # For each collection, generate minicube, mosaic or wcs based cog
    for x in collections_asset.iloc:
        collection = x['collection']
        asset = x['asset']
        str_col_asset = f" {collection}, {asset} "
        print(str_col_asset.center(80, '#'))


        # Create a cog_chip, minicube or mosaic from a cog
       
        if asset : 
            df_collection = df.query('collection_id == @collection and asset_key == @asset')#.loc[df['collection_id']==collection]#&df['asset_key']==asset]
        else:
            df_collection = df.query('collection_id == @collection')
        urls = list(df_collection.url)
        
        print(f'INFO : Clipping {len(urls)} items from collection {collection}, asset {asset}')

    
        out_dir = dex.check_outpath(out_dir)
        

        if urls:
            
           
            # Make mosaic by passing to _mosaic
            print(f'Mosaic will be created for collection {collection} in crs {out_crs} and resolution {resolution}')
            #TODO : do something with suffix
            if suffix:
                out_file = (f"{collection}_{asset}_mosaic_{out_crs.split(':')[1]}_{resolution}m-{suffix}.tif")
            else:
                out_file = (f"{collection}_{asset}_mosaic_{out_crs.split(':')[1]}_{resolution}m.tif")
            out_dict = dce.mosaic_by_window(df=df_collection,orderby=orderby,resolution=resolution,
                                  desc=desc,list_resolutions=list_resolutions,bbox=bbox,
                                  bbox_crs=bbox_crs,method=method,out_crs=out_crs,
                                  out_dir=out_dir,out_file=out_file,overviews=overviews)
            #If None is return, we don't want to add it to the list
            if isinstance(out_dict, dict):
                out_files.append(out_dict)
      

    if len(out_files) > 0 :
        print(''.rjust(75, '.'))
        print(f'Extracts are available here {out_dir}{os.sep}')
    
        return out_files
    else:
        return None