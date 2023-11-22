#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Description:
-----------
    Creates a valid cog from a clip
    Creates cogs from clipped imagery based on STAC collection_id and bbox'


Parameters:
----------
   See extract_cog() definition. 
   
Example usage:
-------------
    CLI or jobfile:
        python <path_to_file>/extract_cog.py <collections:asset,collection> -bbox="<bbox>" -bbox_crs <bbox_crs> -resolution <resolution> -out_dir <outpath>
        python extract_cog.py landcover -bbox="-1862977,312787,-1200000,542787" -bbox_crs EPSG:3979 -resolution 1000 -out_dir <outpath>

    As module:
        from extract.extract_cog import extract_cog
        
        #For independent file extraction
        extract_cog.extract_cog(<collections:asset>,
                                bbox="-1862977,312787,-1200000,542787",
                                bbox_crs='EPSG:3979',
                                resolution=None,
                                method='nearest',
                                out_crs=None,
                                out_dir=None,
                                suffix=None,
                                datetime_filter=None,
                                resolution_filter=None,
                                overviews=False,
                                debug=False)
        
        #For mosaic :
        extract_cog.extract_cog(<collections:asset>,
                                bbox="-1862977,312787,-1200000,542787",
                                bbox_crs='EPSG:3979',
                                resolution=10, #This parameter is obligatory with mosaic
                                method='nearest',
                                out_crs='EPSG:3979', #This parameter is obligatory with mosaic
                                out_dir=None,
                                suffix=None,
                                datetime_filter=None,
                                resolution_filter=1,
                                overviews=False,
                                mosaic=True) 
        #With geopackage :
        extract_cog.extract_cog(<collections:asset>,
                                field_value=<value_of_the_field_inside_geopackage>,
                                field_id='<field_name_inside_geopackage>'
                                geom_file='path_to_geopackage>',
                                resolution=10, #This parameter is obligatory with mosaic
                                method='nearest',
                                out_crs='EPSG:3979', #This parameter is obligatory with mosaic
                                out_dir=None,
                                suffix=None,
                                datetime_filter=None,
                                resolution_filter=None,
                                overviews=False)     

Created on Thu Mar 24 18:33:31 2022

Copyright:
---------
    Developed by Norah Brown, Charlotte Crevier, Marc-André Daviault
    Crown Copyright as described in section 12 of
    Copyright Act (R.S.C., 1985, c. C-42)
    © Her Majesty the Queen in Right of Canada,
    as represented by the Minister of Natural Resources Canada, 2022

Testing COG validation:
----------------------
    # source /space/partner/nrcan/geobase/work/opt/miniconda-datacube/etc/profile.d/conda.sh
    # conda activate datacube
    # rio cogeo validate /path_to_file/filename.tif

"""
# Standard modules
import argparse
import os
import sys
import pathlib

# Custom packages
import geopandas as gpd

# Ensure syspath first  reference is to .../dc_extract/... parent of all local files
# for this file it is  .../dc_extract/*/<modules> so need parents[1]
# define number of subdirs from 'root' (dc_extract)
_CHILD_LEVEL = 1
_DIR_NEEDED = str(pathlib.Path(__file__).parents[_CHILD_LEVEL].absolute())
if _DIR_NEEDED not in sys.path:
    sys.path.insert(0,_DIR_NEEDED)

import ccmeo_datacube.extract as dce
import ccmeo_datacube.extract_cog_validator as exc_validator
from ccmeo_datacube.utils import print_time

# Main functions
def extract_cog(collections:str,
                bbox:str=None,
                bbox_crs:str=None,
                field_value=None,
                field_id:str=None,
                geom_file:str=None,
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
    """
    Validate the input parameters before calling the _extract_cog() 

    Generates datacube layers ready for n-dimensional analysis (minicube)

    All collection data assets that intersect the bbox or geom_file
    are clipped, converted to cogs and aligned to the output crs define.
    The result is a stac of cogs ready for n-dimensional analysis (minicube)
    per pixel geospatial resolution / pixel size


    The generated cogs are aligned to the output crs define
    and snapped to a grid based on the input or output pixel size.

    The output area may be larger than the bbox submitted
    due to the alignment to the crs and resolution.

    Parameters
    ----------
    collections : str
        A List of STAC collection and asset with the convention:
            'collection1:asset,collection2:asset2,collection3'
        Asset are optionnal
    bbox : str, optional
        A text version of bounding box
        One of bbox of geom_file must be define for the extraction.
                'minx,miny,maxx,maxy' or 'west,south,east,north'
                 The default is None, which gets reassigned to '-120,46,-110,56'.
    bbox_crs : str, optional
        The CRS of the bbox. The default is 'EPSG:4326'. 
        Must be define with bbox
    field_value : str, int or float, optional
        if geom_file is define, corresponds to the value of the field define in field_id
        to filter the geopackage geometry. Must be define along with field_id
    field_id : str, optional
        If geom_file is define, can be define if a specific field in the geopackage should be use to 
        select the extraction geometry. Must be define along with field_value
    geom_file : str, optional
        Path to a geojson or geopackage containing the extent for extraction. 
        One of bbox of geom_file must be define for the extraction.
    resolution : int, optional
        The output resolution. The default is None,
        which keeps the original resolution for each layer.
        Must be define when mosaic=True
    method : str, optional
        Resampling alogrithms ('nearest', 'cubic', 'average', 'mode', and 'gauss')
        Default is nearest
    out_crs : str, optional
        The CRS of the output. The default is None
        If different from input crs, reprojection will be done.
        Must be define when mosaic=True
    out_dir : str, optional
        The output directory. The default is None, which gets reassigned to cwd.
    suffix : str
        Identificator added to output file name
        Default is None
    datetime_filter : str, optional
        Filter based on RFC 3339
        Should follow formats
        A date-time: "2018-02-12T23:20:50Z" or date : '2018-02-12'
        A closed interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z" or "2018-02-12/2018-03-18"
        Open intervals: "2018-02-12T00:00:00Z/.." or "../2018-03-18T12:31:12Z" or "2018-02-12/.." or "../2018-03-18"
        Default is None
    resolution_filter : str, optional
        Should follow formats:
        A specific resolution = "resolution" - Example :  "2" (only 2 meters)
        A closed interval = "min:max" - Example : "2:10" (from 2 to 10 meters inclusivly)
        Open intervals = "min:" or ":max" - Example: "2:" (2 meters and more) or ":2" (2 meters and less)
        Default is None
    overviews : bool, optional
        If the output tiff are real cog with overviews or normal geotiff.
        Most analysis outside of the datacube doesn't need overviews
        and the creation slow down the extraction process
        Default is False.
    debug : bool, optional
        The debug switch.
    mosaic : bool, optional
        The mosaic flag if we want the mosaic to be created from the output files
        Default is False
    orderby : str, optional
        The parameter to order the files to create the mosaic. 
        Default is 'date', which will put the latest file on top (reverse painters)
        Other accepted values are for now: ('resolution')
    desc : bool, optional
        The method to order the parameter to create the mosaic.
        Default is True, which take the latest (when orderby='date') or finest (when orderby='resolution') on top and goes from there in descending order

    Returns
    -------
    Path of the result minicube files.

    Example
    -------


    """
    params = {'collections':collections,
                'bbox':bbox,
                'bbox_crs':bbox_crs,
                'field_value':field_value,
                'field_id':field_id,
                'geom_file':geom_file,
                'resolution':resolution,
                'method':method,
                'out_crs':out_crs,
                'out_dir':out_dir,
                'suffix':suffix,
                'datetime_filter':datetime_filter,
                'resolution_filter':resolution_filter,
                'overviews':overviews,
                'debug':debug,
                'mosaic':mosaic, 
                'orderby':orderby, 
                'desc':desc}
    

    validated_parameters = exc_validator.ExtractCogSetting(**params)   
    # print(params, ' to --> ',validated_parameters)
    #TODO : add the validation of the input parameters
    results = _extract_cog(**dict(validated_parameters))
    # results = _extract_cog(**params)
    return results
    

@print_time
def _extract_cog(collections,bbox, bbox_crs,
                field_value,field_id,geom_file,
                resolution,method,out_crs,
                out_dir,suffix,datetime_filter,
                resolution_filter,overviews,
                debug,mosaic,orderby,desc):
    """
    Wrapper of the extract functionnalities
    """
    dex = dce.DatacubeExtract(debug=debug)
    
    #Create the empty list of output files
    out_files = []

    collections_asset = dex.collection_str_to_df(collections)
    
    if mosaic:
        print(f"Creation of mosaic for collection(s) {list(collections_asset['collection'])} located {out_dir}{os.sep} ...")
    else:
        print(f"Creation of a minicube with input {list(collections_asset['collection'])} located {out_dir}{os.sep} ...")
    
    #read geopackage or geojson into geodataframe
    if geom_file:
        gdf_geom = gpd.read_file(geom_file)
        extent_crs= gdf_geom.crs.to_string()
        # Todo : validate the number of verticies inside the extract function rather than extract_cog wrapper
        if dce.validate_num_vertex(gdf_geom, field_id, field_value):
            poly = dce.gdf_to_dict(gdf_geom, field_id, field_value)
        else :
            print(f"WARNING : Too many vertices inside {geom_file}. Bbox of geometry will be use instead. "
            "Please simplify your geometry for future extraction. (<500 vertices)")
            poly = None
            try:
                bbox = ','.join(str(v) for v in gdf_geom.loc[gdf_geom[field_id] == field_value].geometry.values[0].bounds)
            except:
                bbox = ','.join(str(v) for v in gdf_geom.geometry[0].bounds)
    else:
        poly = None
        extent_crs = bbox_crs
   
    # A pandas.DataFrame with url,collection_id,item_datetime,item_epsg from all asked collections
    df = dce.asset_urls(collections_asset, 
                        extent_crs,
                        bbox, 
                        poly,
                        datetime_filter=datetime_filter, 
                        resolution_filter=resolution_filter)
    
    # Get the list of all the input file resolutions from all collections
    list_resolutions = df.item_resolution.unique().tolist()
    
    #'Convert' the geometry to a bbox for the extraction if geom was define as input extent
    if geom_file and field_value and field_id:
        bbox = ','.join(str(v) for v in gdf_geom.loc[gdf_geom[field_id] == field_value].geometry.values[0].bounds)
    elif geom_file:
        bbox = ','.join(str(v) for v in gdf_geom.geometry[0].bounds)

        # bbox_crs = gdf_geom.crs.to_string()
    
    # For each collection, generate minicube, mosaic or wcs based cog
    for x in collections_asset.iloc:
        collection = x['collection']
        asset = x['asset']
        str_col_asset = f" {collection}, {asset} "
        print(str_col_asset.center(80, '#'))

        # Create WCS base cog
        if collection == 'hrdem-wcs':
            # Create a cog from a wcs
            out_file = dce.wcs(mosaic=mosaic,collection=collection,asset=asset,bbox=bbox,
                               bbox_crs=extent_crs,resolution=resolution,out_dir=out_dir,
                               method=method,overviews=overviews,suffix=suffix)
            
            if out_file:
                out_files.append(out_file)

        # Create a cog_chip, minicube or mosaic from a cog
        else:
            if asset : 
                df_collection = df.query('collection_id == @collection and asset_key == @asset')#.loc[df['collection_id']==collection]#&df['asset_key']==asset]
            else:
                df_collection = df.query('collection_id == @collection')
            urls = list(df_collection.url)
            
            print(f'INFO : Clipping {len(urls)} items from collection {collection}, asset {asset}')

            #Create output directory
            if not out_dir:
                out_dir = pathlib.Path.cwd()/'test-extract'
                
            out_dir = dex.check_outpath(out_dir)
            

            if urls:
                
                if mosaic == True:
                    if len(urls) > 1:
                        # Make mosaic by passing to _mosaic
                        print(f'Mosaic will be created for collection {collection} in crs {out_crs} and resolution {resolution}')
                        #TODO : do something with suffix
                        if suffix:
                            out_file = (f"{collection}_{asset}_mosaic_{out_crs.split(':')[1]}_{resolution}m-{suffix}.tif")
                        else:
                            out_file = (f"{collection}_{asset}_mosaic_{out_crs.split(':')[1]}_{resolution}m.tif")
                        out_dict = dce.mosaic(df=df_collection,orderby=orderby,resolution=resolution,
                                              desc=desc,list_resolutions=list_resolutions,bbox=bbox,
                                              bbox_crs=extent_crs,method=method,out_crs=out_crs,
                                              out_dir=out_dir,out_file=out_file,overviews=overviews)
                        #If None is return, we don't want to add it to the list
                        if isinstance(out_dict, dict):
                            out_files.append(out_dict)
                            
                    else:
                        # Call cogchip
                        print(f'INFO : Simple extraction (no mosaic) for {collection} with only one item')
                        # for url in urls: # Technically, there will be only one url in list
                        for item in df_collection.itertuples():    
                            out_file = dce.cog_chip(url=item.url,out_crs=out_crs,resolution=resolution,
                                                    list_resolutions=list_resolutions,bbox=bbox,
                                                    bbox_crs=extent_crs,method=method,out_dir=out_dir,
                                                    overviews=overviews,suffix=suffix,in_res=item.item_resolution)
                            if out_file: 
                                out_files.append(out_file)
                else:
                    # Call cogchip
                    # for url in urls:
                    for item in df_collection.itertuples():
                        out_file = dce.cog_chip(url=item.url,out_crs=out_crs,resolution=resolution,
                                                list_resolutions=list_resolutions,bbox=bbox,
                                                bbox_crs=extent_crs,method=method,out_dir=out_dir,
                                                overviews=overviews,suffix=suffix,in_res=item.item_resolution)
                        if out_file:
                            out_files.append(out_file)
            else:
                #todo : modify the message or the logic of urls per collection because not taking into account
                #if one of the asked collection has urls but the other one does not
                print(f'No urls for {collection} at {extent_crs}:{bbox}')

    if len(out_files) > 0 :
        print(''.rjust(75, '.'))
        print(f'Extracts are available here {out_dir}{os.sep}')
    
        return out_files
    else:
        return None


# CLI
def _handle_cli():
    """Processes CLI arguments and passes to appropriate function(s)"""
    desc = 'Creates cogs of clipped imagery based on STAC collection_id and bbox'
    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument('collections',
                        type=str,
                        help='A List of STAC collection and asset with the convention:collection1:asset,collection2:asset2,collection3 Asset are optionnal')
    parser.add_argument('-bbox',
                        type=str,
                        default=None,
                        help='The bounding box as string (minx,miny,maxx,maxy).')
    parser.add_argument('-bbox_crs',
                        type=str,
                        default=None,
                        help='The CRS code of the bbox ex: EPSG:4326.')
    parser.add_argument('-resolution',
                        type=str,
                        default='None',
                        help='The output resolution, if not specified then native resultion is used')
    parser.add_argument('-method',
                        type=str,
                        default='nearest',
                        help='The resampling method, default is nearest')
    parser.add_argument('-out_crs',
                        type=str,
                        default=None,
                        help='The CRS of the output. The default is None')
    parser.add_argument('-out_dir',
                        type=str,
                        default=None,
                        help='The output directory. The default is None, which gets reassigned to cwd.')
    parser.add_argument('-suffix',
                        type=str,
                        default=None,
                        help='identificator added to output file name')
    parser.add_argument('-datetime_filter',
                        type=str,
                        default=None,
                        help='Filter based on datetime')
    parser.add_argument('-resolution_filter',
                        type=str,
                        default=None,
                        help='Filter based on resolution')
    parser.add_argument('-overviews',
                        type=str,
                        default='False',
                        help='If the output tiff are real cog with overviews or normal geotiff.')
    parser.add_argument('-debug',
                        type=str,
                        default='False',
                        help='The debug switch')
    parser.add_argument('-mosaic',
                        type=str,
                        default='False',
                        help='The mosaic switch, trigger the creation of a mosaic instead of minicube')
    parser.add_argument('-orderby',
                        type=str,
                        default='date',
                        help='The parameter to order the files to create the mosaic, default is by date.')
    parser.add_argument('-desc',
                        type=str,
                        default='True',
                        help='The method to order the parameter to create the mosaic, default is True.')
    

    args=parser.parse_args()
    collections = args.collections
    # strip off the brackets and convert to string
    #bbox = str(args.bbox[1:-1])
    bbox = args.bbox
    bbox_crs = args.bbox_crs
    resolution = eval(args.resolution)
    if resolution :
        resolution=int(resolution)
    else:
        resolution=None
    method = args.method
    out_crs = args.out_crs
    out_dir = args.out_dir
    suffix = args.suffix
    datetime_filter = args.datetime_filter
    resolution_filter = args.resolution_filter
    overviews = eval(args.overviews)
    debug = eval(args.debug)
    mosaic = eval(args.mosaic) #For the plugin, we might need to do the trick like the overview flag for the 3 parameters
    orderby = args.orderby
    desc = eval(args.desc)
    print(f'Collections: {collections}')
    print(f'Bounding box: {bbox}')
    print(f'Bounding box crs: {bbox_crs}')
    print(f'Resolution: {resolution}')
    print(f'method: {method}')
    print(f'out_crs: {out_crs}')
    print(f'out_dir: {out_dir}')
    print(f'suffix: {suffix}')
    print(f'datetime_filter: {datetime_filter}')
    print(f'resolution_filter: {resolution_filter}')
    print(f'overviews: {overviews}')
    print(f'debug: {debug}')
    print(f'Output directory {out_dir}, if blank defaults to cwd')
    print(f'mosaic: {mosaic}')
    print(f'orderby: {orderby}')
    print(f'desc: {desc}')
    
    extract_cog(collections=collections,bbox=bbox,bbox_crs=bbox_crs,
                resolution=resolution,method=method,out_crs=out_crs,
                out_dir=out_dir,suffix=suffix,datetime_filter=datetime_filter,
                resolution_filter=resolution_filter,overviews=overviews,
                debug=debug,mosaic=mosaic,orderby=orderby,desc=desc)
    return

if __name__ == '__main__':
    _handle_cli()
