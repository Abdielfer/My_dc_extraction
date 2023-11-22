# -*- coding: utf-8 -*-
"""
Created on Fri Sep 23 13:18:09 2022

@author: ccrevier
"""
# Python standard library
import datetime
# import math
import pathlib
import sys
from osgeo import gdal
import rasterio
import pandas as pd

# Python custom modules
# import numpy
# import rasterio
# from rasterio.transform import Affine, from_origin
# from rasterio.warp import aligned_target, calculate_default_transform, reproject, transform
from shapely.geometry import box as sgbox

import ccmeo_datacube.extract.extract as dce
import ccmeo_datacube.extract.devtests.test_block_windows as tbw
import ccmeo_datacube.extract.devtests.test_extract_vrt as tev

_CHILD_LEVEL = 2
_DIR_NEEDED = str(pathlib.Path(__file__).parents[_CHILD_LEVEL].absolute())
if _DIR_NEEDED not in sys.path:
    sys.path.insert(0,_DIR_NEEDED)    

#NOT WORKING FOR NOW AVEC LES IMPORTS DES MODULES
def test_cog(res='cdem'):
    """Sample cog to run tests on"""
    if res == 'hrdem':
        return 'https://datacube-stage-data-public.s3.ca-central-1.amazonaws.com/store/elevation/hrdem-hrdsm/hrdem/hrdem-nb-dem.tif'
    elif res == 'cdem': 
        return 'http://datacube-prod-data-public.s3.ca-central-1.amazonaws.com/store/elevation/cdem-cdsm/cdem/cdem-canada-dem.tif'
    else:
        print('no res specified')
    
def test_res(collection='cdem'):
    """Native resolution of collection"""
    if collection =='hrdem':
        return 1
    elif collection == 'cdem':
        return 16
    else:
        print('No collection specified')
        
def test_bbox_dynamic(x_ori:int=2150469.4724999964, 
                      y_ori:int=144975.05299999937, 
                      x_km:int=5, 
                      y_km:int=5 ):
    """Sample bboxes based on origin (x, y) of Fredericton NB 5km x 5km provided by Header McGrath
    x_offset and y_offset are the bbox size in km"""
    
    x_max = x_ori + (x_km*1000)
    y_max = y_ori + (y_km*1000)
    bbox = (f'{x_ori}, {y_ori}, {x_max}, {y_max}')
    bbox_crs = 'EPSG:3979'
    return bbox,bbox_crs

def bbox_to_geojson(sizes:list, out_path):
    
    for size in sizes:
        bbox,bbox_crs = test_bbox_dynamic(x_km=size, y_km=size)
        bbox = tuple(float(v) for v in bbox.split(','))
        dex = dce.DatacubeExtract()
        # Write out bounds as geojson
        dict_poly = dex.poly_to_dict(sgbox(*bbox))
        dex.dict_to_vector_file(dict_poly, bbox_crs, out_path, f'bbox-{size}.geojson')

    return
    
        
def test_performance(scale:str='hrdem', reproject:bool=False):
    """Test the performance of the methods :
        1. rasterio original read
        2. rioxarray 
        3. window with rasterio read
        4. window with rioxarray read-write
        For the moment, we know that the rasterio read-clip-write (1) is slower 
        than the rioxarray read-clip-write method, we want to test if the window is 
        faster than the rioxarray
        
        reprojection using gdal is available, but not really fast.
        
        Test implemented in ordrer to create the faster way possible!!!!
   
    Launches modify_by_window in debug mode.
    Writes images amd geojson bboxes to test-images sub folder.
    Writes modify_by_window.log to logs subfolder.

    """
    #creation of a log file to write performance output
    main_folder = pathlib.Path(__file__).parent.joinpath('logs')
    if not main_folder.is_dir():
        main_folder.mkdir(parents=True,exist_ok=True)
    f = main_folder.joinpath('performance.log')
    if not f.is_file():
        fp = f.open(mode='w')
    else:
        fp = f.open(mode='a')
      
    sizes = [5, 10, 20, 40, 50, 75, 100]
    res =  test_res(scale)#toujours natif pour le moment (dem fine = 1meter res)
    resample = 'bilinear'
    epsg = 3979 #toujours natif pour le moment
    dex = dce.DatacubeExtract()
    fp.write(f'\n{datetime.datetime.now()}')
    fp.write('\n----------------Starting performance processes-------------')
    #Creation of the output csv file contaning the mtric for each methods
    df = pd.DataFrame(columns=['method', 'extent', 'num_pixel', 'pixel_size', 'run_time'])
    
    #1. lancer la commande pour extract_by_window
    fp.write('\nStarting extract by window\n')
    for size in sizes:
        start = datetime.datetime.now()
        fp.write(f'\nStart: {start} for read-clip-write')
        bbox,bbox_crs = test_bbox_dynamic(x_km=size, y_km=size)
        method = 'extract_by_window'
        out_name = f'extract-{size}-{res}-{resample}-{epsg}-{method}.tif'
        folder = pathlib.Path(__file__).parent.joinpath('test-images')
        if not folder.is_dir():
            folder.mkdir(parents=True,exist_ok=True)
        f = folder.joinpath(out_name)
        
        params = {
            'img_path':test_cog(scale),
            'out_path':f,
            'bbox':bbox,
            'bbox_crs':bbox_crs,
            'out_res':res,
            'out_crs':f'EPSG:{epsg}',
            'resample':resample,
            'debug':False
            }
        fp.write(f'\nParameters : bbox size {size}km, resolution {res}m, resample {resample}, out crs {epsg}, extract method: {method}')
        # print(params)
        clipped_file = tbw.modify_by_window(**params)
        fp.write(f'\nOut file : {clipped_file} for read-clip-write')
        #time to run
        end = datetime.datetime.now()
        fp.write(f'\nEnd: {datetime.datetime.now()} for read-clip-write')
        total = (end-start).total_seconds()
        fp.write(f'\nTotal time {total} seconds for read-clip-write\n')
        temp = rasterio.open(clipped_file)
        num_pixel = temp.profile['height']*temp.profile['width']
        temp.close()
        df.loc[len(df.index)] = [method, size, num_pixel, res, total] #['method', 'extent', 'num_pixel', 'pixel_size', 'run_time']
        if reproject:
            out_epsg=2960
            fp.write(f'\n------Reprojection to UTM 19 (EPSG:{out_epsg})------')
            start = datetime.datetime.now()
            fp.write(f'\nStart: {start} for reproject')
            out_name = f'{clipped_file.stem}-{res}m-{resample}-{out_epsg}{clipped_file.suffix}'
            out_path = folder.joinpath(out_name)
            out_file = test_reproject(clipped_file,out_path, epsg=epsg, out_epsg=out_epsg, resolution=res, resample=resample)
            fp.write(f'\nReprojected outfile path : {out_file}')          
            end = datetime.datetime.now()
            fp.write(f'\nEnd: {datetime.datetime.now()} for reproject')
            total = (end-start).total_seconds()
            fp.write(f'\nTotal time {total} seconds for reprojection of bbox size {size}\n')
            fp.write(f'\n---------------------------------------------------')
            
    fp.write('\nextract by window is DONE.')
    fp.write('\n------------------------------')
    
    #2. Lancer la commande pour extract with rioxarray (dex.clip_to_grid())
    fp.write('\nStarting clip_to_grid() with rioxarray\n')
    for size in sizes:
        start = datetime.datetime.now()
        fp.write(f'\nStart: {start} for read-clip-write')
        bbox,bbox_crs = test_bbox_dynamic(x_km=size, y_km=size)
        method = 'rioxarray'
        out_name = f'extract-{size}-{res}-{resample}-{epsg}-{method}.tif'
        folder = pathlib.Path(__file__).parent.joinpath('test-images')
        if not folder.is_dir():
            folder.mkdir(parents=True,exist_ok=True)
        f = folder.joinpath(out_name)
        params = {
            'clip_file':test_cog(scale),
            'out_file':f,
            'bbox':bbox,
            'bbox_crs':bbox_crs,
            'resolution':res,
            'national':False,
            'overviews':False, 
            'resample':resample}
        fp.write(f'\nParameters : bbox size {size}km, resolution {res}m, resample {resample}, epsg {epsg}, extract method: {method}')
        clipped_file = dex.clip_to_grid(**params)
        fp.write(f'\nOut file : {clipped_file} for read-clip-write')
        #time to run
        end = datetime.datetime.now()
        fp.write(f'\nEnd: {datetime.datetime.now()} for read-clip-write')
        total = (end-start).total_seconds()
        fp.write(f'\nTotal time {total} seconds for read-clip-write\n')
        temp = rasterio.open(clipped_file)
        num_pixel = temp.profile['height']*temp.profile['width']
        temp.close()
        df.loc[len(df.index)] = [method, size, num_pixel, res, total] #['method', 'extent', 'num_pixel', 'pixel_size', 'run_time']
        if reproject:
            out_epsg=2960
            fp.write(f'\n------Reprojection to UTM 19 (EPSG:{out_epsg})------')
            start = datetime.datetime.now()
            fp.write(f'\nStart: {start} for reproject')
            out_name = f'{clipped_file.stem}-{res}m-{resample}-{out_epsg}{clipped_file.suffix}'
            out_path = folder.joinpath(out_name)
            out_file = test_reproject(clipped_file,out_path, epsg=epsg, out_epsg=out_epsg, resolution=res, resample=resample)
            fp.write(f'\nReprojected outfile path : {out_file}')  
            end = datetime.datetime.now()
            fp.write(f'\nEnd: {datetime.datetime.now()} for reproject')
            total = (end-start).total_seconds()
            fp.write(f'\nTotal time {total} seconds for reprojection of bbox size {size}\n')
            fp.write(f'\n---------------------------------------------------')
    
    fp.write('\nextract with rioxarray is DONE.')
    fp.write('\n------------------------------')
    
    #3. lancer la commande pour extract avec rasterio (dex.clip_to_grid(), seulement si on remarque que le window reading est très lent)
    fp.write('\nStarting clip_to_grid() with rasterio')
    for size in sizes:
        start = datetime.datetime.now()
        fp.write(f'\nStart: {start} for read-clip-write')
        bbox,bbox_crs = test_bbox_dynamic(x_km=size, y_km=size)
        method = 'rasterio'
        out_name = f'extract-{size}-{res}-{resample}-{epsg}-{method}.tif'
        folder = pathlib.Path(__file__).parent.joinpath('test-images')
        if not folder.is_dir():
            folder.mkdir(parents=True,exist_ok=True)
        f = folder.joinpath(out_name)
        params = {
            'clip_file':test_cog(scale),
            'out_file':f,
            'bbox':bbox,
            'bbox_crs':bbox_crs,
            'resolution':res,
            'national':False,
            'overviews':False,
            'resample':resample,
            'xarray':False}
        fp.write(f'\nParameters : bbox size {size}km, resolution {res}m, resample {resample}, epsg {epsg}, extract method: {method}')
        clipped_file = dex.clip_to_grid(**params)
        fp.write(f'\nOut file : {clipped_file} for read-clip-write')
        #time to run
        end = datetime.datetime.now()
        fp.write(f'\nEnd: {datetime.datetime.now()} for read-clip-write')
        total = (end-start).total_seconds()
        fp.write(f'\nTotal time {total} seconds for read-clip-write\n')
        temp = rasterio.open(clipped_file)
        num_pixel = temp.profile['height']*temp.profile['width']
        temp.close()
        df.loc[len(df.index)] = [method, size, num_pixel, res, total] #['method', 'extent', 'num_pixel', 'pixel_size', 'run_time']
        if reproject:
            out_epsg=2960
            fp.write(f'\n------Reprojection to UTM 19 (EPSG:{out_epsg})------')
            start = datetime.datetime.now()
            fp.write(f'\nStart: {start} for reproject')
            out_name = f'{clipped_file.stem}-{res}m-{resample}-{out_epsg}{clipped_file.suffix}'
            out_path = folder.joinpath(out_name)
            out_file = test_reproject(clipped_file,out_path, epsg=epsg, out_epsg=out_epsg, resolution=res, resample=resample)
            fp.write(f'\nReprojected outfile path : {out_file}')
            end = datetime.datetime.now()
            fp.write(f'\nEnd: {datetime.datetime.now()} for reproject')
            total = (end-start).total_seconds()
            fp.write(f'\nTotal time {total} seconds for reprojection of bbox size {size}\n')
            fp.write(f'\n---------------------------------------------------')
    fp.write('\nExtract with rasterio is DONE')
    fp.write('\n------------------------------')
    
    #4. Lancer la commande pour extract avec rioxarray, mais qui utilise la definition du bbox en window
    fp.write('\nStarting extract_by_rio() with window bbox and rioxarray')
    for size in sizes:
        start = datetime.datetime.now()
        fp.write(f'\nStart: {start} for read-clip-write')
        bbox,bbox_crs = test_bbox_dynamic(x_km=size, y_km=size)
        method = 'window-rioxarray'
        out_name = f'extract-{size}-{res}-{resample}-{epsg}-{method}.tif'
        folder = pathlib.Path(__file__).parent.joinpath('test-images')
        if not folder.is_dir():
            folder.mkdir(parents=True,exist_ok=True)
        f = folder.joinpath(out_name)
        params = {
            'img_path':test_cog(scale),
            'out_file':f,
            'bbox':bbox,
            'bbox_crs':bbox_crs,
            'out_res':res,
            'out_crs':False,
            'overviews':False,
            'resample':resample,
            'blocksize':512}
        fp.write(f'\nParameters : bbox size {size}km, resolution {res}m, resample {resample}, epsg {epsg}, extract method: {method}')
        clipped_file = dex.extract_by_rio(**params)
        fp.write(f'\nOut file : {clipped_file} for read-clip-write')
        #time to run
        end = datetime.datetime.now()
        fp.write(f'\nEnd: {datetime.datetime.now()} for read-clip-write')
        total = (end-start).total_seconds()
        fp.write(f'\nTotal time {total} seconds for read-clip-write\n')
        temp = rasterio.open(clipped_file)
        num_pixel = temp.profile['height']*temp.profile['width']
        temp.close()
        df.loc[len(df.index)] = [method, size, num_pixel, res, total] #['method', 'extent', 'num_pixel', 'pixel_size', 'run_time']
        if reproject:
            out_epsg=2960
            fp.write(f'\n------Reprojection to UTM 19 (EPSG:{out_epsg})------')
            start = datetime.datetime.now()
            fp.write(f'\nStart: {start} for reproject')
            out_name = f'{clipped_file.stem}-{res}m-{resample}-{out_epsg}{clipped_file.suffix}'
            out_path = folder.joinpath(out_name)
            out_file = test_reproject(clipped_file,out_path, epsg=epsg, out_epsg=out_epsg, resolution=res, resample=resample)
            fp.write(f'\nReprojected outfile path : {out_file}')
            end = datetime.datetime.now()
            fp.write(f'\nEnd: {datetime.datetime.now()} for reproject')
            total = (end-start).total_seconds()
            fp.write(f'\nTotal time {total} seconds for reprojection of bbox size {size}\n')
            fp.write(f'\n---------------------------------------------------')
    
    fp.write('\nExtract with window and rioxarray is DONE')
    
    
    #5. Lancer la commande pour extract avec les vrt
    #tev l'endroit ou il y a les test_extract_vrt
    fp.write('\nStarting extraction using GDAL virtual warping')
    for size in sizes:
        start = datetime.datetime.now()
        fp.write(f'\nStart: {start} for read-clip-write')
        bbox,bbox_crs = test_bbox_dynamic(x_km=size, y_km=size)
        method = 'warpedVRT'
        out_name = f'extract-{size}-{res}-{resample}-{epsg}-{method}.tif'
        folder = pathlib.Path(__file__).parent.joinpath('test-images')
        if not folder.is_dir():
            folder.mkdir(parents=True,exist_ok=True)
        f = folder.joinpath(out_name)
        params = {
            'img_path':test_cog(scale),
            'out_path':f,
            'bbox':bbox,
            'bbox_crs':bbox_crs,
             }
        
        fp.write(f'\nParameters : bbox size {size}km, resolution {res}m, resample {resample}, epsg {epsg}, extract method: {method}')
        clipped_file = tev.extract_vrt_tap(**params)
        fp.write(f'\nOut file : {clipped_file} for read-clip-write')
        #time to run
        end = datetime.datetime.now()
        fp.write(f'\nEnd: {datetime.datetime.now()} for read-clip-write')
        total = (end-start).total_seconds()
        fp.write(f'\nTotal time {total} seconds for read-clip-write\n')
        temp = rasterio.open(clipped_file)
        num_pixel = temp.profile['height']*temp.profile['width']
        temp.close()
        df.loc[len(df.index)] = [method, size, num_pixel, res, total] #['method', 'extent', 'num_pixel', 'pixel_size', 'run_time']
        if reproject:
            out_epsg=2960
            fp.write(f'\n------Reprojection to UTM 19 (EPSG:{out_epsg})------')
            start = datetime.datetime.now()
            fp.write(f'\nStart: {start} for reproject')
            out_name = f'{clipped_file.stem}-{res}m-{resample}-{out_epsg}{clipped_file.suffix}'
            out_path = folder.joinpath(out_name)
            out_file = test_reproject(clipped_file,out_path, epsg=epsg, out_epsg=out_epsg, resolution=res, resample=resample)
            fp.write(f'\nReprojected outfile path : {out_file}')
            end = datetime.datetime.now()
            fp.write(f'\nEnd: {datetime.datetime.now()} for reproject')
            total = (end-start).total_seconds()
            fp.write(f'\nTotal time {total} seconds for reprojection of bbox size {size}\n')
            fp.write(f'\n---------------------------------------------------')
    
    fp.write('\nExtract extraction using GDAL virtual warping is DONE')
    fp.write('\n-------------END OF RUN-----------------')
    fp.close()
    
    #Save the dataframe to csv
    df.to_csv(main_folder.joinpath('performance.csv'))
    return

def test_reproject(clipped_file,
                   out_path, 
                   epsg:float=3979,
                   out_epsg:float=2960, 
                   resolution:int=1, 
                   resample:str='bilinear'):
    
    input_raster = gdal.Open(str(clipped_file))
    output_raster = str(out_path)
    ulx, xres, xskew, uly, yskew, yres  = input_raster.GetGeoTransform()
    lrx = ulx + (input_raster.RasterXSize * xres)
    lry = uly + (input_raster.RasterYSize * yres)
    in_crs = rasterio.crs.CRS.from_epsg(epsg)
    out_crs = rasterio.crs.CRS.from_epsg(out_epsg)
    dst_transform,dst_width,dst_height = tbw.tap_params(in_crs=in_crs,
                                                   out_crs=out_crs,
                                                   in_left=ulx,
                                                   in_bottom=lry,
                                                   in_right=lrx,
                                                   in_top=uly,
                                                   in_width=input_raster.RasterXSize,
                                                   in_height=input_raster.RasterYSize,
                                                   out_res=resolution)
    (xmin, ymin, xmax, ymax) = rasterio.transform.array_bounds(dst_height,dst_width,dst_transform)
    warp = gdal.Warp(output_raster,input_raster,dstSRS=f'EPSG:{out_epsg}', 
                     resampleAlg=resample, xRes=resolution, yRes=resolution,
                     targetAlignedPixels=True, outputBounds=(xmin, ymin, xmax, ymax))
    warp = None # Closes the files
    return output_raster
    
    