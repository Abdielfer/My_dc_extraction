#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 21 15:28:49 2022

@author: chc002
"""

import rasterio
import pathlib
import os
import numpy 
import pandas as pd
import rioxarray
import datetime
import pathlib
import sys

from rasterio.transform import from_origin

_CHILD_LEVEL = 1
_DIR_NEEDED = str(pathlib.Path(__file__).parents[_CHILD_LEVEL].absolute())
if _DIR_NEEDED not in sys.path:
    sys.path.insert(0,_DIR_NEEDED)
    
import ccmeo_datacube.extract.extract as dce
import ccmeo_datacube.extract.devtests.test_mosaic_all_filled as tmf

def test_bbox_dynamic(x_ori:int=1708821, 
                      y_ori:int=-107045, 
                      x_km:int=5, 
                      y_km:int=5 ):
    """Sample bboxes based on origin (x, y) of Fredericton NB 5km x 5km provided by Header McGrath
    x_offset and y_offset are the bbox size in km"""
    
    x_max = x_ori + (x_km*1000)
    y_max = y_ori + (y_km*1000)
    bbox = (f'{x_ori}, {y_ori}, {x_max}, {y_max}')
    bbox_crs = 'EPSG:3979'
    return bbox,bbox_crs

def performance(output_path='/gpfs/fs5/nrcan/nrcan_geobase/work/transfer/work/datacube/DEV_CC/dev_dc_extract/mosaic/performance'):
    """
    Test the limit of extract before getting memory error
    Also allow to test the time performance for different bbox size

    Returns
    -------
    None.

    """
    sizes = [5, 10, 20, 30, 50, 75, 100, 150]
    df = pd.DataFrame(columns=['extent', 'num_pixel', 'run_time', 'asset_count', 'used_files', 'unused_files'])
    for size in sizes:
        start = datetime.datetime.now()
        bbox,bbox_crs = test_bbox_dynamic(x_km=size, y_km=size)
        out_file = f'hrdem_mosaic_{size}.tif'
        out_path = pathlib.Path(output_path, out_file)
        output, asset_count, used_files, unused_files = wrapper_mosaic(bbox, bbox_crs, out_path)
        end = datetime.datetime.now()
        total = (end-start).total_seconds()
        
        temp = rasterio.open(output)
        num_pixel = temp.profile['height']*temp.profile['width']
        temp.close()
        df.loc[len(df.index)] = [size, num_pixel, total, asset_count, used_files, unused_files]
        
    df.to_csv(pathlib.Path(output_path, 'performance.csv'))
    
    return

def wrapper_mosaic(bbox, bbox_crs, out_path):
    #Test to use the warped read to create the mosaic
    #steps : 
    #1. call the get_extract_params to create the parameters of extraction
    #2. Call the warped_mosaic to create the mosaic
    
    dex = dce.DatacubeExtract()
    # profiles = []
    # paramss = []
    
    # bbox = (1783831, -119026, 1800868, -56631)
    # tbox = '1783831, -119026, 1800868, -56631'
    # tbox_crs = 'EPSG:3979'
    out_crs='EPSG:3979'
    resolution=4
    method=None
    
    
    collections = 'hrdem-lidar'
    
    urls = dex.asset_urls(collections,bbox,bbox_crs)
    
    files = [i for i in urls if 'dsm' in i]
    # out_path = f'/gpfs/fs5/nrcan/nrcan_geobase/work/transfer/work/datacube/DEV_CC/dev_dc_extract/mosaic/02-06-23'

    #ToDO : Redefine the right profile and extraction parameters
    #Test : create a dataframe of the input file and the profile and params 
    # df_params = pd.DataFrame(columns=['file', 'profile', 'params'])
    list_params=[]
    list_profile=[]
    for file in files:
        dict_file = {}
        profile, params = dce.get_extract_params(in_path=file,
        										 out_crs=out_crs,
        										 out_res=resolution,
        										 bbox=bbox,
        										 bbox_crs=bbox_crs,
        										 resampling_method=method)
        dict_file['file']=file
        # dict_file['profile']=profile
        dict_file['params']=params
        list_params.append(dict_file)
        list_profile.append(profile)
    
    #Test qui valide que tous les profiles sont les memes (car c'est supposer etre le cas)
    #ne marche pas
    # validation = len(set(list_profile))
    

    print(len(files), 'files to merge')
 
    #Choisir une des multiples fonction de mosaic suivante:
    #--
    #used_files=[]
    #unused_files=[]
    #rasterio+rasterio.WarpedVRT+rioxarray
    # output = warped_mosaic_1(list_params, out_path, list_profile[0])
    
    #raterio+rasterio.WarpedVRT
    # output = warped_mosaic_2(list_params, out_path, list_profile[0])
    
    #rasterio+rasterio.WarpedVRT+by_window
    # output = warped_mosaic_3(list_params, out_path, list_profile[0])
    #--
    
    #For the test of performance for the nodatamask reading
    #Run the performance code once with those two external functions 
    
    #The mosaic function that exist inside the extract.py
    # used_files, unused_files = tmf.warped_mosaic(list_params, out_path, list_profile[0])
    
    #The mosaic function that read the nodata mask every increment 
    used_files, unused_files = tmf.warped_mosaic_read_mask(list_params, out_path, list_profile[0])
    
    return out_path, len(files), used_files, unused_files
    

@dce.print_time
def warped_mosaic_1(list_of_params, 
                  out_path, 
                  out_profile,  
                  overviews=None):
    
    """
    list_of_params: list of dictionnary
    ex: [{'file': 
    'https://datacube-prod-data-public.s3.amazonaws.com/store/elevation/hrdem/hrdem-lidar/GEOMONT-2013_est_RNCan-2m-dsm.tif',
      'params': {'driver': 'GTiff', 'dtype': 'float32', 'nodata': -32767.0, '
                  width': 2500.0, 'height': 2502.0, 'count': 1, 'crs': CRS.from_epsg(3979), 
                  'transform': Affine(2.0, 0.0, 1708820.0, 0.0, -2.0, -102044.0), 'blockxsize': 512, '
                  blockysize': 512, 'tiled': True, 'compress': 'lzw', 'interleave': 'band', 
                  'resampling': <Resampling.bilinear: 1>}},]
    """
    #Write file on top of each other with the warped method
    #All file must have same crs, same number of bands and same datatype
    dex = dce.DatacubeExtract()
    #TODO : Explore standart env. setup
    #TODO : Create a function for setting up the env.
    env = rasterio.Env(
        GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR", # a way to disable loading of side-car or auxiliary files
        CPL_VSIL_CURL_USE_HEAD=False, #pas certaine de ce que ca fait
        CPL_VSIL_CURL_ALLOWED_EXTENSIONS="TIF", #considering only files that ends with .tif
        GDAL_NUM_THREADS='ALL_CPUS',#Enable multi-threaded compression by specifying the number of worker threads
        GDAL_HTTP_UNSAFESSL=1,
        # CPL_CURL_VERBOSE=1,
        # CURL_CA_BUNDLE='/space/partner/nrcan/geobase/work/opt/miniconda-datacube/envs/datacube-usecase/lib/python3.9/site-packages/certifi/cacert.pem',
        # REQUESTS_CA_BUNDLE='/space/partner/nrcan/geobase/work/opt/miniconda-datacube/envs/datacube-usecase/lib/python3.9/site-packages/certifi/cacert.pem'
          )

    band=1
    with env:
        with rasterio.open(out_path, mode="w+", **out_profile) as out_img:
            for params in list_of_params:
                file = params['file']
                extract_params = params['params']
                print(file)
                with rasterio.open(file) as src:
                    with rasterio.vrt.WarpedVRT(src, **extract_params) as vrt:
                        with rioxarray.open_rasterio(vrt, lock=False, chunks=True) as xar:

                            nodata = xar.rio.nodata
                            
                            #Pour le resampling
                            out_res = out_profile['transform'].a
                            in_res = extract_params['transform'].a
                            if out_res != in_res:
                                print('Output pixel size is different from input pixel size, '\
                                      'starting resampling...')
                                xar = xar.rio.reproject(xar.rio.crs,
                                    shape=(out_profile['height'], out_profile['width']),
                                    resampling=extract_params['resampling'],
                                    transform = out_profile['transform'])
                                
                            #Calcul de la fenetre dans le fichier output
                            dst_window = rasterio.windows.from_bounds(*xar.rio.bounds(), out_img.transform)
                            window_arr = xar.values[0]
                        
                    #Pour valider qu'il y a autre chose que tu nodata dans les fichiers
                    if numpy.all((window_arr == nodata)):
                        print('all no data')

                    else:
                        print('update value in raster')
                        # Read values already written to current window
                        # In out_crs spatial coords
                        existing_arr = out_img.read(band,window=dst_window)

                        # Make an existing window no_data mask
                        no_data_mask = (existing_arr == out_img.nodata)
                        # LOG.debug(f'no_data_mask shape {no_data_mask.shape}')
                        new_data = existing_arr
                        new_data[no_data_mask] = window_arr[no_data_mask]
                        out_img.write(new_data,indexes=band,window=dst_window)

    return out_path


@dce.print_time
def warped_mosaic_2(list_of_params, 
                  out_path, 
                  out_profile,  
                  overviews=None):
    
    """
    list_of_params: list of dictionnary
    ex: [{'file': 
    'https://datacube-prod-data-public.s3.amazonaws.com/store/elevation/hrdem/hrdem-lidar/GEOMONT-2013_est_RNCan-2m-dsm.tif',
      'params': {'driver': 'GTiff', 'dtype': 'float32', 'nodata': -32767.0, '
                  width': 2500.0, 'height': 2502.0, 'count': 1, 'crs': CRS.from_epsg(3979), 
                  'transform': Affine(2.0, 0.0, 1708820.0, 0.0, -2.0, -102044.0), 'blockxsize': 512, '
                  blockysize': 512, 'tiled': True, 'compress': 'lzw', 'interleave': 'band', 
                  'resampling': <Resampling.bilinear: 1>}},]
    """
    #Write file on top of each other with the warped method
    #All file must have same crs, same number of bands and same datatype
    dex = dce.DatacubeExtract()
    #TODO : Explore standart env. setup
    #TODO : Create a function for setting up the env.
    env = rasterio.Env(
        GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR", # a way to disable loading of side-car or auxiliary files
        CPL_VSIL_CURL_USE_HEAD=False, #pas certaine de ce que ca fait
        CPL_VSIL_CURL_ALLOWED_EXTENSIONS="TIF", #considering only files that ends with .tif
        GDAL_NUM_THREADS='ALL_CPUS',#Enable multi-threaded compression by specifying the number of worker threads
        GDAL_HTTP_UNSAFESSL=1,
        # CPL_CURL_VERBOSE=1,
        # CURL_CA_BUNDLE='/space/partner/nrcan/geobase/work/opt/miniconda-datacube/envs/datacube-usecase/lib/python3.9/site-packages/certifi/cacert.pem',
        # REQUESTS_CA_BUNDLE='/space/partner/nrcan/geobase/work/opt/miniconda-datacube/envs/datacube-usecase/lib/python3.9/site-packages/certifi/cacert.pem'
          )

    band=1
    with env:
        with rasterio.open(out_path, mode="w+", **out_profile) as out_img:
            extract_params = out_profile.copy()
            extract_params['resampling'] = dce.resample_value('bilinear')
            for params in list_of_params:
                file = params['file']
                print(file)
                with rasterio.open(file) as src:
                    with rasterio.vrt.WarpedVRT(src, **extract_params) as vrt:
                    
                        nodata = vrt.nodata
                            
                        #Calcul de la fenetre dans le fichier output
                        dst_window = rasterio.windows.from_bounds(*vrt.bounds, out_img.transform)
                        window_arr = vrt.read(1)
                    
                    #Pour valider qu'il y a autre chose que tu nodata dans les fichiers
                    if numpy.all((window_arr == nodata)):
                        print('all no data')

                    else:
                        print('update value in raster')

                        # Read values already written to current window
                        # In out_crs spatial coords
                        existing_arr = out_img.read(band,window=dst_window)
                        print(existing_arr, 'existing array')

                        # Make an existing window no_data mask
                        no_data_mask = (existing_arr == out_img.nodata)
                        new_data = existing_arr
                        new_data[no_data_mask] = window_arr[no_data_mask]
                        out_img.write(new_data,indexes=band,window=dst_window)
                

    return out_path


@dce.print_time
def warped_mosaic_3(list_of_params, 
                  out_path, 
                  out_profile,  
                  overviews=None):
    
    """
    list_of_params: list of dictionnary
    ex: [{'file': 
    'https://datacube-prod-data-public.s3.amazonaws.com/store/elevation/hrdem/hrdem-lidar/GEOMONT-2013_est_RNCan-2m-dsm.tif',
      'params': {'driver': 'GTiff', 'dtype': 'float32', 'nodata': -32767.0, '
                 width': 2500.0, 'height': 2502.0, 'count': 1, 'crs': CRS.from_epsg(3979), 
                 'transform': Affine(2.0, 0.0, 1708820.0, 0.0, -2.0, -102044.0), 'blockxsize': 512, '
                 blockysize': 512, 'tiled': True, 'compress': 'lzw', 'interleave': 'band', 
                 'resampling': <Resampling.bilinear: 1>}},]
    """
    #Write file on top of each other with the warped method
    #All file must have same crs, same number of bands and same datatype
    dex = dce.DatacubeExtract()
    #TODO : Explore standart env. setup
    #TODO : Create a function for setting up the env.
    env = rasterio.Env(
        GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR", # a way to disable loading of side-car or auxiliary files
        CPL_VSIL_CURL_USE_HEAD=False, #pas certaine de ce que ca fait
        CPL_VSIL_CURL_ALLOWED_EXTENSIONS="TIF", #considering only files that ends with .tif
        GDAL_NUM_THREADS='ALL_CPUS',#Enable multi-threaded compression by specifying the number of worker threads
        GDAL_HTTP_UNSAFESSL=1,
        # CPL_CURL_VERBOSE=1,
        # CURL_CA_BUNDLE='/space/partner/nrcan/geobase/work/opt/miniconda-datacube/envs/datacube-usecase/lib/python3.9/site-packages/certifi/cacert.pem',
        # REQUESTS_CA_BUNDLE='/space/partner/nrcan/geobase/work/opt/miniconda-datacube/envs/datacube-usecase/lib/python3.9/site-packages/certifi/cacert.pem'
         )

    band=1
    with env:
        with rasterio.open(out_path, mode="w+", **out_profile) as out_img:
            extract_params = out_profile.copy()
            extract_params['resampling'] = dce.resample_value('bilinear')
            for params in list_of_params:
                file = params['file']
                print(file)
                with rasterio.open(file) as src:
                    with rasterio.vrt.WarpedVRT(src, **extract_params) as vrt:
                        
                        nodata = vrt.nodata
                        
                        for block, src_window in vrt.block_windows(band):
                            window_arr = vrt.read(band, window=src_window)
                            if numpy.all((window_arr == nodata)):
                                print('all no data')
                            else:
                                print('update value in raster')
                                src_bounds = rasterio.windows.bounds(src_window,
                                                                      transform=vrt.transform,
                                                                       height=src_window.height,
                                                                       width=src_window.height)
                                dst_window = rasterio.windows.from_bounds(*src_bounds,
                                                                          transform=out_img.transform)
                                existing_arr = out_img.read(band,window=dst_window)
                                no_data_mask = (existing_arr == out_img.nodata)
                                new_data = existing_arr
                                new_data[no_data_mask] = window_arr[no_data_mask]
                                out_img.write(new_data,indexes=band,window=dst_window)     
                                
                                
    return out_path