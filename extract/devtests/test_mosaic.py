# -*- coding: utf-8 -*-
"""
Created on Wed Feb  1 15:05:22 2023

@author: nbrown
"""
import rasterio
import pathlib
import os
import numpy 
import pandas as pd
import rioxarray
import datetime

from rasterio.transform import from_origin

import ccmeo_datacube.extract.extract as dce

def minicube_mosaic(list_of_params, 
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
    # All file must have same crs, same number of bands and same datatype and same bounds
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
            # extract_params = out_profile.copy()
            # extract_params['resampling'] = dce.resample_value('bilinear')
            # for in_path in files:
            for params in list_of_params:
                in_path = params['file'] 
                extract_params = params['params']
                # with rasterio.open(in_path) as src:
                with rioxarray.open_rasterio(in_path, lock=False, chunks=True) as xar:
                # cog = rioxarray.open_rasterio(vrt, lock=False, chunks=True)
                    nodata = xar.rio.nodata
                    
                    out_res = out_profile['transform'].a
                    in_res = extract_params['transform'].a
                    if out_res != in_res:
                        print('Output pixel size is different from input pixel size, '\
                              'starting resampling...')
                        xar = xar.rio.reproject(xar.rio.crs,
                            shape=(out_profile['height'], out_profile['width']),
                            resampling=extract_params['resampling'],
                            transform = out_profile['transform'])
                    
                    dst_window = rasterio.windows.from_bounds(*xar.rio.bounds(), out_img.transform)
                    window_arr = xar.values[0]
                    if numpy.all((window_arr == nodata)):
                        print('all no data')
                        continue
                print('update value in raster')
                # Assign source no data to numpy.nan
                nodata_mask = (window_arr == nodata)
                window_arr[nodata_mask] = numpy.nan
                # Read values already written to current window
                existing_window = out_img.read(band,window=dst_window)
                
                # Overwrite current nodata values with existing values(reverse painters)
                current_no_data = (numpy.isnan(window_arr))
                window_arr[current_no_data] = existing_window[current_no_data]
                # Write the numpy array to the output per band window
                out_img.write(window_arr,indexes=band,window=dst_window)
                print('Writting done, closing...')

    return out_path
