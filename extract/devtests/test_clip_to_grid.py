#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 14 18:17:07 2022

Contains basic test to validate the output of the new and improved clip_to_grid() function

@author: chc002
"""
# import os
# import subprocess
# import pathlib
import rasterio
import numpy as np
import rioxarray

# def get_params():
#     out_path = '/gpfs/fs5/nrcan/nrcan_geobase/work/transfer/work/datacube/DEV_CC/issue_hm/validate_changes/from_function'
#     file_name = 'hrdem-nb-dem-clip_rasterio_overviews.tif'
    
#     file_path = pathlib.Path(os.path.join(out_path, file_name))
    
#     return file_path

# def validate_cog():
#     #DOES NOT WORK INSIDE PYTHON, need to be done in conda command prompt
#     """Validate that the cog in output is valid
#     path : path to result cog from the clip_to_grid()"""
#     path = get_params()
#     subprocess.run("source /space/partner/nrcan/geobase/work/opt/miniconda-datacube/etc/profile.d/conda.sh", shell=True)
#     subprocess.run("conda activate datacube", shell=True)
#     subprocess.run(f"rio cogeo validate {path}", shell=True)   
    
def compare_output_size(path_raster_1, path_raster_2):
    """Compare the result of the creation of 2 raster
    ex. the creation of cog from the rioxarray.clip and the rasterio.mask
    Results should be the same (return True)"""
    
    result_1 = rasterio.open(path_raster_1)
    result_2 = rasterio.open(path_raster_2)
    
    band_1 = result_1.read()
    band_2 = result_2.read()
    
    return np.size(band_1) == np.size(band_2)

def compare_output_values(path_raster_1, path_raster_2):
    """Compare the values of the creation 2 raster 
    ex. creation of cog from the rioxarray.clip and the rasterio.mask
    Results should be the same (return 0)
    This function can be use only if compare_output_size==True"""
    
    result_1 = rasterio.open(path_raster_1)
    result_2 = rasterio.open(path_raster_2)
    
    band_1 = result_1.read()
    band_2 = result_2.read()
    
    return np.size(band_1) - np.count_nonzero(band_1==band_2)
    
def compare_output_bigtiff(path_raster_1, path_raster_2):
    """Compare the values and dimension of 2 data-array
    Result should be the same (return True)"""
    
    result_1 = rioxarray.open_rasterio(path_raster_1, lock=False, chunks=True)    
    result_2 = rioxarray.open_rasterio(path_raster_2, lock=False, chunks=True)
    
    #xarray.align(resutl_1, result_2, join="exact")
    
    return result_1.equals(result_2) #This returns true is values and dimension are the same
