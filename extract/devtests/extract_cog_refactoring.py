# -*- coding: utf-8 -*-
"""
Created on Thu Mar 23 13:21:30 2023

@author: ccrevier

Test the new refactored extract_cog() flow
"""

import pathlib
import sys
import os

from rasterio import warp
from shapely.geometry import box, mapping, shape

root = pathlib.Path(__file__).parents[2]
if str(root) not in sys.path:
    sys.path.insert(0,str(root))


import ccmeo_datacube.extract as dce
import ccmeo_datacube.extract_cog as exc

def _handle_it():
    """
    https://datacube-prod-data-public.s3.amazonaws.com/store/elevation/hrdem/hrdem-lidar/NRCAN-Monteregie_2020-1m-dsm.tif
    https://datacube-prod-data-public.s3.amazonaws.com/store/elevation/hrdem/hrdem-lidar/QC-Prevention_risques_sinistres_2019-1m-dsm.tif
    https://datacube-prod-data-public.s3.amazonaws.com/store/elevation/hrdem/hrdem-lidar/GEOMONT-2013_est_RNCan-2m-dsm.tif
    https://datacube-prod-data-public.s3.amazonaws.com/store/elevation/hrdem/hrdem-lidar/QC-10-261_Yamaska_2011-1m-dsm.tif
    https://datacube-prod-data-public.s3.amazonaws.com/store/elevation/hrdem/hrdem-lidar/QC-2008_HAUT-RICHELIEU-2m-dsm.tif
    
    
    minicube
    End: 2023-03-27 14:18:51.607737
    Total time 60.571184 seconds
    [WindowsPath('C:/Users/ccrevier/Documents/Datacube/Temp/dev_dc_extract/extract_cog_refactoring/devtests/minicube/GEOMONT-2013_est_RNCan-2m-dsm-clip-3979-resample-4m.tif'), 
     WindowsPath('C:/Users/ccrevier/Documents/Datacube/Temp/dev_dc_extract/extract_cog_refactoring/devtests/minicube/NRCAN-Monteregie_2020-1m-dsm-clip-3979-resample-4m.tif'), 
     WindowsPath('C:/Users/ccrevier/Documents/Datacube/Temp/dev_dc_extract/extract_cog_refactoring/devtests/minicube/QC-Prevention_risques_sinistres_2019-1m-dsm-clip-3979-resample-4m.tif'), 
     WindowsPath('C:/Users/ccrevier/Documents/Datacube/Temp/dev_dc_extract/extract_cog_refactoring/devtests/minicube/QC-10-261_Yamaska_2011-1m-dsm-clip-3979-resample-4m.tif'), 
     WindowsPath('C:/Users/ccrevier/Documents/Datacube/Temp/dev_dc_extract/extract_cog_refactoring/devtests/minicube/QC-2008_HAUT-RICHELIEU-2m-dsm-clip-3979-resample-4m.tif')] minicube
    
    mosaic_date_desc
    End: 2023-03-27 14:19:23.685137
    Total time 32.0774 seconds
    [{WindowsPath('C:/Users/ccrevier/Documents/Datacube/Temp/dev_dc_extract/extract_cog_refactoring/devtests/mosaic_date_desc/hrdem-lidar_dsm_mosaic_3979_4m.tif'): 
      ['https://datacube-prod-data-public.s3.amazonaws.com/store/elevation/hrdem/hrdem-lidar/NRCAN-Monteregie_2020-1m-dsm.tif']}] mosaic_date_desc
    
    mosaic_date_asc
    End: 2023-03-27 14:19:48.255186
    Total time 24.570049 seconds
    [{WindowsPath('C:/Users/ccrevier/Documents/Datacube/Temp/dev_dc_extract/extract_cog_refactoring/devtests/mosaic_date_asc/hrdem-lidar_dsm_mosaic_3979_4m.tif'): 
      ['https://datacube-prod-data-public.s3.amazonaws.com/store/elevation/hrdem/hrdem-lidar/QC-10-261_Yamaska_2011-1m-dsm.tif']}] mosaic_date_asc
    
    mosaic_res_desc
    End: 2023-03-27 14:20:08.734314
    Total time 20.479128 seconds
    [{WindowsPath('C:/Users/ccrevier/Documents/Datacube/Temp/dev_dc_extract/extract_cog_refactoring/devtests/mosaic_res_desc/hrdem-lidar_dsm_mosaic_3979_4m.tif'): 
      ['https://datacube-prod-data-public.s3.amazonaws.com/store/elevation/hrdem/hrdem-lidar/GEOMONT-2013_est_RNCan-2m-dsm.tif', 
       'https://datacube-prod-data-public.s3.amazonaws.com/store/elevation/hrdem/hrdem-lidar/NRCAN-Monteregie_2020-1m-dsm.tif']}] mosaic_res_desc
    
    mosaic_res_asc
    End: 2023-03-27 14:20:33.199648
    Total time 24.465334 seconds
    [{WindowsPath('C:/Users/ccrevier/Documents/Datacube/Temp/dev_dc_extract/extract_cog_refactoring/devtests/mosaic_res_asc/hrdem-lidar_dsm_mosaic_3979_4m.tif'): 
      ['https://datacube-prod-data-public.s3.amazonaws.com/store/elevation/hrdem/hrdem-lidar/NRCAN-Monteregie_2020-1m-dsm.tif']}] mosaic_res_asc
    
    minicube
    End: 2023-03-27 16:36:37.615410
    Total time 86.943446 seconds
    [WindowsPath('C:/Users/ccrevier/Documents/Datacube/Temp/dev_dc_extract/extract_cog_refactoring/devtests/minicube/GEOMONT-2013_est_RNCan-2m-dsm-clip-2960-resample-4m.tif'), 
     WindowsPath('C:/Users/ccrevier/Documents/Datacube/Temp/dev_dc_extract/extract_cog_refactoring/devtests/minicube/NRCAN-Monteregie_2020-1m-dsm-clip-2960-resample-4m.tif'), 
     WindowsPath('C:/Users/ccrevier/Documents/Datacube/Temp/dev_dc_extract/extract_cog_refactoring/devtests/minicube/QC-Prevention_risques_sinistres_2019-1m-dsm-clip-2960-resample-4m.tif'), 
     WindowsPath('C:/Users/ccrevier/Documents/Datacube/Temp/dev_dc_extract/extract_cog_refactoring/devtests/minicube/QC-10-261_Yamaska_2011-1m-dsm-clip-2960-resample-4m.tif'), 
     WindowsPath('C:/Users/ccrevier/Documents/Datacube/Temp/dev_dc_extract/extract_cog_refactoring/devtests/minicube/QC-2008_HAUT-RICHELIEU-2m-dsm-clip-2960-resample-4m.tif')] minicube    
    
    mosaic_date_desc
    End: 2023-03-27 16:33:15.124298
    Total time 38.56308 seconds
    [{WindowsPath('C:/Users/ccrevier/Documents/Datacube/Temp/dev_dc_extract/extract_cog_refactoring/devtests/mosaic_date_desc/hrdem-lidar_dsm_mosaic_2960_4m.tif'): 
      ['https://datacube-prod-data-public.s3.amazonaws.com/store/elevation/hrdem/hrdem-lidar/NRCAN-Monteregie_2020-1m-dsm.tif']}] mosaic_date_desc
    
    mosaic_date_asc
    End: 2023-03-27 16:34:09.044773
    Total time 53.920475 seconds
    [{WindowsPath('C:/Users/ccrevier/Documents/Datacube/Temp/dev_dc_extract/extract_cog_refactoring/devtests/mosaic_date_asc/hrdem-lidar_dsm_mosaic_2960_4m.tif'): 
      ['https://datacube-prod-data-public.s3.amazonaws.com/store/elevation/hrdem/hrdem-lidar/QC-2008_HAUT-RICHELIEU-2m-dsm.tif', 
       'https://datacube-prod-data-public.s3.amazonaws.com/store/elevation/hrdem/hrdem-lidar/QC-10-261_Yamaska_2011-1m-dsm.tif', 
       'https://datacube-prod-data-public.s3.amazonaws.com/store/elevation/hrdem/hrdem-lidar/GEOMONT-2013_est_RNCan-2m-dsm.tif']}] mosaic_date_asc
    
    mosaic_res_desc
    End: 2023-03-27 16:34:46.535551
    Total time 37.490778 seconds
    [{WindowsPath('C:/Users/ccrevier/Documents/Datacube/Temp/dev_dc_extract/extract_cog_refactoring/devtests/mosaic_res_desc/hrdem-lidar_dsm_mosaic_2960_4m.tif'): 
      ['https://datacube-prod-data-public.s3.amazonaws.com/store/elevation/hrdem/hrdem-lidar/GEOMONT-2013_est_RNCan-2m-dsm.tif', 
       'https://datacube-prod-data-public.s3.amazonaws.com/store/elevation/hrdem/hrdem-lidar/QC-2008_HAUT-RICHELIEU-2m-dsm.tif', 
       'https://datacube-prod-data-public.s3.amazonaws.com/store/elevation/hrdem/hrdem-lidar/NRCAN-Monteregie_2020-1m-dsm.tif']}] mosaic_res_desc
    
    mosaic_res_asc
    End: 2023-03-27 16:41:24.106639
    Total time 42.465732 seconds
    [{WindowsPath('C:/Users/ccrevier/Documents/Datacube/Temp/dev_dc_extract/extract_cog_refactoring/devtests/mosaic_res_asc/hrdem-lidar_dsm_mosaic_2960_4m.tif'): 
      ['https://datacube-prod-data-public.s3.amazonaws.com/store/elevation/hrdem/hrdem-lidar/NRCAN-Monteregie_2020-1m-dsm.tif']}] mosaic_res_asc    
    """

    # Minicube extraction
    params = extract_cog_params('minicube', 'EPSG:3979')
    result_minicube  = test_extract_cog(params)
    print(result_minicube, 'minicube')
    
    params = extract_cog_params('minicube', 'EPSG:3979', 'hrdem-lidar:dsm,landcover', resolution=None)
    result_minicube  = test_extract_cog(params)
    print(result_minicube, 'minicube')
    
    #mosaic
    params = extract_cog_params('mosaic_date_desc', 'EPSG:3979')
    result_mosaic = test_extract_cog(params)
    print(result_mosaic, 'mosaic_date_desc')
  
    params = extract_cog_params('mosaic_date_asc', 'EPSG:3979')
    result_mosaic = test_extract_cog(params)
    print(result_mosaic, 'mosaic_date_asc')
    
    params = extract_cog_params('mosaic_res_desc', 'EPSG:3979')
    result_mosaic = test_extract_cog(params)
    print(result_mosaic, 'mosaic_res_desc')
    
    params = extract_cog_params('mosaic_res_asc', 'EPSG:3979')
    result_mosaic = test_extract_cog(params)
    print(result_mosaic, 'mosaic_res_asc')
  
    # Minicube extraction
    params = extract_cog_params('minicube', 'EPSG:2960')
    result_minicube  = test_extract_cog(params)
    print(result_minicube, 'minicube')
    
    #mosaic
    params = extract_cog_params('mosaic_date_desc', 'EPSG:2960')
    result_mosaic = test_extract_cog(params)
    print(result_mosaic, 'mosaic_date_desc')
  
    params = extract_cog_params('mosaic_date_asc', 'EPSG:2960')
    result_mosaic = test_extract_cog(params)
    print(result_mosaic, 'mosaic_date_asc')
    
    params = extract_cog_params('mosaic_res_desc', 'EPSG:2960')
    result_mosaic = test_extract_cog(params)
    print(result_mosaic, 'mosaic_res_desc')
    
    params = extract_cog_params('mosaic_res_asc', 'EPSG:2960')
    result_mosaic = test_extract_cog(params)
    print(result_mosaic, 'mosaic_res_asc')
    
    #test with multiple collection
    params = extract_cog_params('mosaic_res_asc', 'EPSG:3979', 'hrdem-lidar:dsm,landcover')
    result_mosaic = test_extract_cog(params)
    print(result_mosaic, 'mosaic_res_asc')
    
    
    #test with multiple collection
    params = extract_cog_params('mosaic_res_asc', 'EPSG:3979', 'hrdem-lidar')
    result_mosaic = test_extract_cog(params)
    print(result_mosaic, 'mosaic_res_asc')
    
    #list of the functions to run when the code is ran
    # test_calc_res_origin_valid_inputs()
    # test_output_dimension()
    return

def extract_cog_params(extract_type, out_crs, collection='hrdem-lidar:dsm', resolution=4):
    # collection = 'hrdem-lidar:dsm'
    bbox = '1708821, -107045, 1713821, -102045'
    bbox_crs = 'EPSG:3979' 
    # resolution = 4
    # out_crs = 'EPSG:3979'
    method = 'bilinear'
    out_dir = r'C:\Users\ccrevier\Documents\Datacube\Temp\dev_dc_extract\extract_cog_refactoring\issue-123\12-04-23'
    overviews=False
    mosaic = True
    orderby_date = 'date'
    orderby_res = 'resolution'
    desc = True
    asc = False
    
    if extract_type == 'minicube':
        params={'collections':collection,
        		'bbox':bbox,
        		'bbox_crs':bbox_crs,
        		'resolution':resolution,
        		'out_crs':out_crs,
        		'method':method,
        		'out_dir':out_dir+os.sep+'minicube',
        		'overviews':overviews}
    elif extract_type == 'mosaic_date_desc':
        params = {'collections':collection,
        		'bbox':bbox,
        		'bbox_crs':bbox_crs,
        		'resolution':resolution,
        		'out_crs':out_crs,
        		'method':method,
        		'out_dir':out_dir+os.sep+'mosaic_date_desc',
        		'overviews':overviews, 
                'mosaic':mosaic,
                'orderby':orderby_date,
                'desc':desc}
        
    elif extract_type == 'mosaic_date_asc':
        params = {'collections':collection,
        		'bbox':bbox,
        		'bbox_crs':bbox_crs,
        		'resolution':resolution,
        		'out_crs':out_crs,
        		'method':method,
        		'out_dir':out_dir+os.sep+'mosaic_date_asc',
        		'overviews':overviews, 
                'mosaic':mosaic,
                'orderby':orderby_date,
                'desc':asc}
    elif extract_type == 'mosaic_res_desc':
        params = {'collections':collection,
        		'bbox':bbox,
        		'bbox_crs':bbox_crs,
        		'resolution':resolution,
        		'out_crs':out_crs,
        		'method':method,
        		'out_dir':out_dir+os.sep+'mosaic_res_desc',
        		'overviews':overviews, 
                'mosaic':mosaic,
                'orderby':orderby_res,
                'desc':desc}
    elif extract_type == 'mosaic_res_asc':
        params = {'collections':collection,
        		'bbox':bbox,
        		'bbox_crs':bbox_crs,
        		'resolution':resolution,
        		'out_crs':out_crs,
        		'method':method,
        		'out_dir':out_dir+os.sep+'mosaic_res_asc',
        		'overviews':overviews, 
                'mosaic':mosaic,
                'orderby':orderby_res,
                'desc':asc}
    
    return params



def test_extract_cog(params):
    
    result = exc.extract_cog(**params)
    
    return result

    
if __name__ == '__main__':
    _handle_it()