# -*- coding: utf-8 -*-
"""
Created on Tue Oct 25 12:43:55 2022

@author: ccrevier
"""
import rasterio

import rioxarray
import threading
import pathlib
from rasterio.transform import from_origin

import ccmeo_datacube.extract.extract as dce

src_file = r'C:\Users\ccrevier\Documents\Datacube\git_files\dc_extract\extract\devtests\test-images\extract-5-1-bilinear-3979.tif'
dex = dce.DatacubeExtract()

@dce.print_time
def extract_vrt_tap(img_path,  bbox, bbox_crs, out_path):
    """Allow to define out parameters before execution of the vrt. 
    Could be used to define the paramters of the mini cube to extract so all out file have the same extent and resolution (same grid)"""
    # out_file = r'/gpfs/fs5/nrcan/nrcan_geobase/work/transfer/work/datacube/DEV_CC/git_files/dc_extract/extract/devtests/test-images/extract-5-1-bilinear-3979-rioxarray-vrt_tap_15x15-native-100-2.tif'

    # img_path = tbw.test_cog('fine')
    env = rasterio.Env(
        GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR", # a way to disable loading of side-car or auxiliary files
        CPL_VSIL_CURL_USE_HEAD=False, #pas certaine de ce que ca fait
        CPL_VSIL_CURL_ALLOWED_EXTENSIONS="TIF", #considering only files that ends with .tif 
        GDAL_NUM_THREADS="ALL_CPUS" #Enable multi-threaded compression by specifying the number of worker threads
    )
    img = rasterio.open(img_path)
    # bbox, bbox_crs = tbw.test_bbox_dynamic(x_km=size, y_km=size)
    bbox_full = dce.tap_window(img.transform, bbox, bbox_crs)
    
    (minx_full,
     miny_full,
     maxx_full,
     maxy_full) = rasterio.windows.bounds(bbox_full,
                                          img.transform)

    dex = dce.DatacubeExtract()
    bbox_full_transform = from_origin(minx_full,maxy_full,img.res[0],img.res[0])
    out_profile = img.profile
    out_profile.update({'crs':img.crs,
                        'height':bbox_full.height,
                        'width':bbox_full.width,
                        'transform':bbox_full_transform, 
                        })
    
    file_size = dex.calculate_file_size(out_profile['dtype'], out_profile['width'], out_profile['height'])
    if file_size >= 3:
        out_profile['BIGTIFF']='YES'
        print('Outfile is bigger than 4GB, creation of BIGTIFF...')
    else :
        # kwargs['BIGTIFF']='NO'
        print('Outfile is smaller than 4GB, creation of TIFF...')
    #TODO : Add dynamic use of bigtiff

    with env:
        with rasterio.open(img_path) as src:
            # profile = src.profile.copy()
            with rasterio.vrt.WarpedVRT(src, **out_profile) as vrt:
                cog = rioxarray.open_rasterio(vrt, lock=False, chunks=True)
                dst = cog.rio.to_raster(out_path, windowed=True, lock=threading.Lock(), **out_profile)
                # rio_shutil.copy(vrt, out_file, driver='GTiff',**vrt_options) 
    return out_path

@dce.print_time
def extract_by_rio(self, img_path:str,
                     out_file:str,
                     bbox:str,
                     bbox_crs:str,
                     out_res:float=None,
                     suffix:str=None,
                     out_crs:str='ESPG:3979',
                     resample:str='bilinear',
                     overviews:bool=False,
                     blocksize:int=512)->str:
     """Combine the rioxarray method with the tap window develop during the modify by window process
     Will probably replace the clip to grid function in the future"""

     with rasterio.Env():
         original = pathlib.Path(out_file)
         if suffix:
             file_name = out_file.parent /f'{out_file.stem}_{suffix}{out_file.suffix}'
         else:
             file_name = original

         # """Resamples of reprojects by window"""
         img = rasterio.open(img_path)
         resample = dce.resample_enum(resample)
         in_profile = img.profile

         # Ensure the out_crs and out_res are properly set
         if out_crs:
             if isinstance(out_crs,str):
                 out_crs = rasterio.crs.CRS.from_string(out_crs)
         else:
             out_crs = img.crs

         if not out_res:
             out_res = img.res[0]

         bbox_full = dce.tap_window(img.transform,bbox,bbox_crs) #return rasterio.window
         kwargs, clipped = dex.read_rioxarray(bbox_full,
                                            clip_file=img_path,
                                            cog_profile=in_profile,
                                            blocksize=blocksize,
                                            resolution=out_res)

         file_name = dex.save_xarray_as_raster(file_name,
                                                 arr=clipped,
                                                 kwargs=kwargs,
                                                 blocksize=blocksize,
                                                 overviews=overviews,
                                                 resampling_method=resample)
     return file_name