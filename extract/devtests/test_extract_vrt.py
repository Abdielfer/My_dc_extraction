#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 11 13:39:39 2022

@author: chc002
"""
import numpy as np
import rasterio
from rasterio.warp import calculate_default_transform, aligned_target, reproject, Resampling
import geopandas
from rasterio.windows import Window
import math
import affine
from rasterio import shutil as rio_shutil
import rioxarray
import threading
import xarray
from rasterio.transform import Affine, from_origin

import ccmeo_datacube.extract.devtests.test_block_windows as tbw
import ccmeo_datacube.extract.extract as dce

src_file = r'C:\Users\ccrevier\Documents\Datacube\git_files\dc_extract\extract\devtests\test-images\extract-5-1-bilinear-3979.tif'


@dce.print_time
def reproject_vrt_tap():
    """Allow to define out parameters before execution of the vrt. 
    Could be used to define the paramters of the mini cube to extract so all out file have the same extent and resolution (same grid)"""
    out_file = r'/gpfs/fs5/nrcan/nrcan_geobase/work/transfer/work/datacube/DEV_CC/git_files/dc_extract/extract/devtests/test-images/extract-5-1-bilinear-3979-rioxarray-vrt_tap_15x15.tif'

    img_path = tbw.test_cog('fine')
    env = rasterio.Env(
        GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR",
        CPL_VSIL_CURL_USE_HEAD=False,
        CPL_VSIL_CURL_ALLOWED_EXTENSIONS="TIF",
    )
    img = rasterio.open(img_path)
    bbox, bbox_crs = tbw.test_bbox_dynamic(x_km=15, y_km=15)
    bbox_full = dce.tap_window(img.transform, bbox, bbox_crs)


    dex = dce.DatacubeExtract()
    # Rerpoject windows bounds to out crs
    bounds_bbox_full = rasterio.windows.bounds(bbox_full, img.transform)
    geom_d = dex.poly_to_dict(dex.bbox_to_poly(
        dex.tuple_to_bbox(bounds_bbox_full)))
    geom_p = dex.transform_dict_to_poly(geom_d, 'EPSG:3979', 'EPSG:2960')

    # Retap the new bbox that isnow is the new projection
    # geom_proj = dex.tuple_to_bbox(dex.dict_to_tuple(geom_p))
    # geom_proj_tap = dce.tap_window(geom_proj, )
    # TODO : tapped geom
    # TODO : force resolution to 1 (or define resolution)

    # bbox= ','.join([str(i) for i in geom_p.bounds])

    # Destination CRS is Web Mercator
    dst_crs = rasterio.crs.CRS.from_epsg(2960)

    # Output image transform
    left, bottom, right, top = geom_p.bounds
    res = 1
    dst_width = math.ceil((right - left) / res)
    dst_height = math.ceil((top - bottom) / res)
    dst_transform = affine.Affine(res, 0.0, math.floor(left),
                                  0.0, -res, math.floor(top))

    vrt_options = {
        'resampling': Resampling.bilinear,
        'crs': dst_crs,
        'transform': dst_transform,
        'height': dst_height,
        'width': dst_width,
        'blocksize': 512,
        'tiled': True
    }

    with env:
        with rasterio.open(img_path) as src:
            profile = src.profile.copy()
            with rasterio.vrt.WarpedVRT(src, **vrt_options) as vrt:
                cog = rioxarray.open_rasterio(vrt, lock=False, chunks=True)
                dst = cog.rio.to_raster(out_file, windowed=True, lock=threading.Lock(), **vrt_options)
                # rio_shutil.copy(vrt, out_file, driver='GTiff',**vrt_options) 
    return

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
        # GDAL_NUM_THREADS=1 #Enable multi-threaded compression by specifying the number of worker threads
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
    img.close()
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
            profile = src.profile.copy()
            with rasterio.vrt.WarpedVRT(src, **out_profile) as vrt:
                cog = rioxarray.open_rasterio(vrt, lock=False, chunks=True)
                dst = cog.rio.to_raster(out_path, windowed=True, lock=threading.Lock(), **out_profile)
                # rio_shutil.copy(vrt, out_file, driver='GTiff',**vrt_options) 
                cog.close()
    return out_path

"""
Gdal wapred vrt option
<GDALWarpOptions>
    <WarpMemoryLimit>6.71089e+07</WarpMemoryLimit>
    <ResampleAlg>NearestNeighbour</ResampleAlg>
    <WorkingDataType>Byte</WorkingDataType>
    <Option name="INIT_DEST">0</Option>
    <SourceDataset relativeToVRT="1">byte.vrt</SourceDataset>
    <Transformer>
    <ApproxTransformer>
        <MaxError>0.125</MaxError>
        <BaseTransformer>
        <GenImgProjTransformer>
            <SrcGeoTransform>440720,60,0,3751320,0,-60</SrcGeoTransform>
            <SrcInvGeoTransform>-7345.33333333333303,0.0166666666666666664,0,62522,0,-0.0166666666666666664</SrcInvGeoTransform>
            <DstGeoTransform>440720,60,0,3751320,0,-60</DstGeoTransform>
            <DstInvGeoTransform>-7345.33333333333303,0.0166666666666666664,0,62522,0,-0.0166666666666666664</DstInvGeoTransform>
        </GenImgProjTransformer>
        </BaseTransformer>
    </ApproxTransformer>
    </Transformer>
    <BandList>
    <BandMapping src="1" dst="1" />
    </BandList>
</GDALWarpOptions>
"""