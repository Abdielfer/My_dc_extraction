#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 14 15:22:43 2023

@author: chc002


Discovered that the rasterio.warp.aligned_target gives different results based on input resolution
This is not the intended behavior if we want to have the same out_profile no mather the input resolution of the file

Bunch of tests to compare the result of the compbinaison of 
rasterio.warp.calculate_default_transform and rasterio.warp.aligned_target (which is the recommanded steps)
to evaluate what we want to use to tap, cap the bbox independently from the input resolution or crs
"""
import rasterio
import math
import geopandas as gpd

import ccmeo_datacube.extract.extract as dce
dex = dce.DatacubeExtract()
#les différents tests que nous voulons faire
class parameters():
    
    
    # bbox = '2150520.4724999964, 142500.05299999937,'\
    #      ' 2151530.4724999964, 143885.05299999937'
    bbox = '1708821, -107045, 1713821, -102045'
    bbox_crs = 'EPSG:3979'
    
    out_crs_1 = None
    out_crs_2 = 'EPSG:2960'
    
    out_res_1 = None
    out_res_2 = 4
    
    in_path_2m = 'https://datacube-prod-data-public.s3.amazonaws.com/store/elevation/hrdem/hrdem-lidar/GEOMONT-2013_est_RNCan-2m-dsm.tif'
    in_path_1m = 'https://datacube-prod-data-public.s3.amazonaws.com/store/elevation/hrdem/hrdem-lidar/NRCAN-Monteregie_2020-1m-dsm.tif'
    
    def get_params_natif(self):
        params_1m = {'bbox':self.bbox,
                  'bbox_crs':self.bbox_crs,
                  'out_crs':self.out_crs_1,
                  'out_res':self.out_res_1,
                  'in_path':self.in_path_1m}
        
        params_2m = {'bbox':self.bbox,
                  'bbox_crs':self.bbox_crs,
                  'out_crs':self.out_crs_1,
                  'out_res':self.out_res_1,
                  'in_path':self.in_path_2m}
        
        return params_1m, params_2m
    
    
    def get_params_reproject(self):
        params_1m = {'bbox':self.bbox,
                  'bbox_crs':self.bbox_crs,
                  'out_crs':self.out_crs_2,
                  'out_res':self.out_res_1,
                  'in_path':self.in_path_1m}
        
        params_2m = {'bbox':self.bbox,
                  'bbox_crs':self.bbox_crs,
                  'out_crs':self.out_crs_2,
                  'out_res':self.out_res_1,
                  'in_path':self.in_path_2m}
        
        return params_1m, params_2m
    
    
    def get_params_resample(self):
        params_1m = {'bbox':self.bbox,
                  'bbox_crs':self.bbox_crs,
                  'out_crs':self.out_crs_1,
                  'out_res':self.out_res_2,
                  'in_path':self.in_path_1m}
        
        params_2m = {'bbox':self.bbox,
                  'bbox_crs':self.bbox_crs,
                  'out_crs':self.out_crs_1,
                  'out_res':self.out_res_2,
                  'in_path':self.in_path_2m}
        
        return params_1m, params_2m
    
    
    def get_params_resample_reproject(self):
        params_1m = {'bbox':self.bbox,
                  'bbox_crs':self.bbox_crs,
                  'out_crs':self.out_crs_2,
                  'out_res':self.out_res_2,
                  'in_path':self.in_path_1m}
        
        params_2m = {'bbox':self.bbox,
                  'bbox_crs':self.bbox_crs,
                  'out_crs':self.out_crs_2,
                  'out_res':self.out_res_2,
                  'in_path':self.in_path_2m}
        
        return params_1m, params_2m
    
    

def launch_get_extract_params():
    params_1m, params_2m = parameters().get_params_resample_reproject()
    out_profile_1m, extract_params_1m = dce.get_extract_params(**params_1m)
    out_profile_2m, extract_params_2m = dce.get_extract_params(**params_2m)
    return

def validate_input_tap_bbox_bounds(in_path, bbox, bbox_crs):
    """
    example of call 
    bounds_bbox_1m = validate_input_tap_bbox_bounds(parameters().in_path_1m, bbox, bbox_crs)
    """
    
    #Validate that, no mather the input file, the bbox coords are the same
    with rasterio.open(in_path) as img:
    
        in_crs = img.crs #should be the same for both images
        in_res = img.res[0]
        in_transform = img.transform
        in_profile = img.profile
        
    bbox_full = dce.tap_window(in_transform, bbox, bbox_crs, in_crs)
    bounds_bbox_full = rasterio.windows.bounds(bbox_full, in_transform)
    return bounds_bbox_full

#Conclusion : the tap bbox created from the 2 images with different res are not the same 
# Input bbox : '1708821, -107045, 1713821, -102045'
# 1m - bbox bounds after going through dce.tap_window : (1708821.0, -107046.0, 1713821.0, -102045.0)
# 2m - bbox bounds after going through dce.tap_window : (1708820.0, -107046.0, 1713820.0, -102044.0)

def test_tap_window(in_path, bbox, bbox_crs, index): 
    """
    example of call 
    w_orig_1m, w_tapish_1m, bounds_bbox_1m = test_tap_window(parameters().in_path_1m, bbox, bbox_crs, '1m')
    """
    dex = dce.DatacubeExtract()
    bbox = dex.bbox_to_tuple(bbox)
    #Validate that, no mather the input file, the bbox coords are the same
    with rasterio.open(in_path) as img:
    
        in_crs = img.crs #should be the same for both images
        # in_res = img.res[0]
        in_transform = img.transform
        # in_profile = img.profile
        
    # tap_window = dce.tap_window(in_transform, bbox, bbox_crs, in_crs)
    w_orig = rasterio.windows.from_bounds(*bbox,transform=in_transform)
    # win_orig_1m = Window(col_off=84389.0, row_off=32189.0, width=5000.0, height=5000.0)
    # win_orig_2m = Window(col_off=16430.5, row_off=33736.5, width=2500.0, height=2500.0)
    
    ceil = {'op':'ceil', 'pixel_precision':None}
    floor = {'op':'floor', 'pixel_precision':None}
    w_tapish = w_orig.round_lengths(**ceil)
    w_tapish = w_tapish.round_offsets(**floor)
    
    # win_tapish_1m = Window(col_off=84389, row_off=32189, width=5000, height=5000)
    # win_tapish_2m = Window(col_off=16430, row_off=33736, width=2500, height=2500)
    
    bounds_bbox_full = rasterio.windows.bounds(w_tapish, in_transform)
    
    # #Get the bbox in geojson
    geom_p_dict = dex.poly_to_dict(dex.bbox_to_poly(dex.tuple_to_bbox(bounds_bbox_full)))
    dex.dict_to_vector_file(geom_p_dict, in_crs, '/gpfs/fs5/nrcan/nrcan_geobase/work/transfer/work/datacube/DEV_CC/dev_dc_extract/debug_tap_params', f'bbox_full_{index}.geojson')
    # ##
    # bounds_bbox_1m = (1708821.0, -107045.0, 1713821.0, -102045.0)
    # bounds_bbox_2m = (1708820.0, -107044.0, 1713820.0, -102044.0)
    
    
    return w_orig, w_tapish, bounds_bbox_full

def mod_out_profile():
    bbox = '1708821, -107045, 1713821, -102045'
    bbox_crs = 'EPSG:3979'
    
    files = ['https://datacube-prod-data-public.s3.amazonaws.com/store/elevation/hrdem/hrdem-lidar/GEOMONT-2013_est_RNCan-2m-dsm.tif',
        'https://datacube-prod-data-public.s3.amazonaws.com/store/elevation/hrdem/hrdem-lidar/NRCAN-Monteregie_2020-1m-dsm.tif']
    	
    out_crs='EPSG:3979'
    resolution=4
    method=None
    
    list_params=[]
    list_profile=[]
    for file in files:
    	profile, params = dce.get_extract_params(in_path=file,
    											 out_crs=out_crs,
    											 out_res=resolution,
    											 bbox=bbox,
    											 bbox_crs=bbox_crs,
    											 resampling_method=method)
    	list_params.append(params)
    	list_profile.append(profile)
    
def test():  
    dex = dce.DatacubeExtract()
    dst_res = 4
    clip_bbox = '1708821, -107045, 1713821, -102045'
    clip_bbox_crs = 'EPSG:3979'
    files = ['https://datacube-prod-data-public.s3.amazonaws.com/store/elevation/hrdem/hrdem-lidar/GEOMONT-2013_est_RNCan-2m-dsm.tif',
        'https://datacube-prod-data-public.s3.amazonaws.com/store/elevation/hrdem/hrdem-lidar/NRCAN-Monteregie_2020-1m-dsm.tif']
    xs = []
    ys = []
    #define the out_bounds based on the input window from all files 
    for file in files:
        with dce.rasterio.Env():
            with dce.rasterio.open(file) as c:
                print(c.name)
                print(c.transform)
                print(dce.tap_window(c.transform,clip_bbox,clip_bbox_crs,'EPSG:3979'))
                print('***')
                # Getting bounds of output image
                left, bottom, right, top = c.bounds
            xs.extend([left, right])
            ys.extend([bottom, top])
    dst_w, dst_s, dst_e, dst_n = min(xs), min(ys), max(xs), max(ys)
    print("out image bounds in crs")
    print(dst_w,dst_s,dst_e,dst_n)
    print("out image transform")
    dst_transform = dce.rasterio.Affine.translation(dst_w, dst_n) * dce.rasterio.Affine.scale(dst_res, -dst_res)
    dst_transform_2 = dce.rasterio.transform.from_origin(dst_w,dst_n,dst_res,dst_res) #même chose que celui avant
    
    geom_p_dict = dex.poly_to_dict(dex.bbox_to_poly(dex.tuple_to_bbox((dst_w,dst_s,dst_e,dst_n))))
    dex.dict_to_vector_file(geom_p_dict, 'EPSG:3979', r'C:\Users\ccrevier\Documents\Datacube\Temp\dev_dc_extract\debug_tap_params', f'bbox_full_dst.geojson')
    
    
    # tap window with the new out_bound
    index=2
    for file in files:
        with dce.rasterio.Env():
            with dce.rasterio.open(file) as c:
                
                bbox_full = dce.tap_window(c.transform, f'{dst_w}, {dst_s}, {dst_e}, {dst_n}', 'EPSG:3979', c.crs)
                bounds_bbox_full = dce.rasterio.windows.bounds(bbox_full, c.transform)
                print(bbox_full)
                geom_p_dict = dex.poly_to_dict(dex.bbox_to_poly(dex.tuple_to_bbox(bounds_bbox_full)))
                dex.dict_to_vector_file(geom_p_dict, 'EPSG:3979', r'C:\Users\ccrevier\Documents\Datacube\Temp\dev_dc_extract\debug_tap_params', f'bbox_full_{index}.geojson')
                index=index-1


def get_df(collection, bbox, bbox_crs, asset, datetime_filter=None):
    df = dce.asset_urls(collection,
                        bbox,
                        bbox_crs,
                        asset_id=asset,
                        datetime_filter=datetime_filter)
    #Use of the order_by with res to get the resolution
    df,urls = dce.order_by(df, method='resolution')
    return df


        
#######################################
#This is the modified flow for the extract_cog

bbox = '1708821, -107045, 1713821, -102045'
bbox_crs = 'EPSG:3979'
params = {'collections':'hrdem-lidar',
			'bbox' : bbox,
			'bbox_crs':bbox_crs,
			'asset_id':'dsm'}
df = dce.asset_urls(**params)

urls = [url for url in df.url]


        