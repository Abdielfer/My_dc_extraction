# -*- coding: utf-8 -*-
"""
Created on Mon Sep 26 14:03:19 2022

Test the extraction, clipping, resample (not quit yet), reproject(not quite yet) using rioxarray library
to compare with the extract, resample and reproject with window based

For the moment, validate the extract.extract_by_rio()

@author: ccrevier
"""
debug=False


import rasterio
import pathlib
from rasterio.transform import from_origin
import rioxarray
import numpy as np
from rasterio.warp import aligned_target, calculate_default_transform


import ccmeo_datacube.extract.extract as dce
dex = dce.DatacubeExtract(debug=debug)

#Basic function to define parameters
#TODO : put parameters inside one python module that we can call everytime
def test_cog(res='med'):
    """Sample cog to run tests on"""
    if res == 'fine':
        return 'https://datacube-stage-data-public.s3.ca-central-1.amazonaws.com/store/elevation/hrdem-hrdsm/hrdem/hrdem-nb-dem.tif'
    elif res == 'med': 
        return 'http://datacube-prod-data-public.s3.ca-central-1.amazonaws.com/store/elevation/cdem-cdsm/cdem/cdem-canada-dem.tif'
    else:
        print('no res specified')
        
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


def test_extract_by_rio():
    sizes = [5]
    ress = [1]
    # ress = [16, 20, 30, 32]
    resamples = ['bilinear']
    # resamples = ['bilinear', 'nearest', 'average']
    epsgs = [3979]
    # epsgs = [3979, 2960]
    # ress = [ress[1]]
    # resamples = [resamples[0]]
    for size in sizes:
        bbox,bbox_crs = test_bbox_dynamic(x_km=size, y_km=size)
        for resample in resamples:
            for res in ress:
                for epsg in epsgs:
                    name = f'extract-{size}-{res}-{resample}-{epsg}.tif'
                    f = pathlib.Path(__file__).parent.joinpath('test-images')
                    if not f.is_dir():
                        f.mkdir(parents=True,exist_ok=True)
                    f = f.joinpath(name)
                    
                    params = {
                        'img_path':test_cog('fine'),
                        'out_file':f,
                        'bbox':bbox,
                        'bbox_crs':bbox_crs,
                        'out_res':res,
                        'out_crs':f'EPSG:{epsg}',
                        'resample':resample
                        }
                    dex.extract_by_rio(**params)
    return

#not working yet
def reproject_by_rio(img_path:str,
                    out_path:str,
                    bbox:tuple,
                    bbox_crs:str,
                    out_res:float=None,
                    out_crs:str=None,
                    resample:str='bilinear'):
    """Steps :
        Test #1
    1) transform the bbox into img crs (est-ce que les données sur le cube sont tap??? ou il y a un resample?))
    2) tap la bbox
    3) clip to bbox
    4) reproject 
    
    Si ça marche pas:
        Test#2
    1) transform .....
    2) tap la bbox
    3) get les block windows
    4) reproject to out_crs les block_window
    5) reproj bbox to out_crs
    6) clip block_window to the reproject bbox in out_crs"""
    
    #bbox is already in good crs
    dex = dce.DatacubeExtract()
    with rasterio.Env():
        # """Resamples of reprojects by window"""
        img = rasterio.open(img_path)
    
        # Convert string to rasterio resample enum
        resample = dce.resample_enum(resample)
        
        # Image profile
        in_profile = img.profile
        
        # Ensure the out_crs and out_res are properly set
        if out_crs:
            # Ensure out_crs is rasterio.crs.CRS and not str
            if isinstance(out_crs,str):
                # Convert to rasterio.crs.CRS
                out_crs = rasterio.crs.CRS.from_string(out_crs)
        else:
            # No out crs defined, want native crs
            out_crs = img.crs
            
        if not out_res:
            # No out_res provided, want native resolution
            out_res = img.res[0]

        #The bbox tap is created inside the dce.bbox_windows()
        # bbox_full in native image crs
        bbox_full = dce.tap_window(img.transform,bbox,bbox_crs) #return rasterio.window
    
        kwargs, clipped = dex.read_rioxarray(bbox_full,
                                           clip_file=img_path,
                                           cog_profile=in_profile,
                                           blocksize=512,
                                           resolution=out_res)
        
        xmin, ymin, xmax, ymax = clipped.rio.bounds()
        dst_transform,dst_width,dst_height = tap_params(in_crs=img.crs,
                                                       out_crs=out_crs,
                                                       in_left=xmin,
                                                       in_bottom=ymin,
                                                       in_right=xmax,
                                                       in_top=ymax,
                                                       in_width=clipped.rio.width,
                                                       in_height=clipped.rio.height,
                                                       out_res=out_res)
        #Test NOT WORKING
        test_4 = clipped.rio.reproject(dst_crs = out_crs,
                          # resolution = out_res,
                           transform = dst_transform,
                          # shape=(kwargs['height'], kwargs['width']),
                          resampling = resample, nodata=clipped.rio.nodata)
        
        kwargs = dex.modify_kwargs(kwargs=kwargs,
                                    dst_crs=out_crs,
                                    dst_transform=dst_transform,
                                    dst_w=dst_width,
                                    dst_h=dst_height,
                                    blocksize=512)
        
        file_name = dex.save_xarray_as_raster(file_name=out_path,
                                                arr=test_4, 
                                                kwargs=kwargs,
                                                blocksize=512,
                                                overviews=False, 
                                                resampling_method=resample)
        
        # dst = np.zeros((kwargs['height'], kwargs['width']), dtype=np.uint16)
        # test = rasterio.warp.reproject(
        #     source=clipped.values,
        #     destination=dst,
        #     src_transform=in_profile,
        #     src_crs=img.crs,
        #     src_nodata=img.nodata,
        #     dst_transform=kwargs['transform'],
        #     **kwargs,
        # )
    return

def tap_params(in_crs:str,
               out_crs:str,
               in_left:float,
               in_bottom:float,
               in_right:float,
               in_top:float,
               in_width:float,
               in_height:float,
               out_res:int):
    """Returns the tap transform, width and height"""

    # Step 1 of rasterio way to generate a tap warp transform
    (interim_transform, 
     interim_width, 
     interim_height) = calculate_default_transform(src_crs=in_crs, 
                                           dst_crs=out_crs, 
                                           width=in_width, 
                                           height=in_height,
                                           left=in_left,
                                           bottom=in_bottom,
                                           right=in_right,
                                           top=in_top,
                                           resolution=out_res)
    # Step 2 of rasterio way to da a tap warp
    dst_transform,dst_width,dst_height = aligned_target(transform=interim_transform,
                                                width=interim_width,
                                                height=interim_height,
                                                resolution=out_res)

    return dst_transform,dst_width,dst_height

####OLD FUNCTION
# def extract_by_rio(img_path:str,
#                     out_path:str,
#                     bbox:tuple,
#                     bbox_crs:str,
#                     out_res:float=None,
#                     out_crs:str=None,
#                     resample:str='bilinear'):
#     """Combine the rioxarray method with the tap window develop during the modify by window process"""
#     dex = dce.DatacubeExtract()
#     with rasterio.Env():
#         # """Resamples of reprojects by window"""
#         img = rasterio.open(img_path)
    
#         # Convert string to rasterio resample enum
#         resample = dce.resample_enum(resample)
        
#         # Image profile
#         in_profile = img.profile
        
#         # Ensure the out_crs and out_res are properly set
#         if out_crs:
#             # Ensure out_crs is rasterio.crs.CRS and not str
#             if isinstance(out_crs,str):
#                 # Convert to rasterio.crs.CRS
#                 out_crs = rasterio.crs.CRS.from_string(out_crs)
#         else:
#             # No out crs defined, want native crs
#             out_crs = img.crs
            
#         if not out_res:
#             # No out_res provided, want native resolution
#             out_res = img.res[0]

#         #The bbox tap is created inside the dce.bbox_windows()
#         # bbox_full in native image crs
#         bbox_full = dce.tap_window(img,bbox,bbox_crs) #return rasterio.window
#         kwargs, clipped = dex.read_rioxarray(bbox_full,
#                                            clip_file=img_path,
#                                            cog_profile=in_profile,
#                                            file_name=out_path,
#                                            blocksize=512,
#                                            resolution=out_res)
#         file_name = dex.save_xarray_as_raster(out_path,
#                                                 arr=clipped, 
#                                                 kwargs=kwargs,
#                                                 blocksize=512,
#                                                 overviews=False, 
#                                                 resampling_method='nearest')
    
#     return

