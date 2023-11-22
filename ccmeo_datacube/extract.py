"""
#!/usr/bin/python3
DESCRIPTION:
------------
Datacube data extraction, merging and mini-cube generation

REFERENCES:
-----------
https://rasterio.readthedocs.io/en/latest/topics/windowed-rw.html
https://numpy.org/doc/stable/reference/routines.statistics.html
https://gdal.org/drivers/raster/gtiff.html#georeferencing
https://rasterio.readthedocs.io/en/latest/topics/plotting.html
https://matplotlib.org/stable/gallery/subplots_axes_and_figures/subplots_demo.html
https://matplotlib.org/mpl_toolkits/mplot3d/tutorial.html
https://rasterio.readthedocs.io/en/latest/api/rasterio.merge.html
https://automating-gis-processes.github.io/CSC18/lessons/L6/raster-mosaic.html
https://automating-gis-processes.github.io/CSC18/lessons/L6/clipping-raster.html
https://numpy.org/doc/stable/reference/routines.statistics.html
https://numpy.org/doc/stable/user/basics.types.html
https://numpy.org/doc/stable/reference/generated/numpy.ndarray.astype.html
https://het.as.utexas.edu/HET/Software/Numpy/reference/generated/numpy.ndarray.astype.html
https://numpy.org/doc/stable/reference/generated/numpy.round_.html
https://www.geeksforgeeks.org/python-exit-commands-quit-exit-sys-exit-and-os-_exit/
https://rasterio.readthedocs.io/en/latest/api/rasterio.crs.html
https://rasterio.readthedocs.io/en/latest/api/rasterio.enums.html#rasterio.enums.Resampling

Developed by:
-------------
  Norah Brown - Natural Resources Canada,
  Charlotte Crevier - Natural Resources Canada,
  Marc-André Daviault - Natural Resources Canada,
  Siyu Li - University of Waterloo,
  Jean-François Bourgon - Natural Resources Canada
  Crown Copyright as described in section 12 of Copyright Act (R.S.C., 1985, c. C-42)
  © Her Majesty the Queen in Right of Canada, as represented by the Minister
  of Natural Resources Canada, 2022

"""
# Python standard library
from datetime import datetime
# from functools import wraps
import math
import os
import pathlib
from pathlib import Path
import re
import sys
from tempfile import TemporaryDirectory
from typing import Union, Tuple
import xml.etree.ElementTree as et



# Python custom modules under site-packages
import geopandas as gpd
import pandas
import numpy
import rasterio
from rasterio import warp
from rasterio.transform import Affine, from_origin
from rasterio.shutil import copy as rscopy
import requests
from shapely.geometry import box, mapping, shape
import shapely
import rioxarray
import threading
from rasterio.warp import aligned_target, calculate_default_transform

# Local code
# from describe.describe import nrcan_requests_ca_patch
_CHILD_LEVEL = 1
_DIR_NEEDED = str(pathlib.Path(__file__).parents[_CHILD_LEVEL].absolute())
if _DIR_NEEDED not in sys.path:
    sys.path.insert(0,_DIR_NEEDED)

from ccmeo_datacube.utils import nrcan_requests_ca_patch, valid_rfc3339

# Decorators
def win_ssl_patch(f):
    def pt_wrapper(*args,**kwargs):
        """
        Instantinates env variables to patch curl ssl error
        on windows os when opening files in rasterio with s3: protocol

        Patch for CURL errors:
            schannel: CertGetCertificateChain trust error
                CERT_TRUST_REVOCATION_STATUS_UNKNOWN
                CERT_TRUST_IS_UNTRUSTED_ROOT

        Example
        -------
        import dc_stac.dc_stac as dcs

        # Decorate function that relies on rasterio.open(s3:)
        @dcs.win_ssl_patch
        def function_name(*args,**kwargs):
            ...
            rasterio.open(s3://<path_to_s3_object>)
            return result
        ...

        # calling function
        result = function_name()

        """
        env_var = 'GDAL_HTTP_UNSAFESSL'

        # Enable ssl patch
        if 'win' in sys.platform:
            os.environ[env_var]='1'

        # Execute decorated function
        result = f(*args,**kwargs)

        # Disable ssl patch
        if 'win' in sys.platform:
            os.environ[env_var]='0'
        return result
    return pt_wrapper


# Defs
#Main extract methods
#TODO : Do we want to keep this function available? 
def wcs(mosaic:bool,
         collection:str,
         asset:str,
         bbox:str,
         bbox_crs:str,
         resolution:str,
         out_dir:str,
         method:str,
         overviews:bool,
         suffix:str=None)->str:
    
    dex=DatacubeExtract()
    #Condition : if hrdem-wcs is asked along with mosaic==True, a message is returned to the user
    if mosaic == True:
        print('INFORMATION : hrdem-wcs collection is already a mosaic, no mosaic processing is done, normal extraction will be performed...')
    
    print(f'collection: {collection}, asset: {asset}')
    poly = dex.bbox_to_poly(bbox=bbox)
    poly_dic = dex.poly_to_dict(poly)
    study_area = 'wcs_extract'
    srv_id='elevation'
        # datetime filter should not be passed to tifftag_datetime
    #datetime='2017:04:20 00:00:00'
    tifftag_datetime = None
    # TODO datetime_filter need to decide if you want start or end if datetime is a range
    # Then remove 'Z' to ensure it is in 8601 then pass date portion to date of WCS
    # And decide if want to pass time portion to time of WCS
    layer_list = []
    if asset :
        layer_list.append(asset)
    else:
        layer_list.append('dtm')
        layer_list.append('dsm')

    for layer_name in layer_list:
        print(''.rjust(75, '-'))
        if suffix:
            suffix = layer_name+'-'+suffix
        else:
            suffix = layer_name
        
        out_file = dex.wcs_coverage_extract(bbox_as_dict=poly_dic,
                                            crs=bbox_crs,
                                            protocol='HTTPS',
                                            lid=layer_name,
                                            level='prod',
                                            srv_id=srv_id,
                                            cellsize=resolution,
                                            cwd=out_dir,
                                            study_area=study_area,
                                            suffix=suffix,
                                            method=method,
                                            overviews=overviews,
                                            tifftag_datetime=tifftag_datetime)
        return out_file
    
    
def cog_chip(url:str,
              out_crs:str,
              resolution:float,
              list_resolutions:list,
              bbox:str,
              bbox_crs:str,
              method:str,
              out_dir:str,
              overviews:bool,
              suffix:str,
              in_res:int):
    """
    Create a cog from url of a cog in the cloud and a desire bbox 

    Parameters
    ----------
    url : str
        Url to the input cog
    out_crs : str
        DESCRIPTION.
    resolution : float
        Desire output resolution.
    list_resolutions : list
        List of the unique resolution of all the input items..
    bbox : str
        DESCRIPTION.
    bbox_crs : str
        DESCRIPTION.
    method : str
        DESCRIPTION.
    out_dir : str
        DESCRIPTION.
    overviews : bool
        DESCRIPTION.
    suffix : str
        DESCRIPTION.
    in_res : int
        Resolution of the input cog.

    Returns
    -------
    out_file : str
        str of the path to the output cog result

    """

    print(''.rjust(75, '-'))
    clip_file = pathlib.Path(url)
    
    profile, params = prepare_extract_cogchip(in_path=url,
                                                  out_crs=out_crs,
                                                  out_res=resolution,
                                                  list_resolutions=list_resolutions,
                                                  bbox=bbox,
                                                  bbox_crs=bbox_crs,
                                                  resampling_method=method)
    if profile and params:
        if resolution and resolution != in_res :
            if suffix:
                out_file = out_dir.joinpath(f"{clip_file.stem}-clip-{profile['crs'].to_epsg()}-resample-{resolution}m-{suffix}{clip_file.suffix}")
            else:
                out_file = out_dir.joinpath(f"{clip_file.stem}-clip-{profile['crs'].to_epsg()}-resample-{resolution}m{clip_file.suffix}")
        else:
            if suffix:
                out_file = out_dir.joinpath(f"{clip_file.stem}-clip-{profile['crs'].to_epsg()}-{suffix}{clip_file.suffix}")
            else:
                out_file = out_dir.joinpath(f"{clip_file.stem}-clip-{profile['crs'].to_epsg()}{clip_file.suffix}")
        
        # print(clip_file)
        print(out_file)
        out_file = extract_cogchip(in_path=url,
                                        out_path = out_file,
                                        out_profile=profile,
                                        extract_params=params,
                                        overviews=overviews)

        # out_files.append(out_file)
        return out_file 
    else:
        print('Ending extraction processus...')
        return None


    
def mosaic(df:pandas.DataFrame,
            orderby:str,
            resolution:float,
            desc:bool,
            list_resolutions:list,
            bbox:str,
            bbox_crs:str,
            method:str,
            out_crs:str,
            out_dir:str,
            out_file:str,
            overviews:bool)->dict:
    """
    Mosaics a list of urls, reverse painters based on date or resolution

    Parameters
    ----------
    df : pandas.DataFrame
        Dataframe with columns : 'url','collection_id','item_datetime',
                                'item_resolution','item_epsg','asset_key'
        Created from asset_url()
    orderby : str
        String of method to order the file by
        Default is 'date'
        Possible values for parameter are ['date', 'resolution']
    resolution : float
        Desire output resolution.
    desc : bool
        Booleen (True or False) for descending order of the files
    list_resolutions : list
        List of the unique resolution of all the input items.
    bbox : str
        DESCRIPTION.
    bbox_crs : str
        DESCRIPTION.
    method : str
        Resampling method.
    out_crs : str
        DESCRIPTION.
    out_dir : str
        DESCRIPTION.
    out_file : str
        DESCRIPTION.
    overviews : bool
        DESCRIPTION.

    Returns
    -------
    dict
        Key is the output mosaic path and values are the path to each cog used in the creation
        of the mosaic.

    """
    
    dex=DatacubeExtract()
    #potentiellement prendre le bord dans le futur
    out_crs = rasterio.crs.CRS.from_string(out_crs)
        
    if desc:
        order_method = 'descending'
    else:
        order_method = 'ascending'
    print(f'Files order will be determine by {orderby} {order_method}')
    
    #Order file by the method defined by the user
    # TODO use the more efficient urls = df.url.values.list with a sort
    df,urls = order_by(df, method=orderby, desc=desc)
    
    #Get the output extent based on the bbox
    (dst_transform, 
     dst_height,
     dst_width) = get_output_dimension(list_resolutions, 
                                           bbox=bbox, 
                                           bbox_crs=bbox_crs, 
                                           out_crs=out_crs, 
                                           out_res=resolution)	

    #get input nodata value and dtype to add to the output_profile
    in_meta = read_profile(urls[0], ['nodata', 'dtype'])

    #create the profile for the extracted cog chip
    out_profile = update_profile(in_profile=default_profile(),
                                     new_crs=out_crs,
                                     new_height=dst_height,
                                     new_width=dst_width,
                                     new_transform=dst_transform,
                                     new_blocksize=512,
                                     new_nodata=in_meta['nodata'],
                                     new_dtype=in_meta['dtype'])

    #Create list of parameters and output profile using the same tool as the extract_cogchips()
    list_params=[]
    for url in urls:
        dict_file = {}
        params = get_extract_params(out_profile, 
                                        src_res=df.query('url==@url')['item_resolution'].values[0],#row.item_resolution, 
                                        src_crs=f"EPSG:{df.query('url==@url')['item_epsg'].values[0]}", 
                                        dst_res=resolution, 
                                        resampling_method=method)
        
        dict_file['file']=url
        dict_file['params']=params
        list_params.append(dict_file)

    print('Starting mosaic process for each assets...')
    #Call the mosaic tool 
    if list_params and out_profile:
        #TODO : validate that the file does not exist or delete file
        out_path = pathlib.Path(os.path.join(out_dir, out_file))
        dex.check_outfile(out_path)
        files_used, file_unused = warped_mosaic(list_params, out_path, out_profile, overviews=overviews)
        dict_mosaic = {out_path:files_used}
        #TODO : do something with the file unused
    else:
        print('Ending extraction processus...')

    return dict_mosaic

#Test pour le window mosaic 
def mosaic_by_window(df:pandas.DataFrame,
                    orderby:str,
                    resolution:float,
                    desc:bool,
                    list_resolutions:list,
                    bbox:str,
                    bbox_crs:str,
                    method:str,
                    out_crs:str,
                    out_dir:str,
                    out_file:str,
                    overviews:bool)->dict:
    """Mosaics a list of urls, reverse painters based on date or resolution"""
    
    dex=DatacubeExtract()
    #potentiellement prendre le bord dans le futur
    out_crs = rasterio.crs.CRS.from_string(out_crs)
        
    if desc:
        order_method = 'descending'
    else:
        order_method = 'ascending'
    print(f'Files order will be determine by {orderby} {order_method}')
    
    #Order file by the method defined by the user
    # TODO use the more efficient urls = df.url.values.list with a sort
    df,urls = order_by(df, method=orderby, desc=desc)
    
    #Get the output extent based on the bbox
    (dst_transform, 
     dst_height,
     dst_width) = get_output_dimension(list_resolutions, 
                                           bbox=bbox, 
                                           bbox_crs=bbox_crs, 
                                           out_crs=out_crs, 
                                           out_res=resolution)	

    #get input nodata value and dtype to add to the output_profile
    in_meta = read_profile(urls[0], ['nodata', 'dtype'])

    #create the profile for the extracted cog chip
    out_profile = update_profile(in_profile=default_profile(),
                                     new_crs=out_crs,
                                     new_height=dst_height,
                                     new_width=dst_width,
                                     new_transform=dst_transform,
                                     new_blocksize=512,
                                     new_nodata=in_meta['nodata'],
                                     new_dtype=in_meta['dtype'])

    #Create list of parameters and output profile using the same tool as the extract_cogchips()
    list_params=[]
    for url in urls:
        dict_file = {}
        params = get_extract_params(out_profile, 
                                        src_res=df.query('url==@url')['item_resolution'].values[0],#row.item_resolution, 
                                        src_crs=f"EPSG:{df.query('url==@url')['item_epsg'].values[0]}", 
                                        dst_res=resolution, 
                                        resampling_method=method)
        
        dict_file['file']=url
        dict_file['params']=params
        list_params.append(dict_file)

    print('Starting mosaic process for each assets...')
    #Call the mosaic tool 
    if list_params and out_profile:
        #TODO : validate that the file does not exist or delete file
        out_path = pathlib.Path(os.path.join(out_dir, out_file))
        dex.check_outfile(out_path)
        files_used, file_unused = window_mosaic(list_params, out_path, out_profile, overviews=overviews)
        # files_used, file_unused = warped_mosaic(list_params, out_path, out_profile, overviews=overviews)
        dict_mosaic = {out_path:files_used}
        #TODO : do something with the file unused
    else:
        print('Ending extraction processus...')

    return dict_mosaic
#Sub-level extract methods
def default_profile():
    default_profile = {'driver': 'GTiff',
                       'count': 1,
                       'blockxsize': 512, 
                       'blockysize': 512,
                       'tiled': True, 
                       'compress': 'lzw', 
                       'interleave': 'band'
                       # 'dtype': 'float32',
                       # 'nodata': -32767.0
                       }
    #Do we really want to hardcode the nodata value of the output?
    #Things that may change : dtype, nodata
    return default_profile


@win_ssl_patch
def read_profile(filepath:str,
                 list_meta:list):
    """
    Get metadata from a raster

    Parameters
    ----------
    filepath : str
        Path to raster file.
    meta : list
        List of wanting metadata from filepath
    Returns
    -------
    in_nodata : int
        Filepath nodata value.
    in_dtype : str
        Filepath dtype value.

    """
    file_metadata = {}
    
    with rasterio.open(filepath) as in_raster:
        for meta in list_meta:
            value = in_raster.profile[meta]
            file_metadata[meta] = value
        
    return file_metadata


def bbox_windows(img_path:str,
                 bbox:str,
                 bbox_crs:str,
                 band:int=1,
                 clip:bool=True)->list:
    """
    Full or clipped image block_windows that intersect the tap version of bbox

    Memory light, window based image access
    Image read, write, reproject etc. now possible for portion of image

    Potential for concurrent processing (multi-threading)


    Parameters
    ----------
    img_path : str
        The path to the image.
    bbox : str
        The image bbox (minx,miny,maxx,maxy).  The box is converted to tap
    bbox_crs : str
        The EPSG code as string.
    band : int, optional
        The image band to be. The default is 1.
    clip : bool, optional
        If true image block windows are clipepd to  tap input window.
        The default is True.

    Returns
    -------
    list
        A list of rasterio.windows.Window that intersect or fall within bbox.

    Example Code
    ------------
    img_path = 'http://datacube-prod-data-public.s3.ca-central-1.amazonaws.com/store/elevation/cdem-cdsm/cdem/cdem-canada-dem.tif'
    bbox = ('2150469.4724999964,
            144975.05299999937,
            2155469.4724999964,
            149975.05299999937')
    bbox_crs = 'EPSG:3979'

    # Per band
    for band in range(1,img.count+1):
        # List of windows in study area based on images internal block_windows
        b_wins = bbox_windows(img_path,bbox,bbox_crs,band)

        # Per bbox_window
        for b_win in b_wins:
            arr = img.read(band,window=b_win)
            b_win_transform = img.window_transform(b_win)
            # Write per window, resample per window, etc.

    References
    ----------
    https://rasterio.readthedocs.io/en/latest/api/rasterio._base.html?highlight=block_shapes#id0
    https://rasterio.readthedocs.io/en/latest/topics/reproject.html
    https://github.com/rasterio/rasterio/blob/main/examples/reproject.py
    https://rasterio.readthedocs.io/en/latest/topics/concurrency.html
    """

    # Create the tap window for the image and get the image blocksize
    with rasterio.open(img_path) as img:
        w_tap = tap_window(img.transform,bbox,bbox_crs, img.crs)
        pix_per_block = img.block_shapes[0][0]
    
        # Calculate the block window start indices
        block_col_index_start = math.floor(w_tap.col_off/pix_per_block)
        block_row_index_start = math.floor(w_tap.row_off/pix_per_block)
    
        # Calculate the number of block windows required
        col_index_start = block_col_index_start*pix_per_block
        row_index_start = block_row_index_start*pix_per_block
    
        diff_block_col_start = w_tap.col_off - col_index_start
        diff_block_row_start = w_tap.row_off - row_index_start
    
        corrected_width = w_tap.width+diff_block_col_start
        corrected_height = w_tap.height+diff_block_row_start
    
        num_block_cols = math.ceil(corrected_width/pix_per_block)
        num_block_rows = math.ceil(corrected_height/pix_per_block)
    
        # num_block_cols = math.ceil(w_tap.width/pix_per_block)
        # num_block_rows = math.ceil(w_tap.height/pix_per_block)
    
    
        # Use range and pix_per_block to calculate all block windows required
        extract_windows = []
        for bl_idx_row in range(block_row_index_start,block_row_index_start+num_block_rows):
            for bl_idx_col in range(block_col_index_start,block_col_index_start+num_block_cols):
                extract_window = img.block_window(band,bl_idx_row,bl_idx_col)
                # print(f'Extract Window : {extract_window}')
                if clip:
                    # Clip the block window limits to tap window limits
                    current_window = clip_window(win=extract_window,clip_to=w_tap)
                    if current_window:
                        extract_windows.append(current_window)
                else:
                    # Use full block window
                    extract_windows.append(extract_window)
    
    return extract_windows


def tap_window(img_transform,
               bbox:str,
               bbox_crs:str=None,
               img_crs:str=None,
               add:int=0)->rasterio.windows.Window:
    """
    Matrix based calculation of a Target Aligned Pixel Window

    Parameters
    ----------
    img_tranform : from img.transform, the affine
        The image opened as a rasterio.DatasetReader.
    bbox : str
        The bbox as a tuple of floats.
    bbox_crs : str
        The crs of the input bbox.
    img_crs : str
        The crs of the input raster.
    add : int, optional
        The number of extra pixels to add to the window width and height.
        The default is 1.

    Returns
    -------
    w_tap : rasterio.windows.Window
        A target aligned pixel window based on user input.

    """
    
    dex = DatacubeExtract()
    if bbox_crs != img_crs:
        geom_d = dex.poly_to_dict((dex.bbox_to_poly(bbox)))
        geom_p = dex.transform_dict_to_poly(geom_d,bbox_crs,img_crs)
        bbox = geom_p.bounds
        #Doesnt change anything.
        # bbox = (bbox_temp[0] - 500 , bbox_temp[1]-500,bbox_temp[2]+500, bbox_temp[3]+500)
        # print('Test: adding a buffer around the bbox before reprojection')

    else:
        #Convert bbox str to tuple
        bbox = dex.bbox_to_tuple(bbox)
    # bbox = tuple(float(v) for v in bbox.split(','))
    # Original window based on original bounds
    w_orig = rasterio.windows.from_bounds(*bbox,transform=img_transform)

    # Assuming the window properties are not integers
    # TODO check to see if all window.col_off,row_off,height,width are integers
    # We want the lengths to be 'ceil' and the offsets to be 'floor' => TAPish
    # w_tapish = w_orig.round_lengths('ceil')
    # w_tapish = w_tapish.round_offsets('floor')
    ceil = {'op':'ceil', 'pixel_precision':None}
    floor = {'op':'floor', 'pixel_precision':None}
    w_tapish = w_orig.round_lengths(**ceil)
    w_tapish = w_tapish.round_offsets(**floor)

    # Add cells to height and width to cover extra distance (one should do)
    w_tap = rasterio.windows.Window(w_tapish.col_off,
                                    w_tapish.row_off,
                                    w_tapish.width+add,
                                    w_tapish.height+add)
    return w_tap


def clip_window(win:rasterio.windows.Window,
                clip_to:rasterio.windows.Window)->rasterio.windows.Window:
    """
    Clips the window limits to the clip_to window

    Parameters
    ----------
    win : rasterio.windows.Window
        The window to be clipped.
    clip_to : rasterio.windows.Window
        The cliping bounds definition.

    Returns
    -------
    clipped_window : rasterio.windows.Window
        The window clipped to the bounds.

    """
    #TODO Explore rasterio.window methods (intersection,intersect,crop)
    clipped_window = None
    # Clipping Window parameters
    clip_width = clip_to.width
    clip_height = clip_to.height
    clip_col_min = clip_to.col_off
    clip_col_max = clip_col_min + clip_width
    clip_row_min = clip_to.row_off
    clip_row_max = clip_row_min + clip_height
    win_width = win.width
    win_height = win.height
    win_col_min = win.col_off
    win_col_max = win_col_min + win_width
    win_row_min = win.row_off
    win_row_max = win_row_min + win_height

    wcmin = win_col_min if win_col_min > clip_col_min else clip_col_min
    wcmax = win_col_max if win_col_max < clip_col_max else clip_col_max
    wrmin = win_row_min if win_row_min > clip_row_min else clip_row_min
    wrmax = win_row_max if win_row_max < clip_row_max else clip_row_max
    w = wcmax-wcmin
    h = wrmax-wrmin
    # print(f'wcmin:{wcmin}, wrmin:{wrmin}, wcmax:{wcmax}, wrmax:{wrmax}, width:{w}, height:{h}')
    # If w or h is negative or 0 then window block is outside clip area and should be ignored
    if w > 0 and h > 0:
        clipped_window = rasterio.windows.Window(col_off=wcmin,
                                                 row_off=wrmin,
                                                 width=w,
                                                 height=h)
    return clipped_window


def resample_value(resample:str='bilinear'):
    """Assigns rasterio.enums.Resampling numerical value based on resample name

    This value is required as resample parameter in many rasterio methods


    Parameters
    ----------
    resample : str, optional
        The name of the resampling method. The default is 'bilinear'.

    Returns
    -------
    value : int
        The rasterio.enums.Resampling.<name>.value.

    """

    try:
        value = [r for r in rasterio.enums.Resampling if r.name == resample][0]
    except IndexError:
        value = rasterio.enums.Resampling.bilinear
    return value


def tap_params(in_crs:str,
               out_crs:str,
               in_left:float,
               in_bottom:float,
               in_right:float,
               in_top:float,
               in_width:float,
               in_height:float,
               out_res:int):
    """
    Returns the tap transform, width and height
    Is needed before reprojection

    Parameters
    ----------
    in_crs : str
        Source coordinate reference system.
    out_crs : str
        Target coordinate reference system..
    in_left : float
        Left bounding coordinate.
    in_bottom : float
        Bottom bounding coordinate.
    in_right : float
        Right bounding coordinate.
    in_top : float
        Top bounding coordinate.
    in_width : float
        Source raster width.
    in_height : float
        Source raster height.
    out_res : int
        Target resolution.

    Returns
    -------
    dst_transform : TYPE
        Transform of the destination raster with new crs and/or resolution.
    dst_width : TYPE
        Width of the destination raster with new crs and/or resolution.
    dst_height : TYPE
        Height of the destination raster with new crs and/or resolution.

    """

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


def update_profile(in_profile:dict,
                   new_crs=None,
                   new_height=None,
                   new_width=None,
                   new_transform=None,
                   new_blocksize=None, 
                   new_nodata=None,
                   new_dtype=None)->dict:
    """
    Function to update the img.profile with new metadata
    Parameters
    ----------
    in_profile : dict
        Profile of the input raster.
    new_crs : TYPE
        Crs of the output raster.
    new_height : TYPE
        Height of the output raster.
    new_width : TYPE
        Width of the output raster.
    new_transform : TYPE
        Transform of the output raster.
    new_blocksize : TYPE
        Blocksize of the output raster.

    Returns
    -------
    new_profile : dict
        Profile of the output raster.

    """
    new_profile = in_profile.copy()
    if new_crs:
        new_profile.update({'crs': new_crs})
    if new_height:
        new_profile.update({'height':new_height})
    if new_width:
        new_profile.update({'width':new_width})
    if new_transform:
        new_profile.update({'transform':new_transform})
    if new_blocksize:
        new_profile.update({'blockxsize': new_blocksize,
                            'blockysize': new_blocksize})
    if not (new_nodata is None): #Because nodata can be 0
        new_profile.update({'nodata':new_nodata})
        
    if new_dtype:
        new_profile.update({'dtype':new_dtype})

    new_profile['tiled'] = True
    new_profile['compress'] = 'lzw'

    return new_profile


def calc_lcm_bounds(orig_anti:float,orig_pro:float,ress:list)->Tuple[float,float]:
    """
    The lowest common multiple TAP bounds for corner coordinate pairs.
    
    Calculates North West corner or South East corner 
    based on list of all input and output resolutions.
    
    Parameters
    ----------
    orig_anti: float
        The value that needs to be 'negatively increased',
        that goes against axis positive direction.
        The bbox / clip box north west corner, western value 
        or south east corner, southern value.
    orig_pro: float
        The value that needs to be 'positively increased',
        that goes with axis positive direction.
        The bbox / clip box north west corner, northern value
        or south east corner, eastern value
    ress: list
        The list of all input and the output resolutions.
    
    Returns
    -------
    anti: float
        The resolution aligned 'anti' value.
        West for the north west corner, or South for the south east corner.
    pro: float
        The resolution aligned 'pro' value.
        North for the north west corner, or East for the south east corner.
    """
    # For resolution with mantessas, convert to integer
    factor = 100 
    ress = [int(x * factor) for x in ress]
    lcm = (math.lcm(*tuple(ress))) / factor

    # Calc west floor
    anti = (math.floor(orig_anti / lcm)) * lcm
    # Move 'anti' west, or south if not evenly divisable by lcm
    if anti % lcm != 0 :
        # move west (less east) or south (less north) by subtracting lcm
        anti -= lcm

    # Calc north ceiling    
    pro = (math.ceil(orig_pro / lcm)) * lcm
    # Move 'pro' north, or east if not evenly divisable by lcm
    if pro % lcm != 0:
        # Move 'north' or 'east' by adding lcm
        pro += lcm

    return anti, pro

def get_output_dimension(resolutions:list,
                          bbox:str, 
                          bbox_crs:str, 
                          out_crs:str=None, 
                          out_res:int=None):#->Tuple[affine.Affine,int,int]:
    """
    Calculate the dimensions and transform of the output raster based on 
    input and output resolution, crs and bbox

    Parameters
    ----------
    resolutions : list
        List of all the input raster resolution.
    bbox : str
        String of the w, s, e, n coods of the extent.
    bbox_crs : str
        Crs of the bbox coordinates.
    out_crs : str
        Desire crs of the output cog.
    out_res : int
        Desire resolution of the output cog.

    Returns
    -------
    dst_transform : TYPE
        Transform of the output raster.
    dst_height : int
        Height of the output raster in the output resolution.
    dst_width : int
        Width of the output raster in the output resolution.

    """
    dex = DatacubeExtract()
    
    #Reproject the bbox if out_crs is different from input crs
    if out_crs != bbox_crs:
        geom_d = dex.poly_to_dict(dex.bbox_to_poly(bbox)) #mapping(box(*[float(v) for v in bbox.split(',')])
        geom_p = dex.transform_dict_to_poly(geom_d,bbox_crs,out_crs) #shape(warp.transform_geom(in_crs,out_crs,geom_d))
        bbox_tuple = geom_p.bounds
    else:
        bbox_tuple = dex.bbox_to_tuple(bbox) #tuple(float(v) for v in bbox.split(','))
    
    #Get bbox bounds
    (bbox_w,
     bbox_s,
     bbox_e,
     bbox_n) = bbox_tuple

    #add the output resolution to the resolution list
    ress = resolutions + [out_res]
    #Calculate the output west and north based on lcm of all resolutions
    west, north = calc_lcm_bounds(bbox_w, bbox_n, ress)
    # Calculate the output south and east based on lcm of all resolutions
    south, east = calc_lcm_bounds(bbox_s,bbox_e,ress)
    
    # Create the transform
    transform = from_origin(west,north,out_res,out_res)
    
    #tap the out_bbox with window
    # Create the new lcm bbox
    bbox_new = f'{west},{south},{east},{north}'
    # Reprojected already to out_crs so pass out_crs both bbox_crs and out_crs
    out_win = tap_window(transform, bbox_new, out_crs, out_crs)
    #Define the parameters  
    dst_transform = transform #dce.rasterio.transform.from_origin(minx_full,maxy_full,out_res,out_res)
    dst_height = out_win.height
    dst_width = out_win.width
    
    return dst_transform, dst_height, dst_width


@win_ssl_patch
def prepare_extract_cogchip(in_path:str,
                        bbox:str,
                        bbox_crs:str,
                        list_resolutions:list,
                        out_crs=None,
                        out_res=None,
                        resampling_method:str='bilinear',
                        verbose:bool=True):
    """
    Function that act as a wrapper for the extraction of a minicube 
    to define the parameters of extraction and 
    the profile for the out raster

    Parameters
    ----------
    in_path : str
        String path to the raster file.
    bbox : str
        Extent of the region of extraction.
    bbox_crs : str
        Crs of the input bbox.
    out_crs : TYPE, optional
        Crs of the output raster. If different from input crs, reprojection will be done.
        The default is None.
    out_res : TYPE, optional
        Resolution of the output raster. If different from the input resolution, resample will be done.
        The default is None.
    resampling_method : str, optional
        Resampling method for the resampling and reprojection when needed.
        The default is 'bilinear'.
    verbose : bool, optional
        Indicate to print the information to user, mainly use to stop printing of message when using mosaic 
        The default is True

    Returns
    -------
    out_profile : dict
        Profile of the output raster.
    extract_params : dict
        Dictionnary of the extraction parameters needed inside the warped vrt function used for the
        extraction.

    """
    # print(os.getenv('GDAL_HTTP_UNSAFESSL'))
    dex = DatacubeExtract()

    # env = rasterio.Env(GDAL_HTTP_UNSAFESSL='YES')
    env = rasterio.Env()

    with env:
        with rasterio.open(in_path) as img:

            in_crs = img.crs
            in_res = img.res[0]
            in_profile = img.profile
           

    #Those parameters can be None if natif extraction is wanted
    if out_crs == None:
        out_crs = in_crs
        
    if out_res == None:
        out_res = in_res
    
    if isinstance(out_crs, str):
        out_crs = rasterio.crs.CRS.from_string(out_crs)
        
    if out_crs.is_geographic:
        #This is a fix,  because we cannot control the size of the file if we don't know the pixel size
        print('Extraction is only available with projected output crs. '\
              'Please provid a projected output crs.')
        return None, None
    
    if in_crs.is_geographic and out_res == in_res:
        #This is a fix for now, but in the futur we should transform the res from deg to meters
        # print('Input crs is a geographic coordinate reference system, '\
        #       'cellsize will be converted to the closes meter integer value')
        print('Input crs is a geographic coordinate reference system, '\
              'cellsize is set to 10m (closes meter value for 1/3 arc-second). '\
              '\nExtraction time might be longer...')
        out_res = 10
    
    print(in_path)
    print(f'INFO : Extraction in {out_crs} crs and {out_res}m resolution')
    
    dst_transform, dst_height, dst_width = get_output_dimension(list_resolutions, bbox, bbox_crs, out_crs, out_res)	

    #create the profile for the extracted cog chip
    out_profile = update_profile(in_profile=in_profile,
                                 new_crs=out_crs,
                                 new_height=dst_height,
                                 new_width=dst_width,
                                 new_transform=dst_transform,
                                 new_blocksize=512)
    
    extract_params = get_extract_params(out_profile, in_res, in_crs, out_res, resampling_method)
   
    out_profile = _add_bigtiff(out_profile, verbose)
        
    return out_profile, extract_params


# #TODO : Define the extract params based not on the out profile, but on the in profile, but with the out parameters (crs, res, height, width)
def get_extract_params(profile, src_res, src_crs, dst_res, resampling_method):
    """
    Creates the dictionnary of parameters needed for extraction inside the rasterioWarpedVRT
    based on the output profile 

    Parameters
    ----------
    out_profile : rasterio.profiles.Profile
        Profile of the desire output raster.
    in_res : int
        input raster resolution.
    in_crs : rasterio.crs.CRS
        input crs.
    out_res : int
        output desire resolution.
    resampling_method : str
        resampling method for the reprojection and resample of the raster.

    Returns
    -------
    extract_params : TYPE
        DESCRIPTION.

    """
    if isinstance(src_crs, str):
        src_crs = rasterio.crs.CRS.from_string(src_crs)
        
    extract_params = profile.copy()
    if dst_res != src_res and src_crs.is_projected:
        # extract_params = out_profile.copy()
        resample_height = profile['height'] * dst_res / src_res
        resample_width = profile['width'] * dst_res / src_res
        resample_transform = Affine(src_res, 0.0, profile['transform'].c,
                                      0.0, -src_res, profile['transform'].f)
        extract_params.update({'transform': resample_transform,
                                'height': resample_height,
                                'width': resample_width,
                                'resampling': resample_value(resampling_method)})
    else:
        extract_params.update({'resampling': resample_value(resampling_method)})
    
    return extract_params


@win_ssl_patch
def extract_cogchip(in_path:str,
                     out_path:pathlib.Path,
                     out_profile:dict,
                     extract_params,
                     overviews:bool=False):
    """
    Parameters
    ----------
    in_path : str
        String of the path to the raster.
    out_path : pathlib.Path
        pathlib.Path of the output result from the extraction
    out_profile : dict
        Profile of the output raster.
    extract_params : dict
        Parameters of the extraction needed inside the warpedVRt extraction.
    overviews : bool, optional
        Trigger the creation of overviews to the ouput cog. The default is False.

    Returns
    -------
    out_path : str
        String of the path to the output raster.

    """
    dex = DatacubeExtract()
    #TODO : Explore standart env. setup
    #TODO : Create a function for setting up the env.
    env = rasterio.Env(
        GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR", # a way to disable loading of side-car or auxiliary files
        CPL_VSIL_CURL_USE_HEAD=False, #pas certaine de ce que ca fait
        CPL_VSIL_CURL_ALLOWED_EXTENSIONS="TIF", #considering only files that ends with .tif
        GDAL_NUM_THREADS='ALL_CPUS',#Enable multi-threaded compression by specifying the number of worker threads
        # GDAL_HTTPS_UNSAFESSL=1,
    )


    with env:
        with rasterio.open(in_path) as src:
            #datetime
            metadata = src.tags()
            try:
                date = metadata["TIFFTAG_DATETIME"]
            except:
                date = None
            #color
            try:
                color_dict = src.colormap(1)
            except :
                color_dict = None
           
            with rasterio.vrt.WarpedVRT(src, **extract_params) as vrt:
                with rioxarray.open_rasterio(vrt, lock=False, chunks=True) as cog:
                    out_res = out_profile['transform'].a
                    in_res = extract_params['transform'].a
                    if out_res != in_res:
                        print('Output pixel size is different from input pixel size, '\
                              'starting resampling...')
                        cog = cog.rio.reproject(cog.rio.crs,
                            shape=(out_profile['height'], out_profile['width']),
                            resampling=extract_params['resampling'],
                            transform = out_profile['transform'])

                    #Adding overviews if needed
                    if overviews:
                        with TemporaryDirectory() as temp_dir:
                            temp_file = pathlib.Path(temp_dir,f'{out_path.name}.temp')
                            temp = cog.rio.to_raster(temp_file, windowed=True, lock=threading.Lock(), **out_profile)
                            with rasterio.open(temp_file, 'r+') as dst:
                                dst = dex.add_overviews(dst, resample=extract_params['resampling'])
                                rscopy(dst,out_path,copy_src_overviews=True,**out_profile)
    
                    else:
                        print('No overviews added to the outfile')
                        # rscopy(temp_file,file_name,**kwargs)
                        cog.rio.to_raster(out_path, windowed=True, lock=threading.Lock(), **out_profile)
                        # rio_shutil.copy(vrt, out_file, driver='GTiff',**vrt_options)
                # cog.close()
                #datetime
                if date:
                    with rasterio.open(out_path, "r+") as img:
                        img.update_tags(TIFFTAG_DATETIME=date)
                #color
                if color_dict:
                    with rasterio.open(out_path, "r+") as img:
                       img.write_colormap(1, color_dict)
    return out_path


def order_by(dataframe:pandas.DataFrame, 
            method:str='date', 
            desc:bool=True,
            date_field:str='item_datetime',
            res_field:str='item_resolution',
            url_field:str='url')->Tuple[pandas.DataFrame,list]:
    """
    Order files according to method defined by user for mosaic creation
    
    Paramters
    ----------
    dataframe : pandas.DataFrame
        DataFrame with url,collection_id,item_datetime
    method : str
        string of method to order the file by
        Default is 'date'
        Possible values for parameter are ['date', 'resolution']
    desc : bool
        Booleen (True or False) for descending order of the files
        Default is True
    date_field : str
        The name of the date field in the dataframe.
        The default is item_datetime.
    res_field : str
        The name of the resolution field in the dataframe.
        The default is item_resolution.
    url_field : str
        The name of the url / href field in the dataframe.
        The default is url.
    Returns
    -------
    dataframe: pandas.DataFrame
        The dataframe
    urls: list
        An ordered list of urls
    """
    # Validate the imputs
    if method not in ['date', 'resolution']:
        msg = (f"ERROR : {method} is not a valid value for 'orderby' parameter."
              "Please provide a valide method."
              " Accepted values are ['date', 'resolution']")
        print(msg)
        return dataframe,None
    
    if 'collection_id' not in dataframe.columns:
            msg = ("ERROR : missing collection_id field."
                   "The dataframe does not contain"
                   " the collection_id field")
            print(msg)
            return dataframe,None
    
    if date_field not in dataframe.columns:
            msg = ("ERROR : missing datetime field."
                   "The dataframe does not contain"
                   f" the datetime field {date_field}")
            print(msg)
            return dataframe,None
    
    if url_field not in dataframe.columns:
            msg = ("ERROR : missing url field."
                   "The dataframe does not contain"
                   f" the url field {url_field}")
            print(msg)
            return dataframe,None
    
    if res_field not in dataframe.columns:
            msg = ("ERROR : missing resolution field."
                   "The dataframe does not contain"
                   f" the datetime field {res_field}")
            print(msg)
            return dataframe,None
    

    # Order by
    if method == 'date':
        # Order by collection_id, datetime
        ordered_list = [url for url in dataframe.sort_values(by=['collection_id',date_field],
                                                         ascending=[True,not desc]).url]
        
    elif method == 'resolution':
        # Order by collection_id, resolution, datetime
        ordered_list = [url for url in dataframe.sort_values(by=['collection_id',res_field,date_field],
                                                            ascending=[True,not desc,False],
                                                            na_position='last')
                                                            .url]
    else:
        # TODO can add other fields to dataframe from STAC or COG
        pass
    
    return dataframe,ordered_list

@win_ssl_patch
def warped_mosaic(list_of_params, 
                  out_path, 
                  out_profile,  
                  overviews=False):
    
    """
    Parameters
    ----------
    list_of_params : list of dictionnary
        Dictionnary of input file path and extraction paramters for each file to includ in the mosaic
        ex. [{'file': 
        'https://datacube-prod-data-public.s3.amazonaws.com/store/elevation/hrdem/hrdem-lidar/GEOMONT-2013_est_RNCan-2m-dsm.tif',
          'params': {'driver': 'GTiff', 'dtype': 'float32', 'nodata': -32767.0, '
                      width': 2500.0, 'height': 2502.0, 'count': 1, 'crs': CRS.from_epsg(3979), 
                      'transform': Affine(2.0, 0.0, 1708820.0, 0.0, -2.0, -102044.0), 'blockxsize': 512, '
                      blockysize': 512, 'tiled': True, 'compress': 'lzw', 'interleave': 'band', 
                      'resampling': <Resampling.bilinear: 1>}},...]
    out_path : pathlib.Path
        pathlib.Path of the output result from the extraction
    out_profile : dict
        Profile of the output raster.
    overviews : bool, optional
        Trigger the creation of overviews to the ouput cog. The default is False.

    Returns
    -------
    out_path : str
        String of the path to the output raster.

    """
    #Write file on top of each other with the warped method
    #All file must have same crs, same number of bands and same datatype
    dex = DatacubeExtract()
    #TODO : Explore standart env. setup
    #TODO : Create a function for setting up the env.
    env = rasterio.Env(
        GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR", # a way to disable loading of side-car or auxiliary files
        CPL_VSIL_CURL_USE_HEAD=False, #pas certaine de ce que ca fait
        CPL_VSIL_CURL_ALLOWED_EXTENSIONS="TIF", #considering only files that ends with .tif
        GDAL_NUM_THREADS='ALL_CPUS',#Enable multi-threaded compression by specifying the number of worker threads
        # GDAL_HTTP_UNSAFESSL=1,
        # CPL_CURL_VERBOSE=1,
        # CURL_CA_BUNDLE='/space/partner/nrcan/geobase/work/opt/miniconda-datacube/envs/datacube-usecase/lib/python3.9/site-packages/certifi/cacert.pem',
        # REQUESTS_CA_BUNDLE='/space/partner/nrcan/geobase/work/opt/miniconda-datacube/envs/datacube-usecase/lib/python3.9/site-packages/certifi/cacert.pem'
          )

    band=1
    temp_file = f'{out_path}.temp'
    
    out_profile = _add_bigtiff(out_profile)
        
    with env:
        with rasterio.open(temp_file, mode="w+", **out_profile) as out_img:
            used_file = []
            unused_file = []
            #TODO: add color palettes if exist
            #TODO: add datetime (question is which datetime? oldest of files or date of creation?)
            for params in list_of_params:
                file = params['file']
                extract_params = params['params']
                with rasterio.open(file) as src:
                    with rasterio.vrt.WarpedVRT(src, **extract_params) as vrt:
                        with rioxarray.open_rasterio(vrt, lock=False, chunks=True) as xar:
                            
                            xar_nodata = xar.rio.nodata
                            #Calcul de la fenetre dans le fichier output
                            input_extent = xar.rio.bounds() 
                            dst_window = rasterio.windows.from_bounds(*input_extent, out_img.transform) #Since the get_extract_params tap and cap parameters already
                            
                            print(''.rjust(75, '-'))
                            #Validate that the output mosaic window for this file is not already filled up with values
                            if out_img.read_masks(window=dst_window).all():
                                #Si toutes les valeurs du mask sont valide, donc pas du nodata on skip l'image
                                print(f'Extent covered by {file} is already filled in mosaic, skipping file...')
                                unused_file.append(file)
                                continue
                            
                            print(file)
                            
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
                          
                            window_arr = xar.values[0]
                            
                #Pour valider qu'il y a autre chose que tu nodata dans le input file window
                if numpy.all((window_arr == xar_nodata)):
                    print('All no data')
                    unused_file.append(file)

                else:
                    
                    # Read values already written to current window
                    # In out_crs spatial coords
                    existing_arr = out_img.read(band,window=dst_window)
                    
                    print(f'Update values in mosaic with value from : {file}')
                    # Make an existing window no_data mask
                    no_data_mask = (existing_arr == out_img.nodata)
                    #Modify window_arr no data to out_img to data
                    window_arr[window_arr == xar_nodata] = out_img.nodata
                    
                    # LOG.debug(f'no_data_mask shape {no_data_mask.shape}')
                    new_data = existing_arr
                    new_data[no_data_mask] = window_arr[no_data_mask]
                    out_img.write(new_data,indexes=band,window=dst_window)
                    
                    #Populate the list of file used inside the mosaic
                    used_file.append(file)

        #Adding overviews if needed
        if overviews:
            print(''.rjust(75, ' '))
            print('Creation of overviews...')
            with rasterio.open(temp_file, 'r+') as temp_mosaic:
                temp_mosaic = dex.add_overviews(temp_mosaic, resample=extract_params['resampling'])
                rscopy(temp_mosaic,out_path,copy_src_overviews=True,**out_profile)
            os.remove(temp_file)
           
        else:
            print(''.rjust(75, ' '))
            print('No overviews added to the mosaic')
            os.rename(temp_file, out_path)
            
               
                   
    return used_file, unused_file #list of file used inside the mosaic


@win_ssl_patch
def window_mosaic(list_of_params, 
                  out_path, 
                  out_profile,  
                  overviews=False):
    
    """
    Parameters
    ----------
    list_of_params : list of dictionnary
        Dictionnary of input file path and extraction paramters for each file to includ in the mosaic
        ex. [{'file': 
        'https://datacube-prod-data-public.s3.amazonaws.com/store/elevation/hrdem/hrdem-lidar/GEOMONT-2013_est_RNCan-2m-dsm.tif',
          'params': {'driver': 'GTiff', 'dtype': 'float32', 'nodata': -32767.0, '
                      width': 2500.0, 'height': 2502.0, 'count': 1, 'crs': CRS.from_epsg(3979), 
                      'transform': Affine(2.0, 0.0, 1708820.0, 0.0, -2.0, -102044.0), 'blockxsize': 512, '
                      blockysize': 512, 'tiled': True, 'compress': 'lzw', 'interleave': 'band', 
                      'resampling': <Resampling.bilinear: 1>}},...]
    out_path : pathlib.Path
        pathlib.Path of the output result from the extraction
    out_profile : dict
        Profile of the output raster.
    overviews : bool, optional
        Trigger the creation of overviews to the ouput cog. The default is False.

    Returns
    -------
    out_path : str
        String of the path to the output raster.

    """
    #Write file on top of each other with the warped method
    #All file must have same crs, same number of bands and same datatype
    dex =  DatacubeExtract()
    #TODO : Explore standart env. setup
    #TODO : Create a function for setting up the env.
    env = rasterio.Env(
        GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR", # a way to disable loading of side-car or auxiliary files
        CPL_VSIL_CURL_USE_HEAD=False, #pas certaine de ce que ca fait
        CPL_VSIL_CURL_ALLOWED_EXTENSIONS="TIF", #considering only files that ends with .tif
        GDAL_NUM_THREADS='ALL_CPUS',#Enable multi-threaded compression by specifying the number of worker threads
        # GDAL_HTTP_UNSAFESSL=1,
        # CPL_CURL_VERBOSE=1,
        # CURL_CA_BUNDLE='/space/partner/nrcan/geobase/work/opt/miniconda-datacube/envs/datacube-usecase/lib/python3.9/site-packages/certifi/cacert.pem',
        # REQUESTS_CA_BUNDLE='/space/partner/nrcan/geobase/work/opt/miniconda-datacube/envs/datacube-usecase/lib/python3.9/site-packages/certifi/cacert.pem'
          )

    band=1
    temp_file = f'{out_path}.temp'
    
    out_profile = _add_bigtiff(out_profile)
        
    with env:
        with rasterio.open(temp_file, mode="w+", **out_profile) as out_img:
            used_file = []
            unused_file = []
            #TODO: add color palettes if exist
            #TODO: add datetime (question is which datetime? oldest of files or date of creation?)
            for params in list_of_params:
                file = params['file']
                extract_params = params['params']
                with rasterio.open(file) as src:
                    #get the input datatype
                    dt = src.dtypes[0]
                    print(''.rjust(75, '-'))
                    print(file)
                    
                    out_res = out_profile['transform'].a
                    in_res = extract_params['transform'].a
                    if out_res != in_res:
                        print('Output pixel size is different from input pixel size, '\
                              'there will be resampling...')
                    
                    with rasterio.vrt.WarpedVRT(src, **extract_params) as vrt:
                        #count the number of block_windows for the file
                        total_win = 0
                        not_used_window = 0
                        for rc,src_window in vrt.block_windows(band):
                            #count the total number of window (there must be a better way to do it....)
                            total_win = total_win+1
                            #get the input nodata value
                            in_nodata = vrt.nodata
                            
                            # #Calcul de la fenetre dans le fichier output
                            input_extent = rasterio.windows.bounds(src_window,
                                                 transform=vrt.transform)
                            dst_window = rasterio.windows.from_bounds(*input_extent, out_img.transform)
                          
                            #Validate that the output mosaic window for this file is not already filled up with values
                            if out_img.read_masks(window=dst_window).all():
                                #Si toutes les valeurs du mask sont valide, donc pas du nodata on skip l'image
                                #TODO: modify the message to take into account the window, and only includ it in the log 
                                # print(f'Extent covered by {file} is already filled in mosaic, skipping file...')
                                # unused_file.append(file)
                                not_used_window = not_used_window+1
                                continue
                            
                            window_arr = vrt.read(band,window=src_window)
                            #Pour valider qu'il y a autre chose que tu nodata dans le input file window
                            if numpy.all((window_arr == in_nodata)):
                                # print('All no data')
                                # unused_file.append(file)
                                not_used_window = not_used_window+1
            
                            else:
                               
                                # Read values already written to current window
                                # In out_crs spatial coords
                                existing_arr = out_img.read(band,window=dst_window)
                                
                                # print(f'Update values in mosaic with value from : {file}')
                                # Make an existing window no_data mask
                                no_data_mask = (existing_arr == out_img.nodata)
                                #Modify window_arr no data to out_img to data
                                window_arr[window_arr == in_nodata] = out_img.nodata
                                
                                # LOG.debug(f'no_data_mask shape {no_data_mask.shape}')
                                new_data = existing_arr
                                
                                #Pour le resampling                                
                                if out_res != in_res:
                                    
                                    # Get the source and destination transforms
                                    src_window_transform = rasterio.windows.transform(src_window,vrt.transform)
                                    dst_window_transform = rasterio.windows.transform(dst_window,out_img.transform)
                                
                                    # Create a destination array filled with no data
                                    #I believe that this introduce some errors at the marging of the projects when using resampling method 'bilinear'
                                    destination = numpy.zeros((int(dst_window.height),int(dst_window.width)),dt)
                                    destination[destination==0] = out_img.nodata
                                    
                                    
                                    rasterio.warp.reproject(window_arr,
                                                            destination,
                                                            src_transform=src_window_transform,
                                                            src_crs=vrt.crs,
                                                            dst_transform=dst_window_transform,
                                                            dst_crs=out_profile['crs'],
                                                            resampling=extract_params['resampling'],
                                                            src_nodata=in_nodata)
                                    
                                    new_data[no_data_mask] = destination[no_data_mask]
                                else :
                                    new_data[no_data_mask] = window_arr[no_data_mask]
                                
                                #Write to the outfile
                                out_img.write(new_data,indexes=band,window=dst_window)
                                
                                
                    if not_used_window == total_win:
                        #Only happens if all the windows in the file were skiped
                        print(f'Extent covered by {file} is already filled in mosaic, skipping file...')
                        unused_file.append(file)
                    else :
                        used_file.append(file)
                        print(f'Updated values in mosaic with values from : {file}')
                                
                    # break
        #Adding overviews if needed
        if overviews:
            print(''.rjust(75, ' '))
            print('Creation of overviews...')
            with rasterio.open(temp_file, 'r+') as temp_mosaic:
                temp_mosaic = dex.add_overviews(temp_mosaic, resample=extract_params['resampling'])
                rscopy(temp_mosaic,out_path,copy_src_overviews=True,**out_profile)
            os.remove(temp_file)
           
        else:
            print(''.rjust(75, ' '))
            print('No overviews added to the mosaic')
            os.rename(temp_file, out_path)
            
               
                   
    return used_file, unused_file #list of file used inside the mosaic


def _add_bigtiff(out_profile, verbose=True):
    dex = DatacubeExtract()
    #Add the bigtiff tag inside the output_profile
    file_size = dex.calculate_file_size(out_profile['dtype'],
                                        out_profile['width'],
                                        out_profile['height'])
    
    if file_size >= 3:
        out_profile['BIGTIFF']='YES'
        if verbose:
            print(f'INFO : Estimated file size is {file_size}GB, creation of BIGTIFF...')
    else :
        if verbose:
            print('INFO : Out file is smaller than 3GB, creation of TIFF...')
    
    return out_profile


def _filter_by_resolution(df, resolution_filter):
    """Return the dataframe fitlered for the resolution asked by user"""
    
    match_range = r'([\d])+:([\d])+' #Example 2:10 
    match_min = r'([\d])+:' #Example 2: and 2:10
    match_max = r':([\d])+' #Example :2 and 2:10
    
    if ':' not in resolution_filter:
        #Specific resolution 
        resolution_filter=int(resolution_filter)
        df =  df.query('item_resolution == @resolution_filter')
        # print('Specific resolution')
    elif re.search(match_range, resolution_filter):
        #Close range of resolution
        res_min, res_max = resolution_filter.split(':')
        res_min=int(res_min)
        res_max=int(res_max)
        df = df.query('item_resolution >= @res_min and item_resolution <= @res_max')
        # print(f'Range resolution {resolution_filter}')
    elif re.search(match_min, resolution_filter):
        #Open range of resolution, higher of equal to the given value
        res_min, res_max = resolution_filter.split(':')
        res_min = int(res_min)
        df = df.query('item_resolution >= @res_min')
        # print(f'from {resolution_filter}')
    elif re.search(match_max, resolution_filter):
        #Open range of resolution, smaller of equal to the given value
        res_min, res_max = resolution_filter.split(':')
        res_max= int(res_max)
        df = df.query('item_resolution <= @res_max')
        # print(f'to {resolution_filter}')
    else:
        print(f'resolution_filter {resolution_filter} is not valid, no filter executed')
        return df
    
    return df

@nrcan_requests_ca_patch
def asset_urls(collection_asset:pandas.DataFrame,
                extent_crs:str,
                bbox:str=None,
                poly_dict:dict=None,
                asset_role:str='data',
                datetime_filter:str=None,
                resolution_filter:str=None,
                )->pandas.DataFrame:

    """A module level function that scrapes STAC API search
    
    Queries each release level STAC API and compiles list of asset urls for collections

    Parameters
    ----------
    collections : pandas.DataFrame
        dataframe of collection : asset.
    bbox : str
        BBOX geometry in a string.
    bbox_crs : str
        Crs of the input bbox.
    poly : dict
        Geojson like polygon from shapely geometry
    asset_role : str, optional
        The default is 'data'.
   datetime_filter : str, optional
        Filter based on RFC 3339
        Should follow formats
        A date-time: "2018-02-12T23:20:50Z"
        A closed interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
        Open intervals: "2018-02-12T00:00:00Z/.." or "../2018-03-18T12:31:12Z"
    resolution_filter : str, optional
        Should follow formats:
        A specific resolution = "resolution" - Example :  "2" (only 2 meters)
        A closed interval = "min:max" - Example : "2:10" (from 2 to 10 meters inclusivly)
        Open intervals = "min:" or ":max" - Example: "2:" (2 meters and more) or ":2" (2 meters and less)
        Default is None
    Returns
    -------
    geopandas.DataFrame
        DataFrame with urls, collections, item_datetimes, item_resolution, item_epsg and asset_key

    Example
    -------
    import extract.extract as dce
    dex = dce.DatacubeExtract()
    df = asset_urls(pandas.DataFrame({'collection':['hrdem-lidar', 'landcover'], 'asset':['dsm', None]}),
                        '-75,45,-73,47',
                        'EPSG:4326')
    # Make a list of urls
    urls = [u for u in df.url]
    """
    urls = []
    pages = []
    
    if bbox :
        bbox_dict = bbox_to_dict(bbox)
        g4326 = stac_api_search_geometry(bbox_dict,extent_crs)
    else:
        #TODO : Allow multiple geometry from geopackage?
        g4326 = stac_api_search_geometry(poly_dict,extent_crs)
        
    # Check collection availability in prod and stage
    for index, row in collection_asset.iterrows():
        collection = row['collection']
        asset_id = row['asset']
        query = stac_api_search_query([collection],
                                      g4326,
                                      datetime_filter=datetime_filter)
        # Check for collection on production level
        url = api_root_url('prod') + '/search'
        pages = _search_pages(url=url,method='post',payload=query)
        if len(pages) == 0:
            # Check for collection on staging level
            url = api_root_url('stage') + '/search'
            pages = _search_pages(url=url,method='post',payload=query)
        for page in pages:
            # POST first request then GET rest
            print(f'INFO : scraping {page}')
            r = requests.post(page,json=query)
            if r.status_code == 200:
                _scrape_results(urls=urls,
                                results=r.json(),
                                collections=[collection],
                                asset_role=asset_role,
                                asset_id=asset_id)

    df = pandas.DataFrame(data=urls,
                          columns=['url','collection_id','item_datetime','item_resolution','item_epsg','asset_key'])
    
    #filter on resolution, if specified
    if resolution_filter:
        final_df = _filter_by_resolution(df, resolution_filter)
    else:
        final_df = df
        
    return final_df

def _valid_file(asset_values:dict,
                asset_role='data')-> Union[bool, list]:
    
    valid_extensions = ['tif','tiff','gtiff']
    valid = False
    if asset_role in asset_values['roles']:
        url = asset_values['href']
        if url.split('.')[-1].lower() in valid_extensions:
            valid = True

        return valid,url
    else:
        return valid,None

def _scrape_results(urls:list,
                    results:dict,
                    collections:list,
                    asset_role='data',
                    asset_id=None):
    """
    Parses Feature Collection for assets

    Parameters
    ----------
    urls : list
        list of tuple (url,collection,datetime) extracted from results.
    results : dict
        FeatureCollection (items) JSON response from STAC API search endpoint
    collections : list
        Collection filter list.
    asset_role : TYPE, optional
        Asset role. The default is 'data'.
    asset_id : TYPE, optional
        Asset ID. The default is None.

    Returns
    -------
    None.

    """
    returned_collections = []
    item_datetime = None
    features = results['features']

    for feature in features:
        # for each defined cog, pass url to url
        returned_collection = feature['collection']
        returned_collections.append(returned_collection)
        # Only return urls from collections requested
        # Franklin bug returns multiple collections from mutlipage result link
        if returned_collection in collections:
            # Extract item datetime to use in ordering
            try:
                item_datetime = feature['properties']['datetime']
            except:
                item_datetime = None
            # Extract spatial resolution from item.properties.proj:tranform
            try:
                item_res = feature['properties']['proj:transform'][1]
            except:
                item_res = None
            # Extract epsg code for projection from item.properties.proj:epsg
            try:
                item_epsg = feature['properties']['proj:epsg']
            except:
                item_epsg = None
            for key,values in feature.items():
                if key == 'assets':
                    for asset_key, asset_values in values.items():
                        if asset_id: 
                            #Is true when asset_id is define by user
                            # only return url for specific asset_id and asset role
                            if asset_key == asset_id:
                                valid,url = _valid_file(asset_values,asset_role)
                                if valid:
                                    urls.append((url,returned_collection,item_datetime,item_res,item_epsg,asset_key))
                        else:
                            # When asset_id is not define by user
                            # get all the asset_id with specified role
                            valid,url = _valid_file(asset_values,asset_role)
                            if valid:
                                urls.append((url,returned_collection,item_datetime,item_res,item_epsg,asset_key))
    # print(f'INFORMATION : collections returned {list(set(returned_collections))}')
    return


def _search_pages(url:str,method='get',payload:dict=None)->list:
    """
    A valid list of urls based on stac api link['next'] for the search endpoint
    
    Franklin STAC API generates a next link even when there is no next page
    (https://datacube.services.geo.ca/api/collections/landcover/items)

    This pagenator verifies the validity of the next link and returns a list
    of valid pages.

    Parameters
    ----------
    url : str
        The stac api endpoint.

    Returns
    -------
    pages: list
        A list of valid page urls to paginate through.
    method: str
        The HTTP method, get or post.
        The default is 'get'
    payload: dict
        The POST payload.
        The default is None.
    
    Example
    -------
    url = 'datacube.services.geo.ca/collections/msi/items'
    pages = stac_api_paginate(url)
    for page in pages:
        r = requests.get(page)
        ...

    """
    # Get a list of collections from /collections endpoint
    pages = []
    next_page = url
    returned = 0
    matched = 0
   
    while next_page:
        if method == 'post':
            # Only use post for first request
            # print(f'POST {next_page}, {payload}')
            r = requests.post(next_page,json=payload)
        else:
            # print(f'GET {next_page}')
            r = requests.get(next_page)
        if r.status_code == 200:
            j = r.json()                     
            # Test the returns total against total matched
            returned += j['context']['returned']
            matched = j['context']['matched']
            if returned > 0:
                pages.append(next_page)
            if returned < matched:
                links = j['links']
                next_page = _get_next_page(links)
            else:
                next_page = None
        else:
            next_page = None
                            
    r.close()
    return pages

def _get_next_page(links:list):
    """Returns the next page link or None from STAC API links list"""
    next_page = None
    for link in links:
        if link['rel'] == 'next':
            next_page = link['href']
    return next_page

def api_root_url(level:str='stage')->str:
    """
    Return api url base on level

    Parameters
    ----------
    level : str, optional
        Env level (stage, prod, dev). The default is 'prod'.

    Returns
    -------
    str
        service url.

    """
    if not level in ['dev','stage','prod']:
        level = 'stage'

    if 'prod' in level:
        rd = 'https://datacube.services.geo.ca/api'
    else:
        rd = f'https://datacube-{level}.services.geo.ca/api'
    return rd

def stac_api_search_geometry(geom:str,geom_crs:str) -> dict:
    """
    Transforms  GeoJson style dict polygon into 4326 

    Parameters
    ----------
    geom : GeoJson style dict polygon
        GeoJson style dict polygon.
    geom_crs : str
        EPSG number for coordinates of the input GeoJson style dict polygon.

    Returns
    -------
    dict
        4326 GeoJson style dict polygon.

    """

    # geom_d = bbox_to_dict(bbox)
    if geom_crs == 'EPSG:4326':
        g4326 = geom
    else:
        g4326 = warp.transform_geom(src_crs=geom_crs,
                                    dst_crs='EPSG:4326',
                                    geom=geom)
    return g4326.copy()

def bbox_to_dict(bbox:str='-269985.0,90015.0,-265875.0,95895.0'):
    """
    Converts bbox string to GeoJSON like dictionary
    
    Parameters
    ----------
    bbox : str, optional
            The default is '-269985.0,90015.0,-265875.0,95895.0'.

    Returns
    -------
    g_json : dict
        GEoJSON like dictionary

    """
    #bbox='-269985.0,90015.0,-265875.0,95895.0'
    #crs='EPSG:3979'
    # convert bounding box strings to dict of floats

    b=[float(b) for b in bbox.split(',')]
    #convert list to shapely polygon to geojson style dict
    g_json = mapping(box(*b))
    return g_json

def gdf_to_dict(gdf: gpd.geodataframe.GeoDataFrame, field_id=None, field_value=None):
    """
    Converts geodataframe geometry to GeoJSON like dictionary
    
    Parameters
    ----------
    gdf : gpd.geodataframe.GeoDataFrame
    field_id : Optional
        id of the field to filter geometry on
    field_value: Optional
        Value of the field in the field_id to select a geometry

    Returns
    -------
    poly_json : dict
        GEoJSON like dictionary
    """
    if field_id and field_value:
        poly_json = mapping(gdf.loc[gdf[field_id] == field_value].geometry.values[0])
    else :
        poly_json = mapping(gdf.geometry[0])
    return poly_json

def validate_num_vertex(gdf:gpd.geodataframe.GeoDataFrame, field_id=None, field_value=None):
    """ Validate that the number of verticies from a geopackage or geojson geom is not 
    greater than 500 (this value can be changed)"""
    if field_id and field_value :
        vertex = shapely.get_num_coordinates(gdf.loc[gdf[field_id] == field_value].geometry.values[0])
    else:
        vertex = shapely.get_num_coordinates(gdf.geometry[0]) 
    
    if vertex > 500 :
        return False
    else:
        return True

def stac_api_search_query(collections:list,
                          g4326:dict,
                          datetime_filter:str=None) -> dict:
    """
    STAC API query for api/search

    Parameters
    ----------
    collections : list
        A collection name in a list.
    g4326 : dict
        4326 GeoJson style dict polygon.
    datetime_filter: str
    Filter based on RFC 3339
    Should follow formats
    A date-time: "2018-02-12T23:20:50Z"
    A closed interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
    Open intervals: "2018-02-12T00:00:00Z/.." or "../2018-03-18T12:31:12Z"

    Returns
    -------
    query : dict
        STAC API query for api/search.

    """
    query = {"intersects":g4326,"collections":collections}
    if datetime_filter:
        # Validate and transform query to rfc 3339
        datetime_filter = valid_rfc3339(datetime_filter)
        if datetime_filter:
            query.update({"datetime":datetime_filter})
        
    return query

class DatacubeExtract():
    """All standard Datacube Extract parameters and methods
    TODO: Moved def outside of class if not necessary"""

    def __init__(self,debug=False):
        self._main_log_name = 'extract_main.log'
        self._error_log_name = 'extract_error.log'
        self._main_log = []
        self._error_log = []
        self._debug = debug
        if self._debug:
            self._debug_log = []
        # # Initialise DatacubeStandard functionality
        # DatacubeStandard.__init__(self)
        self._hpc_access_levels = ['stage','prod']
        return

    def __del__(self):
        if self:
            self.write_logs()
        return

    def copy_cog(self,
                 dst,
                 out_file,
                 **kwargs):
        """

        Parameters
        ----------
        dst : str
            Path to input raster.
        out_file : str
            path to output raster.
        **kwargs : TYPE
            Other raster arguments.

        Returns
        -------
        None.

        """
        rscopy(dst,out_file,copy_src_overviews=True,**kwargs)
        return


    def collection_str_to_df(self,collections:str):
        """
        Create a dataframe of collection and asset
    
        Parameters
        ----------
        collections : str
            List of collection and asset with the convention:
                'collection1:asset,collection2:asset2,collection3'
            Asset are optionnal
    
        Returns
        -------
        dataframe
            Liste of collecitons
    
        """
        #Validate that the collections are a str
        if not isinstance(collections, str):
            raise ValueError('Collection should be a string')
        
        collections_list = collections.split(",")
        collection_list = []
        asset_list = []
        for a in collections_list:
            b = a.strip()
            #print(b)
            if ':' in b:
                collection = b.split(":")[0]
                asset = b.split(":")[1]
            else:
                collection = b
                asset = None #'Na'
            collection_list.append(collection)
            asset_list.append(asset)
        df = pandas.DataFrame({'collection': collection_list, 'asset': asset_list})
        return df
    

    def collection_str_to_dict(self,collections:str) -> dict:
        """
        Create a dictionnary of collection and asset

        Parameters
        ----------
        collections : str
            List of collection and asset with the convention:
                'collection1:asset,collection2:asset2,collection3'
            Asset are optionnal

        Returns
        -------
        dict
            Liste of collecitons

        """
        collection_dict = {}
        collections_list = collections.split(",")
        number=0
        for a in collections_list:
            b = a.strip()
            collection_dict[number] = {}
            b = a.strip()
            if ':' in b:
                collection = b.split(":")[0]
                asset = b.split(":")[1]
            else:
                collection = b
                asset = None
            collection_dict[number]['collection'] = collection
            collection_dict[number]['asset'] = asset
            number+=1
        return collection_dict


    def add_overviews(self,
                      img,
                      resample:str='nearest',
                      blocksize:int=512):
        """
        Parameters
        ----------
        img : rasterio.io.DataserReader
            resample : str, optional
            resampling_method :  Resampling alogrithms (‘nearest’, ‘cubic’,
                                                        ‘average’, ‘mode’, and ‘gauss’)
            The default is 'nearest'.

        blocksize : int, optional
            The default is 512.

        Returns
        -------
        img : rasterio.io.DataserReader
            DESCRIPTION.

        """
        # TODO calculate dec_factors based on num of pixels
        rows, columns = img.shape
        dec_factors = self.overview_level(rows, columns, blocksize)
        if dec_factors :
            print('Overviews added to the outfile...')
            resamp = resample_value(resample)
            img.build_overviews(dec_factors,resamp)
        else:
            print('Output too small for overview creation...')
        return img


    def overview_level(self,
                       rows:int,
                       columns:int,
                       blocksize:int=512) -> list:
        """
        Calculate overviews based on a tif file

        Parameters
        ----------
        rows : int
            Number of rows of the array.
        columns : int
            Number of columns of the array.
        blocksize : int, optional
            The default is 512.

        Raises
        ------
        Exception
            DESCRIPTION.

        Returns
        -------
        list
            list of overview level.
        or None
            if no overviews are neede in the file

        """
        if blocksize <= 0:
            raise Exception('Error blocksize should be grater than 0')
        # rows, columns = arr.shape
        maximum = max(rows, columns)
        factor = 0
        while maximum >= blocksize:
            maximum = maximum/2
            factor +=1
        print('Overviews:', factor)
        factors = []
        for i in range(factor):
            factors.append(2**(i+1))
        if factor == 0:
            return None
        else:
            return factors


    def calculate_size(self,
                       deltax:int,
                       deltay:int,
                       cellsize:int) -> int:
        """
        Calculate number of pixels

        Parameters
        ----------
        deltax : int
            Delta x in meters.
        deltay : int
            Delta y in meters.
        cellsize : int
            Pixel size.

        Returns
        -------
        nb_pixel : int
            Number of pixel.

        """

        nb_pixel = (deltax*deltay)/(cellsize**2)
        return nb_pixel


    def save_cog_from_file(self,
                           img_name:pathlib.Path,
                           tifftag_datetime:str=None,
                           resampling_method:str='average',
                           blocksize:int=512,
                           overviews:bool=False) -> None:
        """
        Saves tif file to cog, assumes kwargs has height, width and transform

        Parameters
        ----------
        img_name : pathlib.Path
            The input and output
        tifftag_datetime : str, optional
            TIFFTAG_DATETIME value must be in format YYYY:MM:DD hh:mm:ss.
            The default is None.
        resampling_method :  Resampling alogrithms ('nearest', 'cubic',
                                                    'average', 'mode', and 'gauss')
        blocksize : int, optional
        overviews : bool, optional

        Returns
        -------
        None.

        """
        if tifftag_datetime:
            self.check_date_time(tifftag_datetime)

        with rasterio.open(img_name, 'r') as img:
            arr = img.read(1)
            kwargs = img.profile
            
        with TemporaryDirectory() as temp_dir:
            temp_dir_tif = pathlib.Path(temp_dir,f'{img_name.name}.temp')
            kwargs['compress'] = 'LZW'
            kwargs['tiled'] = True
            kwargs['blockxsize'] = blocksize
            kwargs['blockysize'] = blocksize
    
            with rasterio.open(temp_dir_tif,'w+',**kwargs) as new:
                new.write(arr,indexes=1)
                if overviews:
                    new = self.add_overviews(new, resampling_method, blocksize)
                else:
                    print('No overviews added to file')
                if tifftag_datetime:
                    new.update_tags(TIFFTAG_DATETIME=tifftag_datetime)
            os.remove(img_name)
            self.copy_cog(temp_dir_tif,img_name,**kwargs)


    def calculate_file_size(self,
                            dtype:str,
                            width:int,
                            height:int):
        """
        Calculate the file size to define if bigtiff needs to be created of not
        BIGTIFF is created when raster size is bigger than 3GB - to keep a buffer of 1000MG for headers

        Parameters
        ----------
        dtype : str
            DESCRIPTION.
        width : int
            DESCRIPTION.
        height : int
            DESCRIPTION.

        Returns
        -------
        size_gb : TYPE
            DESCRIPTION.

        """
        size_per_pixel = int(re.findall(r'\d+', dtype)[0])

        size_bits = width * height * size_per_pixel
        size_bytes = size_bits / 8
        size_mb = size_bytes / 1024**2
        size_gb = size_bytes / 1024**3
        return size_gb


    def write_logs(self):
        if self._main_log:
            self.append_to_file(self._main_log, self._main_log_name)
        if self._error_log:
            self.append_to_file(self._error_log, self._error_log_name)
        if self._debug:
            self.append_to_file(self._debug_log, self._error_log_name)
        return


    def stac_api_query(self,
                       collections:dict,
                       g4326:dict,
                       datetime_filter:str=None) -> dict:
        """
        STAC API query for api/search

        Parameters
        ----------
        collections : dict
            A collection name in a dictionnary.
        g4326 : dict
            4326 GeoJson style dict polygon.
        datetime_filter: str
        Filter based on RFC 3339
        Should follow formats
        A date-time: "2018-02-12T23:20:50Z"
        A closed interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
        Open intervals: "2018-02-12T00:00:00Z/.." or "../2018-03-18T12:31:12Z"

        Returns
        -------
        query : dict
            STAC API query for api/search.

        """
        query = {"intersects":g4326,"collections":collections}
        if datetime_filter:
            # Validate and transform query to rfc 3339
            datetime_filter = valid_rfc3339(datetime_filter)
            if datetime_filter:
                query.update({"datetime":datetime_filter})
            
        return query



    # @win_ssl_patch
    def scrape_urls_from_result(self,
                                urls:list,
                                results:dict,
                                collections:list,
                                asset_role='data',
                                asset_id=None,):
        """
        Parses Feature Collection for assets

        Parameters
        ----------
        urls : list
            list of tuple (url,collection,datetime) extracted from results.
        results : dict
            FeatureCollection (items) JSON response from STAC API search endpoint
        collections : list
            Collection filter list.
        asset_role : TYPE, optional
            Asset role. The default is 'data'.
        asset_id : TYPE, optional
            Asset ID. The default is None.

        Returns
        -------
        None.

        """
        returned_collections = []
        item_datetime = None
        features = results['features']

        if self._debug:
            self._debug_log.append(f'features : {features}')
            self._debug_log.append(f'type(features) : {type(features)}')
            self._debug_log.append(f'len(features) : {len(features)}')
        for feature in features:
            # for each defined cog, pass url to url
            returned_collection = feature['collection']
            returned_collections.append(returned_collection)
            # Only return urls from collections requested
            # Franklin bug returns multiple collections from mutlipage result link
            if returned_collection in collections:
                # Extract item datetime to use in ordering
                try:
                    item_datetime = feature['properties']['datetime']
                except:
                    item_datetime = None
                if self._debug:
                    self._debug_log.append(f'feature : {feature}')
                    self._debug_log.append(f'type(feature) : {type(feature)}')
                for key,values in feature.items():
                    if self._debug:
                        self._debug_log.append(f'key : {key}')
                        self._debug_log.append(f'values: {values}')
                    if key == 'assets':
                        for asset_key, asset_values in values.items():
                            if self._debug:
                                self._debug_log.append(f'asset_key : {asset_key}')
                                self._debug_log.append(f'asset_values : {asset_values}')
                            if asset_id:
                                # only return url for specific asset_id and asset role
                                if asset_key == asset_id:
                                    if asset_role in asset_values['roles']:
                                        url = asset_values['href']
                                        self._main_log.append(f'url: {url}')
                                        urls.append((url,returned_collection,item_datetime))
                            else:
                                # get all the asset_id with specified role
                                if asset_role in asset_values['roles']:
                                    url = asset_values['href']
                                    self._main_log.append(f'url: {url}')
                                    urls.append((url,returned_collection,item_datetime))
        print(f'INFORMATION : collections returned {list(set(returned_collections))}')
        return


    def append_to_file(self,
                       lines:str,
                       fname:str)->str:
        """
        Appends a list to an existing file, used mainly for logging

        Parameters
        ----------
        lines : str
            String to add.
        fname : str
            file name.

        Returns
        -------
        str
            Succes or error message.

        """
        try:
            with open(fname,'a') as f:
                for line in lines:
                    f.write(f'{line}\n')
            r = f'Wrote {len(lines)} lines to {fname}.'
        except:
            r = f'Failed to write to {fname}.'
        return r


    def check_outpath(self,
                      outpath,
                      mode:int=511):
        """
        Creates directory structure if it doesnt already exist

        Parameters
        ----------
        outpath : str
            Path to folder.
        mode : int, optional
            Permission for file acces

        Returns
        -------
        outpath : str
            Path to folder.

        """
        outpath = pathlib.Path(outpath)
        if not outpath.is_dir():
            outpath.mkdir(parents=True,exist_ok=True, mode=mode)
        return outpath


    def check_outfile(self,
                      file_path):
        """
        Check if a file existe and delete it if so

        Parameters
        ----------
        file_path : str
            Path to file.

        Returns
        -------
        None.

        """
        file_path = Path(file_path)
        if file_path.is_file() == True:
            print(f"{file_path} already exist, it will be overwritten.")
            os.remove(file_path)
        else:
            print(f"{file_path} does not exist, it will be created.")


    def check_date_time(self,
                        tifftag_datetime:str)->None:
        """
        Validate if tifftag_datetime follow format YYYY:MM:DD hh:mm:ss

        Parameters
        ----------
        tifftag_datetime : TYPE
            Should follow format: 'YYYY:MM:DD hh:mm:ss'.

        Raises
        ------
        ValueError
            Worng format or before 1900 or after today.

        Returns
        -------
        None.

        """
        #TODO make this more flexible or rename check tifftag_datetim
        try:
            d = datetime.strptime(tifftag_datetime, '%Y:%m:%d %H:%M:%S')
        except ValueError:
            raise ValueError("Incorrect data format, should be YYYY:MM:DD hh:mm:ss, not", tifftag_datetime)
        if d <= datetime(1900, 1, 1):
            raise ValueError('Date should be after 1900')
        if d >= datetime.now():
            raise ValueError('Date should be before tomorrow')
        # else:
        #     print("Datetime is None")

    @nrcan_requests_ca_patch
    def request_to_file(self,
                        u:str,
                        img_name:str,
                        f_main='main.log')->str:
        """
        Sends get request to wcs and writes out result to image

        Parameters
        ----------
        u : str
            GetCoverage Request.
        img_name : str
            Path of the destination tif file.
        f_main : TYPE, optional
            The default is 'main.log'.

        Raises
        ------
        Exception
            Error with the wcs call, response is not 200.

        Returns
        -------
        str
            Succes message.

        """

        main_log = []
        main_log.append(f'getting wcs result stream from  get request {u}')
        main_log.append(f'writing result to {img_name}')
        self.append_to_file(main_log,f_main)
        r = requests.get(u)
        sc = r.status_code
        reason = r.reason
        # write image out to test.tif in 128 byte chunks if r.status_code=200
        if sc == 200 :
            with open(img_name,'wb') as img:
                for chunk in r.iter_content(chunk_size=128):
                     img.write(chunk)                
            img.close()
        else:
            raise Exception('Error when getting wcs result:', sc)
        r.close()
        return f"{img_name} request sc:{sc} reason:{reason}"


    def wcs_coverage_extract(self,bbox_as_dict:dict,
                             crs:str,
                             protocol:str,
                             lid:str,
                             level:str,
                             srv_id:str,
                             cellsize:int=None,
                             cwd:str='./',
                             study_area:str='wcs_extract',
                             method:str='nearest',
                             suffix:str='wcs',
                             f_main='main.log',
                             tifftag_datetime=None,
                             overviews=False):
        """
        Extract WCS coverage based on bbox

        Parameters
        ----------
        bbox_as_dict : dict
            The extract bounding box as geojson style dictionary.
        crs : str
            EPSG number.
        cellsize : int
            Cellsize in meters.
        protocol : str
           Internet protocol (http or https).
        lid : str
            Layer name.
        level : str
            S3 level (beta, dev, stage, prod) Beta is on stage.
        srv_id : str
            Web service name.
        cwd : str, optional
            The working directory where the output is written.
            The default is './'.
        study_area : str, optional
            The study area discriptor to be included in file name.
            The default is 'test'.
        method : str, optional
            resampling_method :  Resampling alogrithms ('nearest', 'cubic',
                                                        'average', 'mode', and 'gauss')
        suffix : str, optional
            The portion of file name to be added before .tif.
            The default is 'wcs-dtm'.
        f_main : str, optional
            Main log file name. The default is 'main.log'
        overviews : bool, optional
            Trigger the creation of overviews if True in the output cog

        Returns
        -------
        Request
            request result.
        img_name : TYPE
            Path and file name.

        Call example
        ----------
        dex = dce.DatacubeExtract()
        poly = dex.bbox_to_poly(bbox='-1862977,312787,-1200000,542787')
        poly_dic = dex.poly_to_dict(poly)
        dex.wcs_coverage_extract(poly_dic,'3979',300,'HTTPS','dtm','stage',
                                 'elevation','./')
        """
        # cellsize None in 20
        if cellsize == None:
            cellsize = 20
        # convert python geojson dict to shapely geom
        cwd = self.check_outpath(cwd)
        bbox = shape(bbox_as_dict)
        #  get url for request and input bounds tuple for comparison to result
        u = self.wcs_request(bbox,crs,cellsize,protocol,lid,level,srv_id,
                               f_main)
        # print('{} calcd u {}'. format(suffix,u))
        # write image out to test.tif in 128 byte chunks if r.status_code=200
        img_name=pathlib.Path(os.path.join(cwd,"{}_sample-{}.tif".format(study_area,suffix)))
        self.check_outfile(img_name)
        self.request_to_file(u[0],img_name,f_main)
        # TODO ensure tifftag_datetime has actual datetime of WCS data rather than None
        # Read headers from file to validate theire is data in the file
        valid = self.validate_wcs_output(img_name, cellsize)
        # reopen and add overviews
        if valid:
            self.save_cog_from_file(img_name,resampling_method=method, overviews=overviews, tifftag_datetime=tifftag_datetime)
            return img_name
        else :
            return None
            
    
    def validate_wcs_output(self, img_name:pathlib.Path, cellsize:int):
        """
        Validate that the output tiff from wcs request as data inside,
        This allow to only return cog taht are not empty from wcs request"""
        with rasterio.open(img_name, 'r') as img:
            resx, resy = img.res
        if resx != cellsize or resy != cellsize:
            print('Extraction from WCS is empty...')
            os.remove(img_name)
            return False
        else:
            return True


    def wcs_request(self,
                    bbox,
                    crs='EPSG:3979',
                    cellsize=20,
                    protocol='https',
                    lid='dtm',
                    level='beta',
                    srv_id='elevation',
                    f_main='main.log'):
        """
        Create a WCS GetCoverage call based on bbox and cell size

        Parameters
        ----------
        bbox : shapely polygon
            The bounding box as shapely polygon.
        crs : str
            EPSG number.
        cellsize : int
            Cellsize in meters.
        protocol : str
           Internet protocol (http or https).
        lid : str
            Layer name.
        level : str
            S3 level (beta, dev, stage, prod) Beta is on stage.
        srv_id : str
            Web service name.
        f_main : str, optional
            Main log file name. The default is 'main.log'

        Returns
        -------
        WCS
           WCS GetCoverage call
        BBOX : tuple
            BBOX as tuple

        Call example
        ----------
        dex = dce.DatacubeExtract()
        poly = dex.bbox_to_poly(bbox='-1862977,312787,-1200000,542787')
        request, bbox = dex.wcs_request(poly,cellsize=300,level='stage')
        """

        # Converts requests to EPSG:3979
        crs3979='EPSG:3979'
        # Need to select an area evenly divdable by pixel size with bbox
        # Equal to minx-(cellsize/2),minx-(cellsize/2),maxx-(cellsize/2),
        # maxy-cellsize/2)
        # level='stage'
        rd=self.get_root_domain(level)
        srv_id='elevation'
        # convert bbox to 3979 for calculation
        if '3979' in crs:
            bbox3979=bbox
        else:
            bbox3979 = shape(warp.transform_geom(crs,crs3979,mapping(bbox)))
        # print('submited bbox {}'.format(mapping(bbox)))
        # print('bbox3979 {}'.format(mapping(bbox3979)))
        minx,miny,maxx,maxy=bbox3979.bounds
        # print('original 3979 submited to wcs minx,miny,maxx,maxy {}, {}, {},
                # {}'.format(minx,miny,maxx,maxy))
        dx = abs(maxx - minx)
        dy = abs(maxy - miny)
        # print("dx: {}".format(dx))
        # print("dy: {}".format(dy))
        # find the delta x and y
        # deltax = self.getEvenCellSizeLength(dx,cellsize)
        # deltay = self.getEvenCellSizeLength(dy,cellsize)
        deltax = self.get_cells(dx,cellsize)*cellsize
        deltay = self.get_cells(dy,cellsize)*cellsize
        nb_pixel = self.calculate_size(deltax, deltay, cellsize)

        if nb_pixel > 100000000:
            #if nb_pixel > 10000000000:
            raise ValueError('Request reach the size limit, try a smaller area or a lower resolution.')

        maxx = minx + deltax
        maxy = miny + deltay
        # print('calced 3979 submited to wcs bbox minx,miny,maxx,maxy {},
              #{}, {}, {}'.format(minx,miny,maxx,maxy))
        coords=box(minx,miny,maxx,maxy)
        # pass bbox and identifier into call
        root = "{}://{}/{}".format(protocol,rd,srv_id)
        params = "service=WCS&version=1.1.1&request=GetCoverage&format=image/geotiff"
        params += "&identifier={}".format(lid)
        params += "&BoundingBox={},{},{},{},urn:ogc:def:crs:EPSG::3979".format(minx,miny,maxx,maxy)
        params += "&GridBaseCRS=urn:ogc:def:crs:EPSG::3979&GridOffsets={:.1f},-{:.1f}"\
            .format(float(cellsize),float(cellsize))
        return "{}?{}".format(root,params),coords.bounds


    def tuple_to_bbox(self,
                      bounds_tuple:tuple):
        """
        Covert bounds tuple to bbox string

        Parameters
        ----------
        bounds_tuple : tuple

        Returns
        -------
        bbox : str

        """
        bbox = ','.join([str(x) for x in bounds_tuple])
        return bbox


    def bbox_to_tuple(self,
                      str_bbox:str)->tuple:
        """
        Covert bbox string to bounds tuple

        Parameters
        ----------
        str_bbox : str

        Returns
        -------
        bbox : tuple

        """
        bbox = tuple(float(v) for v in str_bbox.split(','))
        return bbox


    def dict_to_poly(self,
                     bbox_dict)->shapely.geometry.polygon.Polygon:
        """
        Convert GeoJson style dictionary to shapely polygon

        Parameters
        ----------
        bbox_dict : dict

        Returns
        -------
        shapely.geometry.polygon.Polygon

        """
        return shape(bbox_dict)


    def poly_to_dict(self,
                     bbox_poly:shapely.geometry.polygon.Polygon)->dict:
        """
        Converts shapely polygon to GeoJson style dict

        Parameters
        ----------
        bbox_poly : shapely.geometry.polygon.Polygon
            Polygon of the bbox

        Returns
        -------
        dict
            Dictionnary of the bbox as geojson format.

        """
        return mapping(bbox_poly)


    def dict_to_vector_file(self,
                            dict_poly,
                            crs,outpath,
                            filename,driver='GeoJSON'):
        """
        Converts a polygon dict geojson defintion to a vector definition file

        Parameters
        ----------
        dict_poly : dict
        crs : str
            input crs of the geometry.
        outpath : str
            Output path.
        filename : str
            Output file name.
        driver : TYPE, optional
            The default is 'GeoJSON'.

        Returns
        -------
        fname : str
            Output of the geojson file.

        """
        # check outpath exists
        outpath=self.check_outpath(outpath)
        gs=gpd.GeoSeries(shape(dict_poly),crs=crs)
        fname=os.path.join(outpath,filename)
        gs.to_file(os.path.join(outpath,filename),driver='GeoJSON')
        return fname


    def transform_dict_to_dict(self,
                               bbox_dict,
                               in_crs,out_crs):
        """
        Reproject geojson dict to geojson_dict

        Parameters
        ----------
        bbox_dict : dict
        in_crs : str
            Input crs of the bbox.
        out_crs : str
            Ouput crs.

        Returns
        -------
        g : dict
            Reprojection dictionnary of the geometry.

        """
        g = warp.transform_geom(in_crs,out_crs,bbox_dict)
        return g


    def transform_dict_to_poly(self,
                               bbox_dict:dict,
                               in_crs:Union[str,rasterio.crs.CRS],
                               out_crs:Union[str,rasterio.crs.CRS])-> shapely.geometry.polygon.Polygon:
        """
        Reproject geojson dict to shapely polygon

        Parameters
        ----------
        bbox_dict : dict
            Dictionnary of the bbox as geojson format
            (ex. {'type': 'Polygon', 'coordinates': (((371107.0, 2601049.0),\
            (371107.0, 1299069.0), (-1285959.0, 1299069.0),\
            (-1285959.0, 2601049.0), (371107.0, 2601049.0)),)})
        in_crs : str|rasterio.crs.CRS
            Input crs in string (ex. 'EPSG:3979') or rasterio.crs.CRS object (ex. rasterio.crs.CRS().from_epsg(3979))
        out_crs : str|rasterio.crs.CRS
            Output crs in string (ex. 'EPSG:3979') or rasterio.crs.CRS object (ex. rasterio.crs.CRS().from_epsg(3979))

        Returns
        -------
        TYPE
            DESCRIPTION.

        """
        g = warp.transform_geom(in_crs,out_crs,bbox_dict)
        return self.dict_to_poly(g)


    def bbox_to_poly(self,
                     bbox:str='-269985.0,90015.0,-265875.0,95895.0'):
        """

        Parameters
        ----------
        bbox : str, optional
             The default is '-269985.0,90015.0,-265875.0,95895.0'.

        Returns
        -------
        bbox : shapely.polygon

        """
        #bbox='-269985.0,90015.0,-265875.0,95895.0'
        #crs='EPSG:3979'
        # convert bounding box strings to dict of floats

        b=[]
        for t in bbox.split(','):
            b.append(float(t))
        #create shapely polygon bbox
        bbox=box(*b)
        return bbox


    def get_root_domain(self,
                        level:str='stage')->str:
        """
        Return service url base on level

        Parameters
        ----------
        level : str, optional
            Env level (stage, prod, dev). The default is 'prod'.

        Returns
        -------
        str
            service url.

        """
        if not level in ['beta','dev','stage','prod']:
            level = 'stage'

        if 'beta' in level:
            rd = 'beta.datacube-stage.services.geo.ca/ows/'
        else:
            if 'prod' in level:
                rd='datacube.services.geo.ca/ows'
            else:
                rd = 'datacube-{}.services.geo.ca/ows'.format(level)
        return rd

    def get_cells(self,
                  delta:int,
                  cellsize:int) -> int:
        """
        Return a number of pixels needed to make sure that the number of pixel
            can be divise by the cellsize.
        Parameters
        ----------
        delta : int
            Numerator
        cellsize : int
            Denominator

        Returns
        -------
        cells : int
            Number of pixels

        """
        numa,rema=divmod(delta,cellsize)
        if rema!=0:
            cells=(numa+cellsize)
        else:
            cells=numa
        return cells

 

if __name__ == '__main__':
    print('No cli functionality available, module only use for now')
    # main(sys.argv[1:])
