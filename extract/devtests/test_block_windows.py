"""
Tests on rasterio window functionality to improve data extraction,
resampling and reprojection

"""
# Python standard library
import datetime
import math
import pathlib
import sys
import os

# Python custom modules
import numpy
import rasterio
from rasterio.transform import Affine, from_origin
from rasterio.warp import aligned_target, calculate_default_transform, reproject, transform
from shapely.geometry import box as sgbox
from rasterio.shutil import copy as rscopy
from shapely.geometry import box, mapping
import rioxarray
import threading
import xarray

from math import ceil, floor

# Datacube local modules
# Ensure syspath first  reference is to .../dc_extract/... parent of all local files
# for this file it is  .../dc_extract/*/devtest/<modules> so need parents[2]
# define number of subdirs from 'root' (dc_extract)
_CHILD_LEVEL = 2
_DIR_NEEDED = str(pathlib.Path(__file__).parents[_CHILD_LEVEL].absolute())
if _DIR_NEEDED not in sys.path:
    sys.path.insert(0,_DIR_NEEDED)      
import ccmeo_datacube.extract.extract as dce



def test_cog(res='med'):
    """Sample cog to run tests on"""
    if res == 'fine':
        return 'https://datacube-stage-data-public.s3.ca-central-1.amazonaws.com/store/elevation/hrdem-hrdsm/hrdem/hrdem-nb-dem.tif'
    elif res == 'med': 
        return 'http://datacube-prod-data-public.s3.ca-central-1.amazonaws.com/store/elevation/cdem-cdsm/cdem/cdem-canada-dem.tif'
    else:
        print('no res specified')
        
def test_bbox(size='05k'):
    """Sample bboxes based on Fredericton NB 5km x 5km"""
    if size == '05k':
    # 5km by 5km
        bbox= ('2150469.4724999964,'
               '144975.05299999937,'
               '2155469.4724999964,'
               '149975.05299999937')
        bbox_crs = 'EPSG:3979'
    elif size == '50k':
        # 50 km by 50 km
        bbox= ('2150469.4724999964,'
               '144975.05299999937,'
               '2200469.4724999964,'
               '194975.05299999937')
        bbox_crs = 'EPSG:3979'
    elif size == '100k':
        # 100 km by 100 km
        bbox= ('2150469.4724999964,'
               '144975.05299999937,'
               '2250469.4724999964,'
               '244975.05299999937')
        bbox_crs = 'EPSG:3979'
    else:
        # 5km by 5km
        bbox= ('2150469.4724999964,'
               '144975.05299999937,'
               '2155469.4724999964,'
               '149975.05299999937')
        bbox_crs = 'EPSG:3979'
    return bbox,bbox_crs

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

def test_window_from_bounds(size='05k'):
    """Tests window and block window based bounds, write out values to log"""
    f = pathlib.Path(__file__).parent.joinpath('logs')
    if not f.is_dir():
        f.mkdir(parents=True,exist_ok=True)
    f = f.joinpath('window_bounds.log')
    if not f.is_file():
        fp = f.open(mode='r')
    else:
        fp = f.open(mode='a')
        
    dex = dce.DatacubeExtract()
    json_out_dir = pathlib.Path(__file__).parent.joinpath('vectors')

    img = rasterio.open(test_cog())
    bbox,bbox_crs = test_bbox(size)

    fp.write(f'Original Values for {size}\n')
    fp.write(f'Bbox: {bbox}\n')
    orig_w = rasterio.windows.from_bounds(*bbox,transform=img.transform)
    orig_b = rasterio.windows.bounds(window=orig_w,transform=img.transform)
    fp.write(f'Window: {orig_w}\n')
    fp.write(f'Bounds of window {orig_b}\n')
    fp.write(f'Window transform {img.window_transform(orig_w)}\n')
    # Write out bounds as geojson
    dict_poly = dex.poly_to_dict(sgbox(*orig_b))
    dex.dict_to_vector_file(dict_poly, bbox_crs, json_out_dir, f'input-{size}.geojson')

    fp.write(f'\nTap window for {size}')

    tap_w = dce.tap_window(img.transform,bbox,bbox_crs)
    tap_b = rasterio.windows.bounds(window=tap_w,transform=img.transform)
    fp.write(f'Window: {tap_w}\n')
    fp.write(f'Bounds of window {tap_b}\n')
    fp.write(f'Window transform {img.window_transform(tap_w)}\n')
    # Write out bounds as geojson
    dict_poly = dex.poly_to_dict(sgbox(*tap_b))
    dex.dict_to_vector_file(dict_poly, bbox_crs, json_out_dir, f'tap-{size}.geojson\n')
    
    fp.write(f'\nWindows as image block windows that intersect bbox  for {size}\n')
    bws = dce.bbox_windows(test_cog(),bbox,bbox_crs,band=1,clip=False)
    for bw in bws:
        bw_b = rasterio.windows.bounds(window=bw,transform=img.transform)
        fp.write(f'\nWindow: {bw}\n')
        fp.write(f'Bounds of window {bw_b}\n')
        fp.write(f'Window transform {img.window_transform(bw)}\n')
        # Write out bounds as geojson
        dict_poly = dex.poly_to_dict(sgbox(*bw_b))
        dex.dict_to_vector_file(dict_poly, bbox_crs, json_out_dir, f'bw-{size}-{bw.col_off}-{bw.row_off}.geojson\n')
    
    fp.write(f'\nWindows derived from image block windows clipped to bbox for {size}')
    bws = dce.bbox_windows(test_cog(),bbox,bbox_crs,band=1,clip=True)
    for bw in bws:
        bw_b = rasterio.windows.bounds(window=bw,transform=img.transform)
        fp.write(f'\nWindow: {bw}')
        fp.write(f'Bounds of window {bw_b}\n')
        fp.write(f'Window transform {img.window_transform(bw)}\n')
        # Write out bounds as geojson
        dict_poly = dex.poly_to_dict(sgbox(*bw_b))
        dex.dict_to_vector_file(dict_poly, bbox_crs, json_out_dir, f'clip-{size}-{bw.col_off}-{bw.row_off}.geojson')
    
    fp.close()
    img.close()
    return

@dce.print_time
def test_modify_by_window():
    """
    Test case for modify by windows
    
    Launches modify_by_window in debug mode.
    Writes images amd geojson bboxes to test-images sub folder.
    Writes modify_by_window.log to logs subfolder.
    
    Example Usage
    -------------
    import dc_extract.extract.devtests.test_block_windows as tbw
    tbw.test_modify_by_window()
    Start: 2022-09-21 16:08:26.046730
    05k 16 bilinear 3979
    05k 16 bilinear 2960
    05k 20 bilinear 3979
    05k 20 bilinear 2960
    ...
    50k 30 average 3979
    50k 30 average 2960
    50k 32 average 3979
    50k 32 average 2960
    End: 2022-09-21 16:11:04.004297
    Total time 157.957567 seconds
    """
    
    sizes = ['05k','50k', '100k']
    ress = [1, 16, 32]
    # ress = [16, 20, 30, 32]
    resamples = ['bilinear']
    # resamples = ['bilinear', 'nearest', 'average']
    epsgs = [3979]
    # epsgs = [3979, 2960]
    # ress = [ress[1]]
    # resamples = [resamples[0]]
    for size in sizes:
        bbox,bbox_crs = test_bbox(size)
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
                        'out_path':f,
                        'bbox':bbox,
                        'bbox_crs':bbox_crs,
                        'out_res':res,
                        'out_crs':f'EPSG:{epsg}',
                        'resample':resample,
                        'debug':True
                        }
                    print(size,res,resample,epsg)
                    # print(params)
                    #test de changement par Charlotte
                    modify_by_window(**params)
                    # modify_by_window(**params)
    return

def modify_by_window(img_path:str,
                     out_path:str,
                     bbox:tuple,
                     bbox_crs:str,
                     out_res:float=None,
                     out_crs:int=None,
                     resample:str='bilinear',
                     band:int=1,
                     debug:bool=False)->pathlib.Path:
    """
    Clips, resamples and reprojects-ish based on user input study area bbox

    Parameters
    ----------
    img_path : str
        The path to the source image.
    out_path : str
        The path to the clipped image.
    bbox : tuple
        The study area bbox.
    bbox_crs : str
        The EPSG crs of the study area.
    out_res : int, optional
        The resolution of the output, if none uses input resolution.
        Assumes images linear units are the same as input image.
        The default is None.
    out_crs : str, optional
        The EPSG crs of the output, if none native crs is used.
        The default is None.
    resample : str, optional
        The resample method to be used during reprojection. 
        The default is 'bilinear'.
    band : int, optional
        The index of the band to be used. The default is 1.
    debug : bool, optional
        Like verbose, generates detailed logs for debugging.
        The default is False.

    Returns
    -------
    None.

    Example Usage
    -------------
    params = {
        'img_path':'http://datacube-prod-data-public.s3.ca-central-1.amazonaws.com/store/elevation/cdem-cdsm/cdem/cdem-canada-dem.tif',
        'out_path':pathlib.Path('C:/users/<myusername>/Documents/test.tif'),
        'bbox':(2150469.4724999964,
               144975.05299999937,
               2155469.4724999964,
               149975.05299999937),
        'bbox_crs':'EPSG:3979',
        'out_res':30,
        'out_crs':None,
        'resample':'bilinear',
        'debug':False
        }

    modify_by_window(**params)

    """
    if debug:
        dp = pathlib.Path(__file__).parent.joinpath('logs')
        if dp.is_dir():
            debug_log = dp.joinpath('modify_by_window.log').open(mode='a')
        else:
            dp.mkdir(parents=True,exist_ok=True)
            debug_log = dp.joinpath('modify_by_window.log').open(mode='w')
        debug_log.write(f'\nDate: {datetime.datetime.now()}---------------\n')
        debug_log.write(f'\nIn file: {img_path}')
        debug_log.write(f'\nOut file: {out_path}')
        debug_log.write(f'\nOut res: {out_res}')
        debug_log.write(f'\nOut crs: {out_crs}')
        debug_log.write(f'\nOut resample: {resample}')

    # GDAL env vars discussed here https://trac.osgeo.org/gdal/wiki/ConfigOptions
    # with rasterio.Env(CHECK_WITH_INVERT_PROJ='NO'):
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
    
        # Get the full study area as a window and as spatial bounds in image native resolution
        # Assuming bbox_crs = img.crs
        # TODO Handle mulitple crs esp. out_crs difft from img.crs
        # TODO 1) Get the tap window in out_crs
        # TODO 2) Convert the tap window to img_crs
        # TODO 3) Use #2 to generate clipped window_boxes in img.crs
        # TODO 4) Read from #3 and write to out_img after converting windows to out_crs etc.
        
        #other idea to handle multiple crs epsg. out_crs diff from img.crs
        #TODO : 1) Ensure that the bbox is in img.crs
        #TODO : 2) Get block_windows fom img intersecting with bbox 
        #TODO : 3) For each block windows : reproject to out_crs, clip, write

        #The bbox tap is created inside the dce.bbox_windows()
        # bbox_full in native image crs
        bbox_full = dce.tap_window(img.transform,bbox,bbox_crs)
        #TODO : Remove bbox_full.height and bbox_full.width, not necessary, gives the same output
        (minx_full,
         miny_full,
         maxx_full,
         maxy_full) = rasterio.windows.bounds(bbox_full,
                                              img.transform)#,
                                              # bbox_full.height,
                                              # bbox_full.width)

        if debug:
            debug_log.write(f'\nFull window for study area {bbox_full}')

        # Get the study_area windows based on the image block_windows
        bbox_windows = dce.bbox_windows(img_path, bbox, bbox_crs)
        ##CHARLOTTE REMOVE AFTER USE
        # for bw in bbox_windows:
        #     bw_b = rasterio.windows.bounds(window=bw,transform=img.transform)
        #     # Write out bounds as geojson
        #     dict_poly = dex.poly_to_dict(sgbox(*bw_b))
        #     dex.dict_to_vector_file(dict_poly, bbox_crs, r'C:\Users\ccrevier\Documents\Datacube\MNEHR-cog\a-test\test_block_windows', f'bw-{bw.col_off}-{bw.row_off}.geojson')
        # ##
        
        if out_crs != img.crs or out_res != img.res[0]:
            # Reproject or resample the windows before writing
            # Get the transformed bounds for the full study area
            (dst_transform,
              dst_width,
              dst_height) = tap_params(in_crs=img.crs,
                                      out_crs=out_crs,
                                      in_left=minx_full,
                                      in_bottom=miny_full,
                                      in_right=maxx_full,
                                      in_top=maxy_full,
                                      in_width=bbox_full.width,
                                      in_height=bbox_full.height,
                                      out_res=out_res)
            # if debug:
            #     debug_log.write(f'\nDestination width {dst_width}, height {dst_height}')
            #     debug_log.write(f'\nDestination trans {dst_transform}')
            # Generate the full size output image array and populate with zeros DONT NEED IT
            # out_arr = numpy.zeros(shape=(dst_height,dst_width),dtype=img.dtypes[band-1])
            # Generate the out image profile
            out_profile = in_profile
            out_profile.update({'crs':out_crs,
                                'height':dst_height,
                                'width':dst_width,
                                'transform':dst_transform})
            # if debug:
            #     debug_log.write(f'\nOut profile: {out_profile}')
            with rasterio.open(out_path,mode='w',**out_profile) as out_img:
                # if debug:
                #     dex = dce.DatacubeExtract()
                #     # Write out the bounding box as geojson
                #     geojson = pathlib.Path(out_path)
                #     json_out_dir = str(geojson.parent)
                #     json_fn = str(geojson.name).replace(geojson.suffix,'.geojson')
                #     dict_poly = dex.poly_to_dict(sgbox(*out_img.bounds))
                #     dex.dict_to_vector_file(dict_poly, out_img.crs, json_out_dir, json_fn)

                for bbox_win in bbox_windows:
                    # Resample and or reproject
                    # Get the in window bounds, transform and array
                    # if debug:
                    #     debug_log.write(f'\nbbox_win: {bbox_win}')
                    minx,miny,maxx,maxy = rasterio.windows.bounds(window=bbox_win,transform=img.transform)
                    in_transform = from_origin(minx,maxy,img.res[0],img.res[0])
                    in_arr = img.read(band,window=bbox_win)
                    # if debug:
                    #     debug_log.write(f'\nin transform {in_transform}')
                    #     debug_log.write(f'\nin_arr shape {in_arr.shape}')
                    #     debug_log.write(f'\nin_arr {in_arr}')
                    # Reproject the window based in_array
                    out_arr, out_transform = reproject(source=in_arr,
                                                        src_transform=in_transform,
                                                        src_crs=img.crs,
                                                        src_nodata=img.nodata,
                                                        dst_crs=out_crs,
                                                        dst_nodata=img.nodata,
                                                        dst_resolution=out_res,
                                                        resampling=resample,
                                                        num_threads=1,
                                                        init_dest_nodata=True,
                                                        warp_mem_limit=0)
                    # if debug:
                    #     # TODO write to log instead of print
                    #     debug_log.write(f'\nout transform {out_transform}')
                    #     debug_log.write(f'\nout_arr shape {out_arr.shape}')
                    #     debug_log.write(f'\nout__arr {out_arr}')

                    # Get the out_array window offsets
                    #pas certaine de ce que ça fait ces quelques lignes là....
                    out_win = rasterio.windows.Window(col_off=0,row_off=0,width=out_arr.shape[-1],height=out_arr.shape[-2])
                    out_bounds = rasterio.windows.bounds(window=out_win,transform=out_transform)
                    print(out_bounds)
                    out_minx,out_miny,out_maxx,out_maxy = out_bounds
                    write_win = rasterio.windows.from_bounds(*out_bounds,transform=dst_transform)
                    # if debug:
                    #     debug_log.write(f'\nout_win {out_win}')
                    #     debug_log.write(f'\nout_bounds {out_bounds}')
                    #     debug_log.write(f'\nwrite_win : {write_win}')
                    
                    # Write out the window to the new image
                    out_img.write(out_arr,window=write_win)
                    del out_arr
                    del in_arr
                    del write_win
        else:
            # Generate the out image profile
            bbox_full_transform = from_origin(minx_full,maxy_full,img.res[0],img.res[0])
            out_profile = in_profile
            out_profile.update({'crs':out_crs,
                                'height':bbox_full.height,
                                'width':bbox_full.width,
                                'transform':bbox_full_transform})
            #update profile to add if needed the bigtiff tag
            file_size = dex.calculate_file_size(out_profile['dtype'], out_profile['width'], out_profile['height'])
            if file_size >= 3:
                out_profile['BIGTIFF']='YES'
                print('Outfile is bigger than 4GB, creation of BIGTIFF...')
            else :
                # kwargs['BIGTIFF']='NO'
                print('Outfile is smaller than 4GB, creation of TIFF...')

            # Write out the study area
            with rasterio.open(out_path,mode='w',**out_profile) as out_img:
                # TODO Add debug logging
                if debug:
                    dex = dce.DatacubeExtract()
                    # Write out the bounding box as geojson
                    geojson = pathlib.Path(out_path)
                    json_out_dir = str(geojson.parent)
                    json_fn = str(geojson.name).replace(geojson.suffix,'.geojson')
                    dict_poly = dex.poly_to_dict(sgbox(*out_img.bounds))
                    dex.dict_to_vector_file(dict_poly, out_img.crs, json_out_dir, json_fn)
                
                # Calculate relative offsets and create write window 
                for bbox_win in bbox_windows:
                    rel_col_offset = bbox_win.col_off - bbox_full.col_off
                    rel_row_offset = bbox_win.row_off - bbox_full.row_off
                    write_win = rasterio.windows.Window(col_off=rel_col_offset,
                                                        row_off=rel_row_offset,
                                                        width=bbox_win.width,
                                                        height=bbox_win.height)
    
                    # Read the array from the original image
                    out_arr = img.read(band,window=bbox_win)
                    # Write out the window to the new image
                    out_img.write(out_arr,window=write_win,indexes=band)
            
        img.close()
        if debug:
            debug_log.close()
    return out_path


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



#Idée c'est qu'il y a une fonction centrale, qui fait l'extraction par fenetre (extract_by_window())
#Avec des sous-fonctions qui fond le reproject ou resample (reproject_by_window() et resample_by_window())
@dce.print_time
def extract_by_window(img_path:str,
                     out_path:str,
                     bbox:tuple,
                     bbox_crs:str,
                     out_res:float=None,
                     out_crs:str=None,
                     resample:str='bilinear',
                     band:int=1,
                     overviews:bool=False)->pathlib.Path:
    """
    From modify_by_window(),
    Only does the extraction for native projection and resolution
    """
    # GDAL env vars discussed here https://trac.osgeo.org/gdal/wiki/ConfigOptions
    # with rasterio.Env(CHECK_WITH_INVERT_PROJ='NO'):
    dex = dce.DatacubeExtract()
    with rasterio.Env():
        #Extract part of dem by window
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
        
        # bbox_full in native image crs
        #used when creating outfile
        bbox_full = dce.tap_window(img.transform,bbox,bbox_crs)
        (minx_full,
         miny_full,
         maxx_full,
         maxy_full) = rasterio.windows.bounds(bbox_full,
                                              img.transform)#,
                                              #bbox_full.height,
                                              #bbox_full.width)

        # Get the study_area windows based on the image block_windows
        #The bbox tap is created inside the dce.bbox_windows()
        bbox_windows = dce.bbox_windows(img_path, bbox, bbox_crs)
        # Generate the out image profile
        bbox_full_transform = from_origin(minx_full,maxy_full,img.res[0],img.res[0])
        out_profile = in_profile
        out_profile.update({'crs':out_crs,
                            'height':bbox_full.height,
                            'width':bbox_full.width,
                            'transform':bbox_full_transform})
        
        #update profile to add if needed the bigtiff tag
        file_size = dex.calculate_file_size(out_profile['dtype'], out_profile['width'], out_profile['height'])
        if file_size >= 3:
            out_profile['BIGTIFF']='YES'
            print('Outfile is bigger than 4GB, creation of BIGTIFF...')
        else :
            # kwargs['BIGTIFF']='NO'
            print('Outfile is smaller than 4GB, creation of TIFF...')
        
        temp_file = f'{out_path}.temp'
        # Write out the study area
        with rasterio.open(temp_file,mode='w',**out_profile) as out_img:
            
            # Calculate relative offsets and create write window 
            for bbox_win in bbox_windows:
                rel_col_offset = bbox_win.col_off - bbox_full.col_off
                rel_row_offset = bbox_win.row_off - bbox_full.row_off
                write_win = rasterio.windows.Window(col_off=rel_col_offset,
                                                    row_off=rel_row_offset,
                                                    width=bbox_win.width,
                                                    height=bbox_win.height)

                # Read the array from the original image
                out_arr = img.read(band,window=bbox_win)
                # Write out the window to the new image
                out_img.write(out_arr,window=write_win,indexes=band)
                
            if overviews:
                print('Overviews added to the outfile...')
                out_img = dex.add_overviews(out_img, resample=resample)
                rscopy(out_img,out_path,copy_src_overviews=True,**out_profile)
            else:
                print('No overviews added to the outfile')
                rscopy(out_img,out_path,**out_profile)
        
        out_img.close()
        os.remove(temp_file)
            
        img.close()
    return



#Work in progress
def reproject_by_window(img_path:str,
                     out_path:str,
                     bbox:tuple,
                     bbox_crs:str,
                     out_res:float=None,
                     out_crs:int=None,
                     resample:str='bilinear',
                     band:int=1)->pathlib.Path:
    """Sub-function of the extract_by_window"""


    # GDAL env vars discussed here https://trac.osgeo.org/gdal/wiki/ConfigOptions
    # with rasterio.Env(CHECK_WITH_INVERT_PROJ='NO'):
    dex = dce.DatacubeExtract()
    with rasterio.Env():
        # """Resamples of reprojects by window"""
        img = rasterio.open(img_path)
        resample = dce.resample_enum(resample)
        in_profile = img.profile
        # Ensure the out_crs and out_res are properly set
       
        
            
        if not out_res:
            # No out_res provided, want native resolution
            out_res = img.res[0]
        #The bbox tap is created inside the dce.bbox_windows()
        # bbox_full in native image crs
        bbox_full = dce.tap_window(img.transform,bbox,bbox_crs)
        #TODO : Remove bbox_full.height and bbox_full.width, not necessary, gives the same output
        (minx_full,
         miny_full,
         maxx_full,
         maxy_full) = rasterio.windows.bounds(bbox_full,
                                              img.transform,
                                              bbox_full.height,
                                              bbox_full.width)

      
        # Get the study_area windows based on the image block_windows
        #gives us the bbox_window clipped to the bbox
        bbox_windows = dce.bbox_windows(img_path, bbox, bbox_crs)
        ##CHARLOTTE REMOVE AFTER USE
        # for bw in bbox_windows:
        #     bw_b = rasterio.windows.bounds(window=bw,transform=img.transform)
        #     # Write out bounds as geojson
        #     dict_poly = dex.poly_to_dict(sgbox(*bw_b))
        #     dex.dict_to_vector_file(dict_poly, bbox_crs, r'C:\Users\ccrevier\Documents\Datacube\MNEHR-cog\a-test\test_block_windows', f'bw-{bw.col_off}-{bw.row_off}.geojson')
        # ##
        
        if out_crs != img.crs:
            
            #LA PARTIE DE CODE QUI FONCTIONNE
            #devient le nouveau extent de référence
            # bbox_full = dce.tap_window(img.tranform,bbox,bbox_crs)
            xmin, ymin, xmax, ymax = img.window_bounds(bbox_full)
            dst_transform_full,dst_width_full,dst_height_full = tap_params(in_crs=img.crs,
                                                                            out_crs=out_crs,
                                                                            in_left=xmin,
                                                                            in_bottom=ymin,
                                                                            in_right=xmax,
                                                                            in_top=ymax,
                                                                            in_width=bbox_full.width,
                                                                            in_height=bbox_full.height,
                                                                            out_res=out_res)

            kwargs = img.profile
            kwargs.update({
                'crs': out_crs,
                'transform': dst_transform_full,
                'width': dst_width_full,
                'height': dst_height_full
            })
            dst_file = r'C:\Users\ccrevier\Documents\Datacube\git_files\dc_extract\extract\devtests\test-images\extract-5-1-bilinear-3979-reprojected-2960_5.tif'

            dst = rasterio.open(dst_file, 'w', **kwargs)
            for bbox_win in bbox_windows:
                sub_xmin, sub_ymin, sub_xmax, sub_ymax= img.window_bounds(bbox_win)
                dst_transform_sub,dst_width_sub,dst_height_sub = tap_params(in_crs=img.crs,
                                                                                out_crs=out_crs,
                                                                                in_left=sub_xmin,
                                                                                in_bottom=sub_ymin,
                                                                                in_right=sub_xmax,
                                                                                in_top=sub_ymax,
                                                                                in_width=bbox_win.width,
                                                                                in_height=bbox_win.height,
                                                                                out_res=out_res)
                
                
                rel_col_offset = bbox_win.col_off - bbox_full.col_off
                rel_row_offset = bbox_win.row_off - bbox_full.row_off
                
                in_transform_sub = rasterio.windows.transform(bbox_win, img.profile['transform'])
                
                # in_arr= img.read(window=bbox_win)
                import numpy as np
                out_arr = np.full((dst_width_sub, dst_height_sub), img.nodata)
                
                reproject(source=img.read(window=bbox_win),
                            destination=out_arr,
                            src_transform=in_transform_sub,
                            src_crs=img.crs,
                            dst_transform=dst_transform_sub,
                            dst_crs=out_crs,
                            resampling=resample, 
                            dst_resolution = out_res)
                
                write_win = rasterio.windows.Window(col_off=rel_col_offset,
                                                    row_off=rel_row_offset,
                                                    width=out_arr.shape[-1],height=out_arr.shape[-2])
                
                dst.write(out_arr,window=write_win, indexes=1)
            dst.close()
            
            #FIN DE LA PARTIE DE CODE QUI FONCTIONNE
            
            
            #Corresponds to all the windows intersecting with the bbox
            windows = dce.bbox_windows(img_path, bbox, bbox_crs, clip=False)
            
            #write windows to out temps file
            out_path_temp = out_path.parents[0].joinpath('temp.tif')
            
            #Get origin if the first block window 
            
            # Generate the out image profile
            bbox_full_transform = from_origin(minx_full,maxy_full,img.res[0],img.res[0])
            out_profile = in_profile
            out_profile.update({'crs':out_crs,
                                'height':bbox_full.height,
                                'width':bbox_full.width,
                                'transform':bbox_full_transform})
            #update profile to add if needed the bigtiff tag
            file_size = dex.calculate_file_size(out_profile['dtype'], out_profile['width'], out_profile['height'])
            if file_size >= 3:
                out_profile['BIGTIFF']='YES'
                print('Outfile is bigger than 4GB, creation of BIGTIFF...')
            else :
                # kwargs['BIGTIFF']='NO'
                print('Outfile is smaller than 4GB, creation of TIFF...')

        
            with rasterio.open(out_path_temp,mode='w',**out_profile) as out_img: 
                # Calculate relative offsets and create write window 
                for bbox_win in bbox_windows:
                    rel_col_offset = bbox_win.col_off - bbox_full.col_off
                    rel_row_offset = bbox_win.row_off - bbox_full.row_off
                    write_win = rasterio.windows.Window(col_off=rel_col_offset,
                                                        row_off=rel_row_offset,
                                                        width=bbox_win.width,
                                                        height=bbox_win.height)
    
                    # Read the array from the original image
                    out_arr = img.read(band,window=bbox_win)
                    # Write out the window to the new image
                    out_img.write(out_arr,window=write_win,indexes=band)
                    
            #JE SUIS RENDU ICI EN DATE DU 29 SEPTEMBRE. SUIVRE LES IDÉES DU SCHÉMA
            #TODO : define the profile for this subset
            #TODO: calculate relative position of the block window (like in the natif clip)
            #TODO : read-write temp file 
            
            #TODO: do the rest of the reproject
            
            
            # Reproject the windows before writing
            # Get the transformed bounds for the full study area
            (dst_transform,
              dst_width,
              dst_height) = tap_params(in_crs=img.crs,
                                      out_crs=out_crs,
                                      in_left=minx_full,
                                      in_bottom=miny_full,
                                      in_right=maxx_full,
                                      in_top=maxy_full,
                                      in_width=bbox_full.width,
                                      in_height=bbox_full.height,
                                      out_res=out_res)
            
            out_profile = in_profile
            out_profile.update({'crs':out_crs,
                                'height':dst_height, #if native resolution, same as src_heigth and width
                                'width':dst_width,
                                'transform':dst_transform})
            
            #reproject bbox_full into out_crs
            poly = box(minx_full, miny_full, maxx_full,maxy_full)
            #reproject sub-window geom into output crs
            dict_bbox_full_out = dex.transform_dict_to_dict(mapping(poly), img.crs,out_crs)
            bbox_full_out = dex.dict_to_poly(dict_bbox_full_out).bounds
            win_bbox_full_out = rasterio.windows.from_bounds(*bbox_full_out, dst_transform)
            
            with rasterio.open(out_path,mode='w',**out_profile) as out_img:             

                for win, bbox_win in zip(windows, bbox_windows):
                    # reproject
                    # Get the in window bounds, transform and array
                    
                    #get the arr corresponding to the block window 
                    in_arr = img.read(band,window=win)
                    
                    #Old way of calculating the in transform of window
                    # minx,miny,maxx,maxy = rasterio.windows.bounds(window=bbox_win,transform=img.transform)
                    # in_transform = from_origin(minx,maxy,img.res[0],img.res[0])
                    #New way to do the same thing 
                    win_transform = rasterio.windows.transform(win, img.transform)
                    bbox_win_transform = rasterio.windows.transform(bbox_win, img.transform)
                    
                    out_arr, out_transform = reproject(source=in_arr,
                                                        src_transform=win_transform,
                                                        src_crs=img.crs,
                                                        src_nodata=img.nodata,
                                                        dst_crs=out_crs,
                                                        dst_nodata=img.nodata,
                                                        dst_resolution=out_res,
                                                        resampling=resample,
                                                        num_threads=1,
                                                        init_dest_nodata=True,
                                                        warp_mem_limit=0)
                    
                    # #get sub window geom from rasterio.window
                    # bbox_win_bounds = rasterio.windows.bounds(window=bbox_win,transform=bbox_win_transform)
                    # poly = box(*bbox_win_bounds)
                    # #reproject sub-window geom into output crs
                    # out_bbox_poly = dex.transform_dict_to_dict(mapping(poly), img.crs,out_crs)
                    
                    # #tap the reprojected bbox poly
                    # #Pas certaine du transform que je dois mettre ici...
                    # out_bbox = dce.tap_window(out_transform,out_bbox_poly,out_crs)
                    
                    #reproject the win 
                    
                 #-------------
                    rel_col_offset = win.col_off - win_bbox_full_out.col_off
                    rel_row_offset = win.row_off - win_bbox_full_out.row_off
                    write_win = rasterio.windows.Window(col_off=rel_col_offset,
                                                        row_off=rel_row_offset,
                                                        width=win.width,
                                                        height=win.height)
    
                    # Read the array from the original image
                    out_arr = img.read(band,window=win)
                    # Write out the window to the new image
                    out_img.write(out_arr,window=write_win,indexes=band)
                    
                    # # Get the out_array window offsets
                    # out_win = rasterio.windows.Window(col_off=0,row_off=0,width=out_arr.shape[-1],height=out_arr.shape[-2])
                    # out_bounds = rasterio.windows.bounds(window=out_win,transform=out_transform)
                    # out_minx,out_miny,out_maxx,out_maxy = out_bounds
                    # write_win = rasterio.windows.from_bounds(*out_bounds,transform=dst_transform)
                  
                    
                    # # Write out the window to the new image
                    # out_img.write(out_arr,window=write_win)
                    del out_arr
                    del in_arr
                    del write_win
        else:
            # Generate the out image profile
            #TODO : Allow to take into account the bigtiff tag
            bbox_full_transform = from_origin(minx_full,maxy_full,img.res[0],img.res[0])
            out_profile = in_profile
            out_profile.update({'crs':out_crs,
                                'height':bbox_full.height,
                                'width':bbox_full.width,
                                'transform':bbox_full_transform})
            #update profile to add if needed the bigtiff tag
            file_size = dex.calculate_file_size(out_profile['dtype'], out_profile['width'], out_profile['height'])
            if file_size >= 3:
                out_profile['BIGTIFF']='YES'
                print('Outfile is bigger than 4GB, creation of BIGTIFF...')
            else :
                # kwargs['BIGTIFF']='NO'
                print('Outfile is smaller than 4GB, creation of TIFF...')

            # Write out the study area
            with rasterio.open(out_path,mode='w',**out_profile) as out_img:
                # TODO Add debug logging
        
                
                # Calculate relative offsets and create write window 
                for bbox_win in bbox_windows:
                    rel_col_offset = bbox_win.col_off - bbox_full.col_off
                    rel_row_offset = bbox_win.row_off - bbox_full.row_off
                    write_win = rasterio.windows.Window(col_off=rel_col_offset,
                                                        row_off=rel_row_offset,
                                                        width=bbox_win.width,
                                                        height=bbox_win.height)
    
                    # Read the array from the original image
                    out_arr = img.read(band,window=bbox_win)
                    # Write out the window to the new image
                    out_img.write(out_arr,window=write_win,indexes=band)
            
        img.close()
    
    return

def resample_by_window(img_path:str,
                     out_path:str,
                     bbox:tuple,
                     bbox_crs:str,
                     out_res:float=None,
                     out_crs:int=None,
                     resample:str='bilinear',
                     band:int=1)->pathlib.Path:
    """Sub-function of the extract_by_window"""
    
    # GDAL env vars discussed here https://trac.osgeo.org/gdal/wiki/ConfigOptions
    # with rasterio.Env(CHECK_WITH_INVERT_PROJ='NO'):
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
        bbox_full = dce.tap_window(img.transform,bbox,bbox_crs)
        #TODO : Remove bbox_full.height and bbox_full.width, not necessary, gives the same output
        (minx_full,
         miny_full,
         maxx_full,
         maxy_full) = rasterio.windows.bounds(bbox_full,
                                              img.transform)#,
                                              # bbox_full.height,
                                              # bbox_full.width)


        # Get the study_area windows based on the image block_windows
        bbox_windows = dce.bbox_windows(img_path, bbox, bbox_crs)
    
        
        if out_res != img.res[0]:
            
            (dst_transform,
              dst_width,
              dst_height) = tap_params(in_crs=img.crs,
                                      out_crs=out_crs,
                                      in_left=minx_full,
                                      in_bottom=miny_full,
                                      in_right=maxx_full,
                                      in_top=maxy_full,
                                      in_width=bbox_full.width,
                                      in_height=bbox_full.height,
                                      out_res=out_res)
           
            # Generate the out image profile
            out_profile = in_profile
            out_profile.update({'crs':out_crs,
                                'height':dst_height,
                                'width':dst_width,
                                'transform':dst_transform})

            #IS NOT WORKING
            with rasterio.open(out_path, 'w', **out_profile) as out_img:
                for bbox_win in bbox_windows:
                   rel_col_offset = bbox_win.col_off - bbox_full.col_off
                   rel_row_offset = bbox_win.row_off - bbox_full.row_off
                   
                   #To have the new width and height of the resampled window
                   (out_transform, 
                    out_width, 
                    out_height,
                    xmin, xmax, ymin, ymax) = aligned_target_mod(img.transform, bbox_win.width, bbox_win.height, out_res)
                   
                   write_win = rasterio.windows.from_bounds(left=xmin, bottom=ymin, right=xmax, top=ymax, transform=out_transform)
                   
                   out_arr = img.read(band, window=bbox_win, out_shape=(img.count, out_width, out_height))#, resampling=resample)
                   
                   out_img.write(out_arr, window = write_win, indexes=band)
                    
        else:
            # Generate the out image profile
            bbox_full_transform = from_origin(minx_full,maxy_full,img.res[0],img.res[0])
            out_profile = in_profile
            out_profile.update({'crs':out_crs,
                                'height':bbox_full.height,
                                'width':bbox_full.width,
                                'transform':bbox_full_transform})
            #update profile to add if needed the bigtiff tag
            file_size = dex.calculate_file_size(out_profile['dtype'], out_profile['width'], out_profile['height'])
            if file_size >= 3:
                out_profile['BIGTIFF']='YES'
                print('Outfile is bigger than 4GB, creation of BIGTIFF...')
            else :
                # kwargs['BIGTIFF']='NO'
                print('Outfile is smaller than 4GB, creation of TIFF...')

            # Write out the study area
            with rasterio.open(out_path,mode='w',**out_profile) as out_img:
                # TODO Add debug logging
                
                # Calculate relative offsets and create write window 
                for bbox_win in bbox_windows:
                    rel_col_offset = bbox_win.col_off - bbox_full.col_off
                    rel_row_offset = bbox_win.row_off - bbox_full.row_off
                    write_win = rasterio.windows.Window(col_off=rel_col_offset,
                                                        row_off=rel_row_offset,
                                                        width=bbox_win.width,
                                                        height=bbox_win.height)
    
                    # Read the array from the original image
                    out_arr = img.read(band,window=bbox_win)
                    # Write out the window to the new image
                    out_img.write(out_arr,window=write_win,indexes=band)
            
        img.close()
       
    return

def aligned_target_mod(transform, width, height, resolution):
    """Aligns target to specified resolution

    Parameters
    ----------
    transform : Affine
        Input affine transformation matrix
    width, height: int
        Input dimensions
    resolution: tuple (x resolution, y resolution) or float
        Target resolution, in units of target coordinate reference
        system.

    Returns
    -------
    transform: Affine
        Output affine transformation matrix
    width, height: int
        Output dimensions

    """
    if isinstance(resolution, (float, int)):
        res = (float(resolution), float(resolution))
    else:
        res = resolution

    xmin = transform.xoff
    ymin = transform.yoff + height * transform.e
    xmax = transform.xoff + width * transform.a
    ymax = transform.yoff

    xmin = floor(xmin / res[0]) * res[0]
    xmax = ceil(xmax / res[0]) * res[0]
    ymin = floor(ymin / res[1]) * res[1]
    ymax = ceil(ymax / res[1]) * res[1]
    dst_transform = Affine(res[0], 0, xmin, 0, -res[1], ymax)
    dst_width = max(int(ceil((xmax - xmin) / res[0])), 1)
    dst_height = max(int(ceil((ymax - ymin) / res[1])), 1)

    return dst_transform, dst_width, dst_height, xmin, xmax, ymin, ymax
##############################################################################################
#### NOT USED FUNCTIONS
def test_performance():
    """Test the performance of the methods :
        1. rasterio original read
        2. rioxarray 
        3. window with rasterio read
        For the moment, we know that the rasterio read-clip-write (1) is slower 
        than the rioxarray read-clip-write method, we want to test if the window is 
        faster than the rioxarray
        
        Test implemented in ordrer to create the faster way possible!!!!
   
    Launches modify_by_window in debug mode.
    Writes images amd geojson bboxes to test-images sub folder.
    Writes modify_by_window.log to logs subfolder.

    """
    #creation of a log file to write performance output
    f = pathlib.Path(__file__).parent.joinpath('logs')
    if not f.is_dir():
        f.mkdir(parents=True,exist_ok=True)
    f = f.joinpath('performance.log')
    if not f.is_file():
        fp = f.open(mode='w')
    else:
        fp = f.open(mode='a')
      
    sizes = [5,50,100]
    res = 1 #toujours natif pour le moment (dem fine = 1meter res)
    resample = 'bilinear'
    epsg = 3979 #toujours natif pour le moment
    dex = dce.DatacubeExtract()
    #1. lancer la commande pour extract_by_window
    fp.write('\nStarting extract by window\n')
    for size in sizes:
        start = datetime.datetime.now()
        fp.write(f'\nStart: {start}')
        bbox,bbox_crs = test_bbox_dynamic(x_km=size, y_km=size)
        method = 'extract_by_window'
        out_name = f'extract-{size}-{res}-{resample}-{epsg}-{method}.tif'
        f = pathlib.Path(__file__).parent.joinpath('test-images')
        if not f.is_dir():
            f.mkdir(parents=True,exist_ok=True)
        f = f.joinpath(out_name)
        
        params = {
            'img_path':test_cog('fine'),
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
        modify_by_window(**params)
        fp.write(f'\nOut file : {out_name}')
        #time to run
        end = datetime.datetime.now()
        fp.write(f'\nEnd: {datetime.datetime.now()}')
        total = (end-start).total_seconds()
        fp.write(f'\nTotal time {total} seconds\n')
    fp.write('\nextract by window is DONE.')
    fp.write('\n------------------------------')
    
    #2. Lancer la commande pour extract with rioxarray (dex.clip_to_grid())
    fp.write('\nStarting clip_to_grid() with rioxarray\n')
    for size in sizes:
        start = datetime.datetime.now()
        fp.write(f'\nStart: {start}')
        bbox,bbox_crs = test_bbox_dynamic(x_km=size, y_km=size)
        method = 'rioxarray'
        out_name = f'extract-{size}-{res}-{resample}-{epsg}-{method}.tif'
        f = pathlib.Path(__file__).parent.joinpath('test-images')
        if not f.is_dir():
            f.mkdir(parents=True,exist_ok=True)
        f = f.joinpath(out_name)
        params = {
            'clip_file':test_cog('fine'),
            'out_file':f,
            'bbox':bbox,
            'bbox_crs':bbox_crs,
            'resolution':res,
            'national':False,
            'overviews':False, 
            'resample':resample}
        fp.write(f'\nParameters : bbox size {size}km, resolution {res}m, resample {resample}, epsg {epsg}, extract method: {method}')
        native_clip = dex.clip_to_grid(**params)
        fp.write(f'\nOut file : {out_name}')
        #time to run
        end = datetime.datetime.now()
        fp.write(f'\nEnd: {datetime.datetime.now()}')
        total = (end-start).total_seconds()
        fp.write(f'\nTotal time {total} seconds\n')
    fp.write('\nextract with rioxarray is DONE.')
    fp.write('\n-------------END OF RUN-----------------')
    
    #3. lancer la commande pour extract avec rasterio (dex.clip_to_grid(), seulement si on remarque que le window reading est très lent)
    fp.write('\nStarting clip_to_grid() with rasterio')
    for size in sizes:
        start = datetime.datetime.now()
        fp.write(f'\nStart: {start}')
        bbox,bbox_crs = test_bbox_dynamic(x_km=size, y_km=size)
        method = 'rasterio'
        out_name = f'extract-{size}-{res}-{resample}-{epsg}-{method}.tif'
        f = pathlib.Path(__file__).parent.joinpath('test-images')
        if not f.is_dir():
            f.mkdir(parents=True,exist_ok=True)
        f = f.joinpath(out_name)
        params = {
            'clip_file':test_cog('fine'),
            'out_file':f,
            'bbox':bbox,
            'bbox_crs':bbox_crs,
            'resolution':res,
            'national':False,
            'overviews':False,
            'resample':resample,
            'xarray':False}
        fp.write(f'\nParameters : bbox size {size}km, resolution {res}m, resample {resample}, epsg {epsg}, extract method: {method}')
        native_clip = dex.clip_to_grid(**params)
        #time to run
        end = datetime.datetime.now()
        fp.write(f'\nEnd: {datetime.datetime.now()}')
        total = (end-start).total_seconds()
        fp.write(f'\nTotal time {total} seconds\n')
    fp.write('\nExtract with rasterio is DONE')
    
    fp.close()
    return

def geojson_img_block_windows(img_path:str,band:int=1,json_out_dir:str=None):
    """Generate geojson files for all block window bounds for an image"""
    dex = dce.DatacubeExtract()
    if not json_out_dir:
        # Set the out dir to a vectors sub directory
        json_out_dir = pathlib.Path(__file__).parent.joinpath('vectors/full_image')
    
    img = rasterio.open(img_path)
    name = img.name.split('/')[-1]
    full_dict_poly = dex.poly_to_dict(sgbox(*img.bounds))
    dex.dict_to_vector_file(full_dict_poly, img.crs, json_out_dir, f'{name}-fullbounds.geojson')
    
    for b_id, bw in img.block_windows(band):
        bw_b = rasterio.windows.bounds(window=bw,transform=img.transform)
        dict_poly = dex.poly_to_dict(sgbox(*bw_b))
        dex.dict_to_vector_file(dict_poly, img.crs, json_out_dir, f'{name}-{bw.col_off}-{bw.row_off}.geojson')
    return


def window_info(bbox:tuple,transform:rasterio.transform):
    win = rasterio.windows.from_bounds(*bbox,transform=transform)
    win_bounds = rasterio.windows.bound(window=win,transform=transform)
    return win,win_bounds


def validate_tap(add = 1):
    """Validate taht adding 1 to the tap window width and height 
    is enough to always make the tap bbox larger than the input bbox"""
    og_x_ori = 2150469.0
    og_y_ori = 144975.0
    
    c = test_cog()
    img = rasterio.open(c)
    list_bbox=[]
    for number in range(10):
        x_ori = og_x_ori + (number/10)
        for number in range(10):
            y_ori = og_y_ori +(number/10)
            
            bbox = x_ori, y_ori, x_ori+5000, y_ori+5000 #creation of a 5x5km bbox
            list_bbox.append(bbox)
    list_bbox_tap = []
    for b in list_bbox:
        #All tap bbox should be the same
        w_orig = rasterio.windows.from_bounds(*b,transform=img.transform)
        w_tapish = w_orig.round_lengths('ceil')
        w_tapish = w_tapish.round_offsets('floor')
        # b_tapish = rasterio.windows.bounds(window=w_tapish,transform=img.transform)
        w_tap = rasterio.windows.Window(w_tapish.col_off,w_tapish.row_off,w_tapish.width+add,w_tapish.height+add)
        b_tap = rasterio.windows.bounds(window=w_tap,transform=img.transform)
        list_bbox_tap.append(b_tap)
    
    #get unique value of the tap bbox, because we hypothesis that the tap bbox is the same 
    tap_bbox = list_bbox_tap[0]
    dict_validate={}
    for bbox in list_bbox:
        list_validate = []
        idx = list_bbox.index(bbox)
        minx_tap, miny_tap, maxx_tap, maxy_tap = tap_bbox
        minx, miny, maxx, maxy = bbox
        #condition 1
        if minx >= minx_tap:
            pass
        else:
            list_validate.append('c1')
        #condition 2
        if maxx <= maxx_tap:
            pass
        else:
            list_validate.append('c2')
        #condition 3
        if miny >= miny_tap:
            pass
        else:
            list_validate.append('c3')
        #condition 4    
        if maxy <= maxy_tap:
            pass
        else:
            list_validate.append('c4')
        
        if not list_validate:
            pass
        else:
            dict_validate[idx] = list_validate
        
        
    return dict_validate

def test_basics():
    """
    From : https://rasterio.readthedocs.io/en/latest/api/rasterio._base.html?highlight=block_shapes#id0
    Blocks:

            0       256     512
          0 +--------+--------+
            |        |        |
            | (0, 0) | (0, 1) |
            |        |        |
        256 +--------+--------+
            |        |        |
            | (1, 0) | (1, 1) |
            |        |        |
        512 +--------+--------+


    Windows:
        definition : ((rows),(cols))
        based on a block size (internal tile size) of 256
        UL: ((0, 256), (0, 256))
        UR: ((0, 256), (256, 512))
        LL: ((256, 512), (0, 256))
        LR: ((256, 512), (256, 512))
    ----------------------------------------------------------------
    Window for block_id (0,0) would be represented by Window(col_off=0, row_off=0, width=256, height=256)
    Window for block_id (0,1) would be represented by Window(col_off=256, row_off=0, width=256, height=256)
    Window for block_id (1,0) would be represented by Window(col_off=0, row_off=256, width=256, height=256)
    Window for block_id (1,1) would be represented by Window(col_off=256, row_off=256, width=256, height=256)

    -----------------------------------------------------------------

    Test examples
    import rasterio
    import dc_extract.extract.devtests.test_block_windows as tb
    c = tb.test_cog()
    img = rasterio.open(c)
    
    # Get the full NW and SE spatial coordinates for the image
    # NW corner (0,0) spatial coordinates
    img.xy(0,0)
    Out[10]: (-2431352.0, 3876408.0)
    
    # SE corner
    img.xy(img.width,img.height)
    Out[82]: (2202904.0, -1611560.0)
    
    
    b,bcrs = tb.test_bbox()
    b
    Out[8]: 
    (2150469.4724999964,
      144975.05299999937,
      2155469.4724999964,
      149975.05299999937)
    
    # Original window for the read based on original bounds
    w_orig = rasterio.windows.from_bounds(*b,transform=img.transform)
    
    # Non-integer offsets and heights(rows) and widths(columns) => resampling
    w_orig
    Out[9]: Window(col_off=286364.3420312498,
                    row_off=232902.55918750004,
                    width=312.5,
                    height=312.5) 
    
    # Testing Windows functionality
    w_orig.round_lengths(op='floor')
    Out[13]: Window(col_off=286364.3420312498, row_off=232902.55918750004, width=312, height=312)
    
    w_orig.round_lengths(op='ceil')
    Out[14]: Window(col_off=286364.3420312498, row_off=232902.55918750004, width=313, height=313)
    
    w_orig.round_offsets(op='floor')
    Out[15]: Window(col_off=286364, row_off=232902, width=312.5, height=312.5)
    
    w_orig.round_offsets(op='ceil')
    Out[16]: Window(col_off=286365, row_off=232903, width=312.5, height=312.5)
    
    # We want the lengths to be 'ceil' and the offsets to be 'floor' => TAP
    w_tapish = w_orig.round_lengths('ceil')
    w_tapish = w_tapish.round_offsets('floor')
    w_tapish
    Out[22]: Window(col_off=286364, row_off=232902, width=313, height=313)
    
    # The tap window spatial bounds using tap window and image transform
    b_tapish = rasterio.windows.bounds(window=w_tap,transform=img.transform)
    b_tapish
    Out[28]: (2150464.0, 144976.0, 2155472.0, 149984.0) # min's ok, max's too small
    
    # Add two cells to height and width to cover extra distance (one would probably suffice)
    w_tap = rasterio.windows.Window(w_tapish.col_off,w_tapish.row_off,w_tapish.width+2,w_tapish.height+2)
    w_tap
    Out[88]: Window(col_off=286364, row_off=232902, width=315, height=315)
    b_tap = rasterio.windows.bounds(window=w_tap,transform=img.transform)
    
    b_tap
    Out[90]: (2150464.0, 144944.0, 2155504.0, 149984.0)
    
    # Image block windows / internal tileing
    The actual size (rows,cols) of internal tileing
    img.block_shapes
    Out[43]: [(512, 512)]
    
    # For each block window in the image, does it include the 'tap window'
    img.block_window(band,0,0)
    Out[48]: Window(col_off=0, row_off=0, width=512, height=512)
    
    img.block_window(band,1,0)
    Out[46]: Window(col_off=0, row_off=512, width=512, height=512)
    
    img.block_window(band,0,1)
    Out[47]: Window(col_off=512, row_off=0, width=512, height=512)
    
    w_tap
    Out[49]: Window(col_off=286364, row_off=232902, width=315, height=315)
    
    # Calculate the block window start indices
    pix_per_block = img.block_shapes[0][0]
    pix_per_block
    Out[71]: 512
    
    block_col_index_start = math.floor(w_tap.col_off/pix_per_block)
    block_col_index_start
    Out[68]: 559
    
    block_row_index_start = math.floor(w_tap.row_off/pix_per_block)
    block_row_index_start
    Out[69]: 454
    
    num_block_cols = math.ceil(w_tap.width/pix_per_block)
    num_block_rows = math.ceil(w_tap.height/pix_per_block)
    
    num_block_cols
    Out[104]: 1
    num_block_rows
    Out[106]: 1
    
    # Use range and pix_per_block to calculate all windows required
    extract_windows = []
    for row in range(block_row_index_start,block_row_index_start+num_block_rows):
        for col in range(block_col_index_start,block_col_index_start+num_block_cols):
            extract_window = img.block_window(band,row,col)
            extract_windows.append(extract_window)
    
    extract_windows
    Out[111]: [Window(col_off=286208, row_off=232448, width=512, height=512)]
    
    # Multi-threading will be done on
    for win in extract_windows:
        arr = img.read(band,window=win)
    """
    return

def test_window_results():
    """
    30 bilinear
    {'img_path': 'http://datacube-prod-data-public.s3.ca-central-1.amazonaws.com/store/elevation/cdem-cdsm/cdem/cdem-canada-dem.tif', 'out_path': WindowsPath('S:/dc_extract/extract/devtests/test-images/resample-30-bilinear.tif'), 'bbox': (2150469.4724999964, 144975.05299999937, 2155469.4724999964, 149975.05299999937), 'bbox_crs': 'EPSG:3979', 'out_res': 30, 'out_crs': None, 'resample': 'bilinear', 'debug': True}
    Destination trans | 30.00, 0.00, 2150460.00|
    | 0.00,-30.00, 150000.00|
    | 0.00, 0.00, 1.00|,width 169,height 169
    Out profile: {'driver': 'GTiff', 'dtype': 'int16', 'nodata': -32767.0, 'width': 169, 'height': 169, 'count': 1, 'crs': CRS.from_epsg(3979), 'transform': Affine(30.0, 0.0, 2150460.0,
            0.0, -30.0, 150000.0), 'blockxsize': 512, 'blockysize': 512, 'tiled': True, 'compress': 'lzw', 'interleave': 'band'}
    bbox_win: Window(col_off=286364, row_off=232902, width=314, height=58)
    in_arr shape (58, 314)
    in_arr [[14 14 14 ... 25 24 24]
      [13 13 14 ... 24 24 24]
      [13 13 13 ... 24 24 24]
      ...
      [13 13 13 ...  6  8  8]
      [13 12 12 ...  5  7  8]
      [12 12 12 ...  5  7  8]]
    out transform | 30.00, 0.00, 2150464.00|
    | 0.00,-30.00, 150912.00|
    | 0.00, 0.00, 1.00|
    out_arr shape (1, 31, 168)
    out__arr [[[    12     12     12 ...      7      6 -32767]
      [    13     13     12 ...      6      7 -32767]
      [    13     13     12 ...      6      5 -32767]
      ...
      [    12     12     12 ...     23     23 -32767]
      [    13     13     13 ...     24     24 -32767]
      [    14     14     14 ...     24     24 -32767]]]
    Matching CRS
    interim_win Window(col_off=0.13333333333139308, row_off=0.5333333333337578, width=167.46666666667443, height=30.933333333333394)
    write_win : Window(col_off=0.13333333333139308, row_off=0.5333333333337578, width=168, height=31)
    bbox_win: Window(col_off=286364, row_off=232960, width=314, height=256)
    in_arr shape (256, 314)
    in_arr [[ 12  12  12 ...   5   7   7]
      [ 12  12  12 ...   5   6   7]
      [ 12  12  12 ...   5   5   7]
      ...
      [108 108 109 ...  90  89  89]
      [108 108 109 ...  90  90  89]
      [108 109 110 ...  90  90  89]]
    out transform | 30.00, 0.00, 2150464.00|
    | 0.00,-30.00, 153152.00|
    | 0.00, 0.00, 1.00|
    out_arr shape (1, 137, 168)
    out__arr [[[   108    110    112 ...     90     90 -32767]
      [   108    110    111 ...     90     90 -32767]
      [   108    109    111 ...     90     89 -32767]
      ...
      [    12     12     12 ...      9      6 -32767]
      [    12     12     12 ...      8      6 -32767]
      [    12     12     12 ...      7      6 -32767]]]
    Matching CRS
    interim_win Window(col_off=0.13333333333139308, row_off=31.46666666666715, width=167.46666666667443, height=136.53333333333285)
    write_win : Window(col_off=0.13333333333139308, row_off=31.46666666666715, width=168, height=137)
    ---------------------------------------------------
    30 bilinear
    {'img_path': 'http://datacube-prod-data-public.s3.ca-central-1.amazonaws.com/store/elevation/cdem-cdsm/cdem/cdem-canada-dem.tif', 'out_path': WindowsPath('S:/dc_extract/extract/devtests/test-images/resample-30-bilinear.tif'), 'bbox': (2150469.4724999964, 144975.05299999937, 2155469.4724999964, 149975.05299999937), 'bbox_crs': 'EPSG:3979', 'out_res': 30, 'out_crs': None, 'resample': 'bilinear', 'debug': True}
    Full window for study area Window(col_off=286364, row_off=232902, width=314, height=314)
    Destination trans | 30.00, 0.00, 2150460.00|
    | 0.00,-30.00, 150000.00|
    | 0.00, 0.00, 1.00|,width 169,height 169
    Out profile: {'driver': 'GTiff', 'dtype': 'int16', 'nodata': -32767.0, 'width': 169, 'height': 169, 'count': 1, 'crs': CRS.from_epsg(3979), 'transform': Affine(30.0, 0.0, 2150460.0,
            0.0, -30.0, 150000.0), 'blockxsize': 512, 'blockysize': 512, 'tiled': True, 'compress': 'lzw', 'interleave': 'band'}
    bbox_win: Window(col_off=286364, row_off=232902, width=314, height=58)
    in transform | 16.00, 0.00, 2150464.00|
    | 0.00, 16.00, 149984.00|
    | 0.00, 0.00, 1.00|
    in_arr shape (58, 314)
    in_arr [[14 14 14 ... 25 24 24]
      [13 13 14 ... 24 24 24]
      [13 13 13 ... 24 24 24]
      ...
      [13 13 13 ...  6  8  8]
      [13 12 12 ...  5  7  8]
      [12 12 12 ...  5  7  8]]
    out transform | 30.00, 0.00, 2150464.00|
    | 0.00,-30.00, 150912.00|
    | 0.00, 0.00, 1.00|
    out_arr shape (1, 31, 168)
    out__arr [[[    12     12     12 ...      7      6 -32767]
      [    13     13     12 ...      6      7 -32767]
      [    13     13     12 ...      6      5 -32767]
      ...
      [    12     12     12 ...     23     23 -32767]
      [    13     13     13 ...     24     24 -32767]
      [    14     14     14 ...     24     24 -32767]]]
    out_win Window(col_off=0, row_off=0, width=168, height=31)
    out_bounds (2150464.0, 149982.0, 2155504.0, 150912.0)
    write_win : Window(col_off=0.13333333333139308, row_off=-30.399999999999636, width=168.0, height=31.0)
    Traceback (most recent call last):
        CPLE_IllegalArgError: S:/dc_extract/extract/devtests/test-images/resample-30-bilinear.tif: 
            Access window out of range in RasterIO().  
            Requested (0,-30) of size 168x31 on raster of 169x169.
    """
    return