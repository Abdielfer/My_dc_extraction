# -*- coding: utf-8 -*-
"""
Created on Thu Sep 15 14:54:55 2022

@author: ccrevier

#Codes that test the reading and writting with window corresponding to input tbox 

#Instanciation du extract
debug=False
import extract as dce
import rasterio
dex = dce.DatacubeExtract(debug=debug)

#Definition du tbox
tbox=(2139524.2365999967, 101780,2239524.2365999967,152600.95809999853)

#Selection d'un fichier sur le bucket
collections='hrdem'
tbox_crs='EPSG:3979'
urls = dex.asset_urls(collections,tbox,tbox_crs)
url=urls[0]
clip_file = pathlib.Path(url)


########Test avec multiprocess
cog=rasterio.open(clip_file,GEOREF_SOURCES='INTERNAL')


pixel_size = cog.transform[0]
resolution=32
window_resolution = dex.clip_window_resolution(pixel_size,resolution)
datacube_aligned=True
national=False

#Definition de la fenetre de lecture en fonction de la boite fournis en input
w_orig = rasterio.windows.from_bounds(*tbox,transform=cog.transform)

w_orig
Out[13]: Window(col_off=229298.2365999967, row_off=285473.04190000147, width=100000.0, height=50820.958099998534)

w_tapish = w_orig.round_lengths('ceil')
w_tapish = w_tapish.round_offsets('floor')
w_tapish
Out[22]: Window(col_off=286364, row_off=232902, width=313, height=313)

# Add two cells to height and width to cover extra distance (one would probably suffice)
w_tap = rasterio.windows.Window(w_tapish.col_off,w_tapish.row_off,w_tapish.width+2,w_tapish.height+2)
w_tap
#######THIS IS WERE I AM NOW########### 15 septembre
#dex.dict_to_vector_file(std_geom,
#						  crs=tbox_crs,
#						  outpath=out_dir,
#						  filename='std_geom.geojson',
#						  driver='GeoJSON')
									
split_geom = dex.split_polygon(dex.dict_to_poly(std_geom), 100, 50) # Pour le gros tbox
list_shape = dex.multipolygon_to_list(split_geom)
list_dict_geom = [dex.poly_to_dict(poly) for poly in list_shape]
#dex.dict_to_vector_file(split_geom,
#						  crs=tbox_crs,
#						  outpath=out_dir,
#						  filename='split_std_geom.geojson',
#						  driver='GeoJSON')

xmin, ymin, xmax, ymax = shape(std_geom).bounds
out_shape = (int((ymax - ymin)/pixel_size), int((xmax - xmin)/pixel_size))
height=out_shape[0]
width=out_shape[1]

transform = rasterio.Affine.from_gdal(xmin, pixel_size, 0, ymax, 0, - pixel_size)
blocksize=512
cog.close()
num_workers=4
---différence avec celui d'avant commence ici---
with rasterio.open(clip_file) as cog:
# TODO use the existing kwargs modification
	kwargs = dex.modify_kwargs(kwargs=cog.meta.copy(),
								dst_crs=cog.crs,
								dst_transform=transform,
								dst_w=width,
								dst_h=height,
								blocksize=blocksize)

	with rasterio.open(temp_file, 'w+',**kwargs) as dst:
	# dst=rasterio.open(temp_file,'w+',**kwargs)
		read_lock = threading.Lock()
		write_lock = threading.Lock()
		
		def process(geom):
		   with read_lock:
			   sample_cog,cog_transform = dex.mask_cog(dataset=cog, shapes=[geom], crop=True)
			   # src_array = src.read(window=window)
		   sub_window = rasterio.features.geometry_window(dataset=dst, shapes=[geom], pad_x=0, pad_y=0)
		   # The computation can be performed concurrently
		   # result = compute(src_array)

		   with write_lock:
			   dst.write(sample_cog, window=sub_window)
		
		# for poly in list_dict_geom:
		#     sample_cog,cog_transform = dex.mask_cog(dataset=cog, shapes=[poly], crop=True) 
		#     sub_window = rasterio.features.geometry_window(dataset=dst, shapes=[poly], pad_x=0, pad_y=0)
		#     dst.write(sample_cog, window=sub_window)
		
		
		
		with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
			executor.map(process, list_dict_geom)
		
		dst = dex.add_overviews(dst)
		rscopy(dst,file_name,copy_src_overviews=True,**kwargs)
"""
import pathlib
import rasterio
import concurrent.futures
import multiprocessing
import threading
from rasterio.transform import Affine, from_origin
from rasterio.shutil import copy as rscopy
import os

import ccmeo_datacube.extract.extract as dce

"""
# These are the resources of the node we're runnning on
import psutil
print('physical cores:', psutil.cpu_count(logical=False))
print('vCPU:', psutil.cpu_count())
"""

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

@dce.print_time
def multithread_extract_by_window(img_path:str,
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
        bbox_full = dce.tap_window(img,bbox,bbox_crs)
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
           
            read_lock = threading.Lock()
            write_lock = threading.Lock()
            
            def process(bbox_win):
                with read_lock:
                    # Read the array from the original image
                    out_arr = img.read(band,window=bbox_win)
                
                write_win = compute(bbox_win, bbox_full)
                
                with write_lock:
                    out_img.write(out_arr,window=write_win,indexes=band)
            
            # We map the process() function over the list of
            # windows.
            with concurrent.futures.ThreadPoolExecutor(
                # max_workers=4 #optional, if not specified, will take total CPU+4
            ) as executor:
                executor.map(process, bbox_windows)
            
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

def compute(bbox_win, bbox_full):
    """reverses bands inefficiently

    Given input and output uint8 arrays, fakes an CPU-intensive
    computation.
    """
    rel_col_offset = bbox_win.col_off - bbox_full.col_off
    rel_row_offset = bbox_win.row_off - bbox_full.row_off
    write_win = rasterio.windows.Window(col_off=rel_col_offset,
                                        row_off=rel_row_offset,
                                        width=bbox_win.width,
                                        height=bbox_win.height)
    return write_win

#############
#Autre test
@dce.print_time
def multithread2_extract_by_window(img_path:str,
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
        bbox_full = dce.tap_window(img,bbox,bbox_crs)
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
        #Ne fonctionne pas... et restart le kernel chaque fois
        with rasterio.open(temp_file,mode='w',**out_profile) as out_img:
            from itertools import repeat
            args = bbox_windows, repeat(bbox_full), repeat(img), repeat(out_img)
            # create a thread pool with the default number of worker threads
            executor = concurrent.futures.ThreadPoolExecutor()
            
            for result in executor.map(read_write, *args):
                    print(result) 
                
        # with rasterio.open(temp_file,mode='w',**out_profile) as out_img:
           
        #     read_lock = threading.Lock()
        #     write_lock = threading.Lock()
            
        #     def process(bbox_win):
        #         with read_lock:
        #             # Read the array from the original image
        #             out_arr = img.read(band,window=bbox_win)
                
        #         write_win = compute(bbox_win, bbox_full)
                
        #         with write_lock:
        #             out_img.write(out_arr,window=write_win,indexes=band)
            
        #     # We map the process() function over the list of
        #     # windows.
        #     with concurrent.futures.ThreadPoolExecutor( #optional, if not specified, will take total CPU+4
        #     ) as executor:
        #         executor.map(process, bbox_windows)
            
        #     if overviews:
        #         print('Overviews added to the outfile...')
        #         out_img = dex.add_overviews(out_img, resample=resample)
        #         rscopy(out_img,out_path,copy_src_overviews=True,**out_profile)
        #     else:
        #         print('No overviews added to the outfile')
        #         rscopy(out_img,out_path,**out_profile)
      
        
        # out_img.close()
        # os.remove(temp_file)
            
        img.close()
    return

def read_write(bbox_win, bbox_full, img, out_img):
    rel_col_offset = bbox_win.col_off - bbox_full.col_off
    rel_row_offset = bbox_win.row_off - bbox_full.row_off
    write_win = rasterio.windows.Window(col_off=rel_col_offset,
                                        row_off=rel_row_offset,
                                        width=bbox_win.width,
                                        height=bbox_win.height)

    # Read the array from the original image
    out_arr = img.read(1,window=bbox_win)
    # Write out the window to the new image
    out_img.write(out_arr,window=write_win,indexes=1)
    
    
    return


######################## 
#Test with rioxarray

