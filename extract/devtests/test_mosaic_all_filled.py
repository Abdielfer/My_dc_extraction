# -*- coding: utf-8 -*-
"""
Created on Thu Mar  9 13:50:08 2023

@author: ccrevier

This performance test if it is sgnificantly longer to read_mask of mosaic before adding new file 
to validate if the mosaic is already filled up. 
function warped_mosaic() is the one being used rn 
function warped_mosaic_read_mask() is the one that reads the nodata mask before adding new input
Idea : When a file start to be discarded, we start to check if the mosaic is full or not.

The problem with checking everytime if the mosaic is full or not is that sometimes the mosaic will
never be completely full due to nodata from lake or other reason, so the mask will be read at each
incrementation and potentially make the process heavier BUT rn, the process continue even if the 
mosaic is already filled, files won't just not be added. 
We have to the balance we want.
THis test will help to know if the processus is sinificantly longer when checking
no data mask to try and fix this issue. 

Those mosaic functions are called inside the extract.monitoring.cog_mosaic.mosaic_performance.py
to creation a csv of the result for both mosaic functions
"""

import rasterio
import rioxarray
import numpy
import os
from rasterio.shutil import copy as rscopy

import ccmeo_datacube.extract.extract as dce

# def test_performance():
#     """
#     Call both mosaic function and print time to see the difference

#     Returns
#     -------
#     None.

#     """
#     dex = dce.DatacubeExtract()
#     out_crs='EPSG:3979'
#     resolution=4
#     method=None
    
    
#     collections = 'hrdem-lidar'
    
#     urls = dex.asset_urls(collections,bbox,bbox_crs)
    
#     files = [i for i in urls if 'dsm' in i]
    
#     list_params=[]
#     list_profile=[]
#     for url in urls:
#         dict_file = {}
#         profile, params = dce.get_extract_params(in_path=url,
#         										 out_crs=out_crs,
#         										 out_res=resolution,
#         										 bbox=tbox,
#         										 bbox_crs=tbox_crs,
#         										 resampling_method=method, 
#                                                  verbose=False)
#         dict_file['file']=url
#         # dict_file['profile']=profile
#         dict_file['params']=params
#         list_params.append(dict_file)
#         list_profile.append(profile)
        
#     out_file = '/gpfs/fs5/nrcan/nrcan_geobase/work/transfer/work/datacube/DEV_CC/dev_dc_extract/mosaic/performance/nodata_mask'
#     params = {'list_of_params':list_of_params,
#               'out_path':out_file,
#               'out_profile':out_profile}
    
#     print('Execution')
#     result = dce.warped_mosaic(**params)
#     return

@dce.print_time
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
    temp_file = f'{out_path}.temp'
    #Print if the file is bigtiff or not, optional
    try : 
        if out_profile['BIGTIFF']=='YES':
            print('INFO : Out mosaic is bigger than 4GB, creation of BIGTIFF...')
        else:
            print('INFO : Out mosaic is smaller than 4GB, creation of TIFF...')
    except:
        print('INFO : Out mosaic is smaller than 4GB, creation of TIFF...')
        
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
                    if numpy.all((window_arr == xar.rio.nodata)):
                        print('All no data')

                    else:
                        
                        # Read values already written to current window
                        # In out_crs spatial coords
                        existing_arr = out_img.read(band,window=dst_window)
                        
                        print(f'Update values in mosaic with value from : {file}')
                        # Make an existing window no_data mask
                        no_data_mask = (existing_arr == out_img.nodata)
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


@dce.print_time
def warped_mosaic_read_mask(list_of_params, 
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
    temp_file = f'{out_path}.temp'
    #Print if the file is bigtiff or not, optional
    try : 
        if out_profile['BIGTIFF']=='YES':
            print('INFO : Out mosaic is bigger than 4GB, creation of BIGTIFF...')
        else:
            print('INFO : Out mosaic is smaller than 4GB, creation of TIFF...')
    except:
        print('INFO : Out mosaic is smaller than 4GB, creation of TIFF...')
        
    with env:
        with rasterio.open(temp_file, mode="w+", **out_profile) as out_img:
            used_file = []
            unused_file = []
            #TODO: add color palettes if exist
            #TODO: add datetime (question is which datetime? oldest of files or date of creation?)
            for params in list_of_params:
                if not out_img.read_masks().all():
                    file = params['file']
                    extract_params = params['params']
                    with rasterio.open(file) as src:
                        with rasterio.vrt.WarpedVRT(src, **extract_params) as vrt:
                            with rioxarray.open_rasterio(vrt, lock=False, chunks=True) as xar:
    
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
                        if numpy.all((window_arr == xar.rio.nodata)):
                            print('All no data')
                            unused_file.append(file)
    
                        else:
                            
                            # Read values already written to current window
                            # In out_crs spatial coords
                            existing_arr = out_img.read(band,window=dst_window)
                            
                            print(f'Update values in mosaic with value from : {file}')
                            # Make an existing window no_data mask
                            no_data_mask = (existing_arr == out_img.nodata)
                            # LOG.debug(f'no_data_mask shape {no_data_mask.shape}')
                            new_data = existing_arr
                            new_data[no_data_mask] = window_arr[no_data_mask]
                            out_img.write(new_data,indexes=band,window=dst_window)
                            
                            #Populate the list of file used inside the mosaic
                            used_file.append(file)
                else:
                    print(''.rjust(100, '-'))
                    print('INFO : All nodata values in mosaic are filled')
                    #TODO: find a way to append remaning input file to the unused list
                    # unused_file.append()
                    break    

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