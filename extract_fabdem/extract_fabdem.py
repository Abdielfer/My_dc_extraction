#!/usr/bin/python3
"""
Tools for working with HPC based fabdem files
Created on Tue Feb  8 20:32:10 2022

@author: nob000

Example Usage:
    CLI or Job file:
        python <path_to_file>/extract_fabdem.py T-39.geojson
    Spyder IPython Console:
    runfile('<path_to_file>/extract_fabdem.py', args='AF-38.geojson',wdir='<working_dir>')

"""
# python standard library
import json
import math
import os
import pathlib
import sys


# python cusomt modules
import pandas
import geopandas
import shapely
import rasterio
from rasterio.transform import Affine
import numpy

# datacube custom modules
# Ensure syspath first  reference is to .../dc_extract... parent of all local files
# for this file it is  .../dc_extract/*/<modules> so need parents[1]
# define number of subdirs from 'root' (usecase)
_child_level = 1
dir_needed = str(pathlib.Path(__file__).parents[_child_level].absolute())
sys.path.append(dir_needed)
import ccmeo_datacube.extract as dce

def fabdem3979_canada_mosaic(mosaic_by_window=True,
                             resample='bilinear',
                             blocksize=1028,
                             compression='LZW',
                             pixel_size=30):
    """Generate a mosaic of all of Canada using the fabdem 3979 tifs

    Should be run as a job file

    #TODO rewrite calls based on new functionality in extract

    """

    # Set up location and file name of out file
    canada_subdir = get_fabdem_path().joinpath(f'mosaics/{resample}/canada')
    dce.checkOutpath(canada_subdir)
    out_file = canada_subdir.joinpath('canada_mosaic.tif')

    opened_imgs = []
    # Get a list of fabdem3979 tifs
    fabdems = get_3979_fabdems(resample=resample)
    # sort the list and ensure unique
    fabdems = list(set(fabdems))

    # Open each one and add to list to be passed to merge
    for fabdem in fabdems:
        img = rasterio.open(fabdem,'r')
        opened_imgs.append(img)

    if mosaic_by_window:
        print(f'Start mosaic written window by window {dce.datetime.now()}')
        """

        This code
             - Forces square pixels by defining a single pixel size input
             - Handles multi-band data
             - Writes output in windows
             - Converts per image nodata to numpy.nan for writing to mosaic
             - Passes non-nodata values already written from previous images
                 back to current window
             - Does not manage numpy no data as the values are already properly
                 handeled in the input imagery
             - Gets output extent with an alignment of pixel coordinates
                 that are integer mulitples of pixel size
                 which matches target_aligned_pixels of rasterio.merge.merge
                 which matches -tap of GDAL utilities
            - Write out a bigtiff
            -Adapted from
                https://github.com/mapbox/rasterio/blob/master/rasterio/merge.py
                https://gis.stackexchange.com/questions/348925/merging-rasters-with-rasterio-in-blocks-to-avoid-memoryerror
        """
        # get destination image extent
        xs = []
        ys = []

        for opened_img in opened_imgs:
            left, bottom, right, top = opened_img.bounds
            xs.extend([left, right])
            ys.extend([bottom, top])
        dst_w, dst_s, dst_e, dst_n = min(xs), min(ys), max(xs), max(ys)

        # Output array shape and transform
        (dst_transform,
         dst_width,
         dst_height) = datacube_standard_transform(dst_w,dst_s,dst_e,dst_n,pixel_size)

        # Update parameters with new transform and shape add blocksize and compression
        kwargs = modify_kwargs(kwargs=opened_img.meta.copy(),
                                   dst_crs=img.crs,
                                   dst_transform=dst_transform,
                                   dst_w=dst_width,
                                   dst_h=dst_height,
                                   blocksize=1024)
        # add bigtiff to kwargs
        kwargs['BIGTIFF'] = 'IF_SAFER'
        # Band count from first image
        band_count = opened_imgs[0].count
        kwargs['count'] = band_count

        # Write all the files into a single file using the total imagery bounds
        # Write out the canada mosaic
        with rasterio.open(out_file,'w+',**kwargs) as dst:
            for opened_img in opened_imgs:
                # Get the no data value
                nodata = opened_img.nodata
                # For each band
                for band in range(1,band_count+1):
                    # For each window
                    for rc,src_window in opened_img.block_windows(band):
                        # Read per band window to numpy array
                        window_arr = opened_img.read(band,window=src_window)
                        # Define proper write window
                        src_bounds = rasterio.windows.bounds(src_window,transform=opened_img.transform)
                        dst_window = rasterio.windows.from_bounds(*src_bounds,transform=dst_transform)
                        # Round the window values
                        dst_window = rasterio.windows.Window(round(dst_window.col_off),
                                                              round(dst_window.row_off),
                                                              round(dst_window.width),
                                                              round(dst_window.height))
                        # Assign source no data to numpy.nan
                        window_arr[window_arr == nodata] = numpy.nan
                        # Read values already written to current window
                        existing_window = dst.read(band,window=dst_window)
                        # Overwrite current nodata values with existing values(reverse painters)
                        current_no_data = (numpy.isnan(window_arr))
                        window_arr[current_no_data] = existing_window[current_no_data]
                        # Write the numpy array to the output per band window
                        dst.write(window_arr,indexes=band,window=dst_window)
                # #close the opened image
                # opened_img.close()

        print(f'End mosaic by window {dce.datetime.now()}')
        # close open images
        for opened_img in opened_imgs:
            opened_img.close()
    else:
        print(f'Starting mosaic using merge {dce.datetime.now()}')

        # Get array and merge_transform from merge of all fabdems
        dst_arr,dst_transform = rasterio.merge.merge(opened_imgs)

        # Close open images
        for opened_img in opened_imgs:
            opened_img.close()

        # Update parameters with new transform and shape add blocksize and compression
        kwargs = modify_kwargs(kwargs=opened_img.meta.copy(),
                                   dst_crs=img.crs,
                                   dst_transform=dst_transform,
                                   dst_w=dst_arr.width,
                                   dst_h=dst_arr.height,
                                   blocksize=1028)
        # Write out the canada mosaic
        dce.checkOutpath(out_file)
        with rasterio.open(out_file,'w+',**kwargs) as dst:
            dst.write(dst_arr,indexes=1)
        print(f'End of mosaic using merge {dce.datetime.now()}')
    print(f'Finished processing {out_file.absolute()}')

    return out_file.absolute()

def dst_shape_transform(dst_w,dst_s,dst_e,dst_n,pixel_size):
    """Calculates shape and transform for output extent as
    integer mulitiples of pixel size.  Assumes coords and pixel size
    in same (destination) crs"""
    #TODO rewrite calls based on new functionality in extract

    # =============================================================================
    # modified from .../site-packages/rasterio/warp.py.aligned_target
    # matches functionality of dst_shape_transform which forces pixles to be square
    # In src crs, need in dst crs
    #
    #     ymin, xmin, ymax, xmax = src_img.bounds
    #     xmin = floor(xmin / res[0]) * res[0]
    #     xmax = ceil(xmax / res[0]) * res[0]
    #     ymin = floor(ymin / res[1]) * res[1]
    #     ymax = ceil(ymax / res[1]) * res[1]
    #     dst_transform = Affine(res[0], 0, xmin, 0, -res[1], ymax)
    #     dst_width = max(int(ceil((xmax - xmin) / res[0])), 1)
    #     dst_height = max(int(ceil((ymax - ymin) / res[1])), 1)
    # =============================================================================

    # Align output extent to integer multiples of pixel size
    dst_w = math.floor(dst_w/pixel_size) * pixel_size
    dst_e = math.ceil(dst_e/pixel_size) * pixel_size
    dst_s = math.floor(dst_s/pixel_size) * pixel_size
    dst_n = math.ceil(dst_n/pixel_size) * pixel_size

    # Output array shape.  Guaranteed to cover output bounds by rasterio.merge.merge
    dst_width = int(round((dst_e-dst_w)/pixel_size))
    dst_height = int(round((dst_n-dst_s)/pixel_size))

    # Calculate the Output transform affine matrix using rasterio.transform.Affine
    dst_transform = (Affine.translation(dst_w, dst_n)
                     * Affine.scale(pixel_size,-pixel_size))

    return dst_transform,dst_width,dst_height

def study_area_mosaics(resample='bilinear',fabdem=True,
                       national_grid=True,
                       grid_id=None,
                       out_dir=None):
    """Prepare mosaics per study area
    #TODO rewrite calls based on new functionality in extract
    """
    print(f'Start : {dce.datetime.now()}')
    print(f'Using resampled DEM : {resample}')
    print(f'Using fabdem DEM : {fabdem}')
    print(f'Using national grid : {national_grid}')
    dex = dce.DatacubeExtract()
    if fabdem:
        dem_source = 'fabdem'
    else:
        dem_source = 'copernicus'
    main_log = []
    error_log = []
    final_files = []
    p = _get_study_areas_file_path(national_grid)
    geojson_file = str(p.absolute())
    field = 'GRID_ID'
    # have to keep 32 bit depth or GeoTiff rounds to meters
    goal_dtype = 32
    goal_csize = 30

    f_main = f'{dem_source}_main.log'
    f_error = f'{dem_source}_error.log'
    main_log = []
    error_log = []
    if grid_id:
        extract_ids = [grid_id]
    else:
        # extract_ids = ['AY-41']
        # extract_ids = ['AY-39','AY-40','AY-41','AX-39','AX-40','AX-41','AW-39','AW-40','AW-41']
        extract_ids = ['X-40','X-41','Y-40','Y-41']
        # extract_ids = [extract_ids[0]]
    for extract_id in extract_ids:
        cwd = str(get_cwd_path(out_dir).joinpath(extract_id).absolute())

        print(f'working on study area {extract_id}')
        study_area,g,crs = useStudyAreaGeojson(geojson_file,field,extract_id)

        if national_grid:
            print('Clipping images from national grid canada mosaic')

            final_file = clip_from_mosaic(study_area=study_area,
                                          cwd=cwd,
                                          geom_d=g,
                                          dex=dex,
                                          resample=resample,
                                          crs=crs,
                                          f_main=f_main,
                                          f_error=f_error,
                                          national_grid=national_grid)


        else:
            print('Clipping and merging images per study area from fabdem')
            hpc_files,hpc_logs = getExtractFromHPC(cwd,study_area,g,dex,crs,
                                               f_main,f_error,
                                               resample,fabdem,national_grid)

            print('Mosaicing files')
            final_file = process_files(cwd,study_area,dex,
                                        main_log,error_log,
                                        f_main,f_error,
                                        goal_dtype=goal_dtype,
                                        goal_csize=goal_csize,
                                        resample='bilinear',
                                        goal_precision=2,
                                        keeptemp=False)
        final_files.append(final_file)
    print(final_files)
    mosaic_dir = f'mosaics/{resample}'

    for f in final_files:
        fp = pathlib.Path(f)
        tp = pathlib.Path(get_fabdem_path()).joinpath(mosaic_dir,fp.parts[-2])
        dex.checkOutpath(str(tp.absolute()))
        cmd = f'cp {f} {tp.joinpath(fp.name).absolute()}'
        # could automate the copy, for now print out to use later
        print(cmd)
    print(f'End : {dce.datetime.now()}')
    return

def clip_from_mosaic(study_area,cwd,geom_d,dex,resample='bilinear',crs='EPSG:3979',
                     f_main='fabdem_main.log',
                     f_error='fabdem_error.log',blocksize=512,national_grid=True):
    """Uses fabdem 3979 mosaic as source data and clips to study_area geometry
    #TODO rewrite calls based on new functionality in extract
    """
    main_log = []
    error_log = []
    cwd = dex.checkOutpath(cwd)

    suffix = "_DTM_O_30.tif"
    # print(f'makeImageFilesFromCog cwd {cwd}')
    # write the sample out to a file 'sample_<filename>'
    file_name = os.path.join(cwd,f'{study_area}{suffix}')
    main_log.append(f'make_image_from_cog file_name {file_name}')
    #TODO set up fxns to return proper canada mosaic, hardcoded for now
    #canada_mosaic = get_canada_mosaic_path(national_grid)
    canada_subdir = get_fabdem_path().joinpath(f'mosaics/{resample}/canada')
    canada_mosaic = canada_subdir.joinpath('canada_mosaic.tif')
    #print(file_name)
    # open cog with all types of GeoTIFF georeference except that within the TIFF file’s keys and tags turned off
    cog=rasterio.open(canada_mosaic,GEOREF_SOURCES='INTERNAL')

    # input crs matches cog crs
    t = dce.warp.transform_geom(crs,cog.crs,geom_d)

    geom_cog = shapely.geometry.shape(t)

    main_log.append(f'clip_from_mosaic geom_cog : {geom_cog}')
    dex.appendListToFile(main_log,f_main)
    # convert geom dict to a list and pass in to mask
    # want crop = True and all other params default values
    try:
        sample_cog,cog_transform = dex.mask(dataset=cog,shapes=[geom_cog],crop=True)

        #calculate height and width index of shape from number of dimensions
        height=sample_cog.shape[sample_cog.ndim-2]
        width=sample_cog.shape[sample_cog.ndim-1]
        # TODO use the existing kwargs modification
        dst=rasterio.open(
            file_name,
            'w+',
            driver='GTiff',
            height=height,
            width=width,
            count=1,
            dtype=cog.dtypes[0],
            crs=cog.crs,
            transform=cog_transform,
            nodata=cog.nodata,
            GEOREF_SOURCES='INTERNAL',
            compress='LZW',
            tiled=True,
            blockxsize=blocksize,
            blockysize=blocksize
            )
        dst.write(sample_cog)
        dst.close()
        cog.close()

    except Exception as e:
        error_log.append(f'clip_from_mosaic Error with mask or writing file {e}')
        dex.appendListToFile(error_log,f_error)
    return file_name


def useStudyAreaGeojson(geojson_file,field,extract_id):
    # extract version needed to be in 4326 for calls to WCS or Franklin
    # fabdem is reprojected to 3979 so we can do all work in in 3979
    #TODO rewrite calls based on new functionality in extract
    epsg_num = 3979
    to_crs=f'EPSG:{epsg_num}'
    # check for geojson file
    if not os.path.isfile(geojson_file):
        dce.sys.exit("No json file, if a service it should be a 400 bad request")
    #create lat lon geodataframe with all of the study areas
    sas = geopandas.read_file(geojson_file)
    if sas.crs == rasterio.crs.CRS.from_epsg(epsg_num):
        rp_sas = sas
    else:
        rp_sas = sas.to_crs(to_crs)
    # grab out study area name
    name = rp_sas[rp_sas[field]==extract_id][field].values[0]
    study_area = name
    # grab out study area geometry as geopandas geoseries
    geometry = rp_sas[rp_sas[field]==extract_id]['geometry']
    # transform geopanda geoseries to python dict
    g = json.loads(geometry.to_json())["features"][0]["geometry"]
    return study_area,g,to_crs

def process_files(cwd,study_area,dex,
                  main_log,error_log,
                  f_main,f_error,
                  goal_dtype=16,
                  goal_csize=30,
                  resample='nearest',
                  goal_precision=2,
                  keeptemp=False):
    #TODO rewrite calls based on new functionality in extract
    internal_log = []
    merge_files = []
    suffix = "_DTM_O_30.tif"
    res_enum = dce.getResampleEnum(resample)
    try:

        sub_areas = sorted(dce.glob.glob(f'{cwd}/{study_area}_sample-*.tif'),reverse=True)
        num_sa = len(sub_areas)
        if num_sa == 0:
            main_log.append("no study areas available, exiting")
            dce.sys.exit("No study areas")
        datasets = []
        if num_sa == 1:
        # if there is only a single tile cliped for a study area rename it to valid study area name
            old_name = sub_areas[0]
            new_name = f'{cwd}/{study_area}-{suffix}'
            os.rename(old_name,new_name)
            main_log.append("only one extract for study area %s renamed %s to %s"%(study_area,old_name,new_name))
        else:
        # pull out the generated sample tifs <prov>_sample_<original-file-name>.tif
        # open each dataset and pass open dataset handle into dict array
        # test data types, resolutions and linear units
            dtypes = []
            unique_dtypes = []
            for sub_area in sub_areas:
                dataset = rasterio.open(sub_area,'r',GEOREF_SOURCES='INTERNAL')
                # print(f'dataset {sub_area} shape {dataset.shape}')
                # print(f'affine transform: {dataset.transform}')
                # merge needs to be on same datatypes, test datatypes for each clip
                test_dtype=dataset.dtypes[0]
                dtypes.append(test_dtype)
                main_log.append("Merge will fail if datatype are different - datatype for {}: {}".format(sub_area,test_dtype))
                datasets.append(dataset)
            unique_dtypes = list(set(dtypes))
            if len(unique_dtypes) > 1:
                dce.sys.exit("FABDEM files have more than one datatype")
            else:
                # grab out a few things from last open dataset
                crs = dataset.crs
                dtype = dataset.dtypes[0]
                nodata = dataset.nodata
                if not nodata:
                    nodata = -9999
                # call merge files directly
                print('merging files fabdem specific')
                print(crs)
                sa_merge_file = merge_reproject_files(datasets,dex,cwd,study_area,
                                                      suffix,crs,nodata,
                                                      goal_csize,res_enum,
                                                      goal_precision)
                merge_files.append(sa_merge_file)
                main_log.append("Merge Log from process_files(single unique dtype):")
                main_log.append(f'merging for study area {study_area} written to {sa_merge_file}')
        main_log.append(f'merging extracts finished {dce.datetime.now()}')
        main_log.append("cleaning up, deleting the sample files")
        sample_files = []
        sample_str = os.path.join(cwd,"*_sample-*.tif")
        sample_files = dce.glob.glob(sample_str)
        main_log.append(f'assessing keeptemp {keeptemp}')
        if keeptemp:
            main_log.append("keeping temp tifs {}".format(sample_files))
        else:
            for sample_file in sample_files:
                os.remove(sample_file)
                main_log.append("removed sample file {}".format(sample_file))

            other_temps = ['request_dsm_intersection.json',
                            'request_dtm_intersection.json',
                            'original_request3979.json',
                            'dtm_dsm_difference.json',
                            'dtm_dsm_intersection.json',
                            'DescribeCoverage_dsm.json',
                            'DescribeCoverage_dsm.xml',
                            'DescribeCoverage_dtm.json',
                            'DescribeCoverage_dtm.xml']
            for other_temp in other_temps:
                temp_file = os.path.join(cwd,other_temp)
                if os.path.isfile(temp_file):
                    try:
                        os.remove(temp_file)
                        main_log.append("deleting temp file: {}".format(temp_file))
                    except Exception as e:
                        main_log.append(f'cannot remove temp file: {temp_file} exception: {e}')
                else:
                    main_log.append("temp file {} is not a file".format(temp_file))

    except Exception as e:
        error_log.append("start {}".format(dce.datetime.now()))
        error_log.append('Exception info: %s' %(str(e)))
        etype, value, traceback = dce.sys.exc_info()
        error_log.append('etype: %s'%(etype))
        error_log.append('value: %s'%(value))
        error_log.append('frame: %s'%(traceback.tb_frame))
        error_log.append('line number: %s'%(traceback.tb_lineno))
        error_log.append('last instruction: %s'%(traceback.tb_lasti))
        internal_log.append(dex.appendListToFile(error_log,f_error))
        print(internal_log)
    #write out logs
    status_str = f'finished {dce.datetime.now()}'
    #print(status_str)
    main_log.append(status_str)
    internal_log.append(dex.appendListToFile(main_log,f_main))
    internal_log.append(dex.appendListToFile(internal_log,f_main))
    #print(internal_log)
    return sa_merge_file

def _get_grid_id_subdir(national_grid=True):
    if national_grid:
        grid_type = 'national'
    else:
        grid_type = 'original'
    sub_dir = f'grids/{grid_type}/studyareas'
    return sub_dir

def _grid_id_fabdems_json_path(resample='bilinear',national_grid=True):
    sub_dir = _get_grid_id_subdir(national_grid)
    file_name = f'grid_id_fabdems3979{resample}.json'
    gfj = get_fabdem_path().joinpath(sub_dir,file_name)
    return gfj

def _grid_id_fabdems_geojson_path(resample='bilinear',national_grid=True):
    """The file path to the grid_id_fabdem geojson"""
    sub_dir = _get_grid_id_subdir(national_grid)
    file_name = f'grid_id_fabdems3979{resample}.geojson'
    gfj = get_fabdem_path().joinpath(sub_dir,file_name)
    return gfj

def _grid_id_copernicus_json_path():
    gfj = get_fabdem_path().joinpath('grid_id_copernicus3979.json')
    return gfj

def get_fabdem_path():
    p = pathlib.Path('/gpfs/fs3/nrcan/nrcan_geobase/work/data/hem/ref/FAUDEM/')
    return p

def get_fabdem_files(study_area,resample='bilinear',national_grid=True):
    files = []

    f = _grid_id_fabdems_geojson_path(resample,national_grid)

    if f.is_file():
        #convert comma seperated string of files into list
        gdf = geopandas.read_file(f)
        files_str = gdf[gdf['study_area']==study_area]['files'].values[0]
        files = files_str.split(',')
    else:
        print(f'File {f} does not exist')
    return files

def get_copernicus_files(study_area):
    files = []
    f = _grid_id_copernicus_json_path()
    if f.is_file():
        fp = f.open()
        j = json.load(fp)
        fp.close()
        files = j[study_area]
    return files

def _get_study_areas_file_path(national_grid=True):
    """Path to the geojson defining study area grids"""
    # /gpfs/fs3/nrcan/nrcan_geobase/work/data/hem/ref/FAUDEM/grids/national/studyareas/FS_NationalGrid.geojson
    # Define subdir and file
    if national_grid:
        file = 'FS_NationalGrid.geojson'
    else:
        file = 'TrainingPtSet_full.geojson'

    # Define the path to the file
    subdir = _get_grid_id_subdir(national_grid)
    # Get root fabdem path
    p = get_fabdem_path()
    # Append sub_dir to root
    fp = p.joinpath(subdir,file)
    return fp

def _get_sample_area_gdf(grid_id='AY-39'):
    p = _get_study_areas_file_path()
    study_areas = geopandas.GeoDataFrame.from_file(p)
    study_area = study_areas[study_areas['GRID_ID']==grid_id]
    return study_area

def get_cwd_path(working_dir):
    if working_dir:
        cwd = pathlib.Path(working_dir)
    else:
        cwd = pathlib.Path(os.getcwd())
    return cwd



def _fabdems_geojson_path(resample='bilinear'):
    sub_dir = _fabdem3979_dir()
    fgj = get_fabdem_path().joinpath(sub_dir,resample,'fabdems3979.geojson')
    return fgj

def _copernicus_geojson_path():
    fgj = get_fabdem_path().joinpath('copernicus3979.geojson')
    return fgj

def make_fabdem_geojson(resample='bilinear'):
    """Creates FABDEM geojson with local file location and
    geometry from cog.bounds"""
    polys = []
    files = []
    # p = get_fabdem_path()

    fabdem_files = list(get_3979_fabdems(resample))
    c = rasterio.open(fabdem_files[0])
    crs = c.crs
    c.close()
    for fabdem_file in fabdem_files:
        c = rasterio.open(fabdem_file)
        b = shapely.geometry.box(*c.bounds)
        polys.append(b)
        c.close()
        files.append(str(fabdem_file.absolute()))
    df = pandas.DataFrame({'file':files,'geometry':polys})
    gdf = geopandas.GeoDataFrame(df,crs=crs)
    location = str(_fabdems_geojson_path(resample).absolute())
    gdf.to_file(location,driver='GeoJSON')
    print(f'GeoJson created {location}')
    return location

def make_copernicus_geojson():
    """Creates copernicus geojson with local file location and
    geometry from cog.bounds"""
    polys = []
    files = []
    # p = get_fabdem_path()

    fabdem_files = list(get_3979_copernicus())
    c = rasterio.open(fabdem_files[0])
    crs = c.crs
    c.close()
    for fabdem_file in fabdem_files:
        c = rasterio.open(fabdem_file)
        b = shapely.geometry.box(*c.bounds)
        polys.append(b)
        c.close()
        files.append(str(fabdem_file.absolute()))
    df = pandas.DataFrame({'file':files,'geometry':polys})
    gdf = geopandas.GeoDataFrame(df,crs=crs)
    location = str(_copernicus_geojson_path().absolute())
    gdf.to_file(location,driver='GeoJSON')
    print(f'GeoJson created {location}')
    return location

def make_gridid_copernicus_json():
    """Creaes copernicus json linking flood susceptability study area grid ids
    to local copernicus files both in 3979 projection"""
    fgj = _copernicus_geojson_path()
    saj = _get_study_areas_file_path()
    fjf = _grid_id_copernicus_json_path()
    print(fjf)
    fabdems = geopandas.GeoDataFrame.from_file(fgj)
    study_areas = geopandas.GeoDataFrame.from_file(saj)
    if study_areas.crs == fabdems.crs:
        intersects = {}
        for j in range(0,study_areas.shape[0]-1):
            sa = study_areas[j:j+1]
            sa_val = sa['GRID_ID'].values[0]
            intersects[sa_val]=[]
            print(sa_val)
            for i in range(0,fabdems.shape[0]-1):
                fabdem = fabdems[i:i+1]
                inter = fabdem.intersects(sa,align=False)
                if inter.values[0]:
                    fabdem_val = fabdem['file'].values[0]
                    intersects[sa_val].append(fabdem_val)
                    # print(fabdem_val)
        fp = fjf.open('w',encoding='utf-8')
        json.dump(intersects,fp,indent=4,ensure_ascii=False)
        fp.close()
    else:
        print(f'no matching crs {study_areas.crs} {fabdem.crs}')
    print(f"made grid_id_fabdem lookup here {fjf}")
    return

def make_gridid_fabdem_json(resample='bilinear'):
    """Creaes FABDEM json linking flood susceptability study area grid ids
    to local fabdem files both in 3979 projection"""
    fgj = _fabdems_geojson_path()
    saj = _get_study_areas_file_path()
    fjf = _grid_id_fabdems_json_path()
    print(fjf)
    fabdems = geopandas.GeoDataFrame.from_file(fgj)
    study_areas = geopandas.GeoDataFrame.from_file(saj)
    if study_areas.crs == fabdems.crs:
        intersects = {}
        for j in range(0,study_areas.shape[0]):
            sa = study_areas[j:j+1]
            sa_val = sa['GRID_ID'].values[0]
            intersects[sa_val]=[]
            print(sa_val)
            for i in range(0,fabdems.shape[0]-1):
                fabdem = fabdems[i:i+1]
                inter = fabdem.intersects(sa,align=False)
                if inter.values[0]:
                    fabdem_val = fabdem['file'].values[0]
                    intersects[sa_val].append(fabdem_val)
                    # print(fabdem_val)
        fp = fjf.open('w',encoding='utf-8')
        json.dump(intersects,fp,indent=4,ensure_ascii=False)
        fp.close()
    else:
        print(f'no matching crs {study_areas.crs} {fabdem.crs}')
    print(f"made grid_id_fabdem lookup here {fjf}")
    return

def make_gridid_fabdem_geojson(resample='bilinear',national_grid=True):
    """Creaes FABDEM geojson linking flood susceptability study area grid ids
    and bounding boxes to
    to local fabdem files both in 3979 projection
    {"<grid_id>":{"geometry":<geometry>,"files":[<files>]}
    """

    fgj = _fabdems_geojson_path(resample)
    saj = _get_study_areas_file_path(national_grid)
    fjf = _grid_id_fabdems_geojson_path(resample,national_grid)
    print(fjf)
    fabdems = geopandas.GeoDataFrame.from_file(fgj)
    study_areas = geopandas.GeoDataFrame.from_file(saj)
    if study_areas.crs == fabdems.crs:
        intersects = {'resample':resample,
                      'national_grid':national_grid,
                      'study_areas':[]}

        for j in range(0,study_areas.shape[0]):
            sa = study_areas[j:j+1]
            sa_val = sa['GRID_ID'].values[0]
            # grab out study area geometry as geopandas geoseries
            geometry = sa['geometry'].values[0]

            # # transform geopanda geoseries to python dict
            # g = json.loads(geometry.to_json())["features"][0]["geometry"]
            int_files=[]
            for i in range(0,fabdems.shape[0]-1):
                fabdem = fabdems[i:i+1]
                inter = fabdem.intersects(sa,align=False)
                if inter.values[0]:
                    fabdem_val = fabdem['file'].values[0]
                    int_files.append(fabdem_val)
                    # print(fabdem_val)
            intersects['study_areas'].append({'study_area':sa_val,
                                              'geometry':geometry,
                                              'files':','.join(int_files)})


        # fp = fjf.open('w',encoding='utf-8')
        # json.dump(intersects,fp,indent=4,ensure_ascii=False)
        # fp.close()
        df = pandas.DataFrame.from_dict(intersects['study_areas'])
        gdf = geopandas.GeoDataFrame(df,crs=study_areas.crs)
        gdf.to_file(fjf,driver='GeoJSON')
    else:
        print(f'no matching crs {saj}.crs {study_areas.crs} {fgj}.crs{fabdems.crs}')
    print(f"made grid_id_fabdem lookup here {fjf}")
    return

def getExtractFromHPC(cwd,study_area,g,dex,crs,f_main,f_error,resample='bilinear',
                      fabdem=True,national_grid=True):
    """Extracts from a local HPC collection currnetly FABDEM"""
    cwd = dex.checkOutpath(cwd)
    main_log = []
    error_log = []
    internal_log = []
    file_names = []

    if fabdem:
        hpc_files = get_fabdem_files(study_area,resample,national_grid)
        dem_source = 'fabdem'
    else:
        hpc_files = get_copernicus_files(study_area)
        dem_source = 'copernicus'

    for hpc_file in hpc_files:
        # if fabdem:
        #     # add bilinear directory to fabdem, copernicus only has bilinear
        #     hpc_file = hpc_file.replace(f'/{dem_source}3979/',f'/{dem_source}3979/{resample}/')

        internal_log.append(f'hpc_file: {hpc_file}')
        # write out the sample
        # try:

        file_name = dex.makeImageFilesFromCog(hpc_file,cwd,study_area,g,crs,f_main,f_error)
        print(f'***Making Image file made: {file_name}')
        file_names.append(file_name)
        main_log.append(f'created file from STAC/COG {file_name}'
                        f' for study area {study_area}')
        # except Exception as e:
        #     main_log.append(f"cant create file from {hpc_file}")
        #     error_log.append(f'makeImageFiles Error {e}')
        #     internal_log.append(dce.appendListToFile(error_log,f_error))
        main_log.append("created files from STAC/COG for study area %s"%(study_area))
        internal_log.append(dex.appendListToFile(main_log,f_main))
    return file_names,internal_log

def make_image_file_from_cog(url,cwd,name,geom_d,crs,f_main,f_error,blocksize=512):
    """Uses local 3979 30 m reprojects of copernicus"""
    main_log = []
    error_log = []
    cwd = dce.checkOutpath(cwd)
    # print(f'makeImageFilesFromCog cwd {cwd}')
    # write the sample out to a file 'sample_<filename>'
    file_name="%s/%s_sample-%s"%(cwd,name,url.split('/')[-1])
    main_log.append(f'make_image_from_cog file_name {file_name}')

    #print(file_name)
    # open cog with all types of GeoTIFF georeference except that within the TIFF file’s keys and tags turned off
    cog=rasterio.open(url,GEOREF_SOURCES='INTERNAL')

    # input crs matches cog crs
    t = dce.transform_geom(crs,cog.crs,geom_d)

    geom_cog = shapely.geometry.shape(t)

    main_log.append(f'makeImageFilesFromCog geom_cog : {geom_cog}')
    dce.appendListToFile(main_log,f_main)
    # convert geom dict to a list and pass in to mask
    # want crop = True and all other params default values
    try:
        sample_cog,cog_transform = dce.mask(dataset=cog,shapes=[geom_cog],crop=True)

        #calculate height and width index of shape from number of dimensions
        height=sample_cog.shape[sample_cog.ndim-2]
        width=sample_cog.shape[sample_cog.ndim-1]

        dst=rasterio.open(
            file_name,
            'w+',
            driver='GTiff',
            height=height,
            width=width,
            count=1,
            dtype=cog.dtypes[0],
            crs=cog.crs,
            transform=cog_transform,
            nodata=cog.nodata,
            GEOREF_SOURCES='INTERNAL',
            compress='LZW',
            tiled=True,
            blockxsize=blocksize,
            blockysize=blocksize
            )
        dst.write(sample_cog)
        dst.close()
        cog.close()
    except Exception as e:
        error_log.append(f'MakeImageFileFromCog Error with mask or writing file {e}')
        dce.appendListToFile(error_log,f_error)
    return file_name


def merge_reproject_files(datasets,dex,cwd,study_area,suffix,crs,nodata,goal_csize,resample,goal_precision):
    # cog default block size
    blocksize=512
    cwd=dex.checkOutpath(cwd)
    mergedfiles=[]
    # res=getResolution(datasets,goal_csize)
    # print(f'resolution {res}')
    # precision=4
    #merge the open datasets to a numpy array
    merge_arr,merge_affine=dex.merge.merge(datasets)

    for dataset in datasets:
        dataset.close()
    #sa_merge_file=os.path.join(cwd,"{}_{}_p{}-{}".format(study_area,resample.name,res[0],suffix))
    #Heather wants GRIDID_DTM_O_30
    sa_merge_file = os.path.join(cwd,f'{study_area}{crs}{suffix}')
    final_file = os.path.join(cwd,f'{study_area}{suffix}')
    #write array to cog
    m=rasterio.open(
        final_file,'w',
        height=merge_arr.shape[1],
        width=merge_arr.shape[2],
        transform=merge_affine,
        dtype=merge_arr.dtype,
        crs=crs,
        count=1,
        driver='GTiff',
        compress='LZW',
        tiled=True,
        blockxsize=blocksize,
        blockysize=blocksize,
        nodata=nodata,
        GEOREF_SOURCES='INTERNAL'
        )
    m.write(merge_arr)
      ## cant add description header with all of the source files - not working
      ## does not work m.update_tags(a= ','.join(datasets))
      ## does not work m.set_band_description(1, ','.join(datasets))
    m.close()
    # No longer reprojecting at end, fabdems reprojected to 3979 are used from beggining
    # reproject_image(sa_merge_file,final_file)
    return final_file

# =============================================================================
# def reproject_image(src_file,dst_file,
#                     dst_crs={'init':'EPSG:3979'},
#                     dst_res=30,
#                     resample='nearest',
#                     blocksize=512):
#     """
#     modfied from https://rasterio.readthedocs.io/en/latest/topics/reproject.html
#     """
#     src_img = rasterio.open(src_file)
#     l,b,r,t = src_img.bounds
#     (dst_transform,
#       dst_w,
#       dst_h) = rasterio.warp.calculate_default_transform(src_crs=src_img.crs,
#                                                         dst_crs=dst_crs,
#                                                         width=src_img.width,
#                                                         height=src_img.height,
#                                                         left=l,
#                                                         bottom=b,
#                                                         right=r,
#                                                         top=t,
#                                                         resolution=dst_res)
#
#     kwargs = src_img.meta.copy()
#     kwargs.update({
#         'crs':dst_crs,
#         'transform':dst_transform,
#         'width':dst_w,
#         'height':dst_h,
#         })
#     # rewrite image compressed, tiled to block size
#     kwargs['compress'] = 'LZW'
#     kwargs['tiled'] = True
#     kwargs['blockxsize'] = blocksize
#     kwargs['blockysize'] = blocksize
#     # print(kwargs)
#     with rasterio.open(dst_file,'w',**kwargs) as dst:
#         # multi-band loop
#         for i in range(1,src_img.count+1):
#             rasterio.warp.reproject(
#                 source=rasterio.band(src_img,i),
#                 destination=rasterio.band(dst,i),
#                 src_transform=src_img.transform,
#                 src_crs=src_img.crs,
#                 dst_transform=dst_transform,
#                 dst_crs=dst_crs,
#                 resampling=dce.getResampleEnum(resample)
#
#                 )
#     src_img.close()
#
#
#
#     return
# =============================================================================

def modify_kwargs(kwargs,dst_crs,dst_transform,dst_w,dst_h,blocksize):
    kwargs.update({
        'crs':dst_crs,
        'transform':dst_transform,
        'width':dst_w,
        'height':dst_h,
        })
    # rewrite image compressed, tiled to block size
    kwargs['compress'] = 'LZW'
    kwargs['tiled'] = True
    kwargs['blockxsize'] = blocksize
    kwargs['blockysize'] = blocksize
    kwargs['GEOREF_SOURCES'] ='INTERNAL'
    return kwargs

def destination_transform(dst_bounds,src_crs,
                          src_w,src_h,
                          dst_crs={'init':'EPSG:3979'},dst_res=30):
    l,b,r,t = dst_bounds
    (dst_transform,
     dst_w,
     dst_h) = rasterio.warp.calculate_default_transform(src_crs=src_crs,
                                                        dst_crs=dst_crs,
                                                        width=src_w,
                                                        height=src_h,
                                                        left=l,
                                                        bottom=b,
                                                        right=r,
                                                        top=t,
                                                        resolution=dst_res)
    return dst_transform,dst_w,dst_h

def reproject_image(src_file,dst_file,
                    dst_crs={'init':'EPSG:3979'},
                    dst_res=30,
                    resample='nearest',
                    blocksize=512):
    """Reprojects image per band, per window
       Uses 'best fit' destination window, not aligned to pixel size
    """
    """
    modfied from https://rasterio.readthedocs.io/en/latest/topics/reproject.html
    need to add more transform origins etc from https://github.com/rasterio/rasterio/blob/master/examples/reproject.py
    """
    src_img = rasterio.open(src_file)
    # uses rasterio.transform.calculate_default_transform
    dst_transform,dst_w,dst_h = destination_transform(src_img.bounds,
                                                      src_img.crs,
                                                      src_img.width,
                                                      src_img.height,
                                                      dst_crs,dst_res)
    kwargs = modify_kwargs(src_img.meta.copy(),
                           dst_crs,dst_transform,
                           dst_w,dst_h,
                           blocksize)

    with rasterio.open(dst_file,'w',**kwargs) as dst:
        # multi-band loop
        for i in range(1,src_img.count+1):
            rasterio.warp.reproject(
                source=rasterio.band(src_img,i),
                destination=rasterio.band(dst,i),
                src_transform=src_img.transform,
                src_crs=src_img.crs,
                dst_transform=dst_transform,
                dst_crs=dst_crs,
                resampling=dce.getResampleEnum(resample)

                )
    src_img.close()
    return

def reproject_image_to_pixel_size(src_file,dst_file,
                    dst_crs={'init':'EPSG:3979'},
                    pixel_size=30,
                    resample='nearest',
                    blocksize=512):
    """Reprojects image per band, per window
       Uses 'destination window that is an integer aligned to pixel size
       Best run on images that are tiled
    """
    """
    modfied from https://rasterio.readthedocs.io/en/latest/topics/reproject.html
    need to add more transform origins etc from https://github.com/rasterio/rasterio/blob/master/examples/reproject.py
    modified from .../site-packages/rasterio/warp.py
    """
    src_img = rasterio.open(src_file)
    if src_img.crs != dst_crs:
        # convert to bbox wkt dict
        bbox = dce.box(*src_img.bounds)
        # get bounds from return polygon
        src_bounds = dce.transform_dict_to_poly(bbox_dict=bbox,
                                                    in_crs=src_img.crs,
                                                    out_crs=dst_crs).bounds
    else:
        src_bounds = src_img.bounds
    west,south,east,north = src_bounds
    dst_transform,dst_w,dst_h = dst_shape_transform(west,south,east,north,pixel_size)

    # dst_transform,dst_w,dst_h = destination_transform(src_img.bounds,
    #                                                   src_img.crs,
    #                                                   src_img.width,
    #                                                   src_img.height,
    #                                                   dst_crs,dst_res)
    kwargs = modify_kwargs(src_img.meta.copy(),
                           dst_crs,dst_transform,
                           dst_w,dst_h,
                           blocksize)

    with rasterio.open(dst_file,'w',**kwargs) as dst:
        # multi-band loop
        for i in range(1,src_img.count+1):
            rasterio.warp.reproject(
                source=rasterio.band(src_img,i),
                destination=rasterio.band(dst,i),
                src_transform=src_img.transform,
                src_crs=src_img.crs,
                dst_transform=dst_transform,
                dst_crs=dst_crs,
                resampling=dce.getResampleEnum(resample)

                )
    src_img.close()
    return



def get_original_fabdems(subdir_filter=None):
    originals = []
    if subdir_filter:
        fp = get_fabdem_path().joinpath('fabdem4326',subdir_filter)
    else:
        fp = get_fabdem_path().joinpath('fabdem4326')
    all_tifs = fp.glob('**/*.tif')
    for tif in all_tifs:
        tif_str = str(tif.absolute())
        if not ('/mosaics/' in tif_str or 'fabdem3979' in tif_str):
            originals.append(tif)
    return originals

def _fabdem3979_dir():
    return 'fabdem3979'

def _copernicus3979_dir():
    return 'copernicus3979'


def get_3979_fabdems(resample='bilinear'):
    all3979s = []
    f3979s = []
    fp = get_fabdem_path()
    all3979s = fp.joinpath(_fabdem3979_dir(),resample).glob('**/*.tif')
    for a3979 in all3979s:
        # exclued samples that were vertical datum corrected while 4326
        #print(a3979.name)
        if 'CGVD2013' not in a3979.name and '_diff.tif' not in a3979.name:
            f3979s.append(a3979)
    return f3979s

def get_3979_copernicus(resample='bilinear'):
    all3979s = []
    f3979s = []
    fp = get_fabdem_path()
    all3979s = fp.joinpath(_copernicus3979_dir(),resample).glob('**/*.tif')
    for a3979 in all3979s:
        # exclued samples that were vertical datum corrected while 4326
        #print(a3979.name)
        if 'CGVD2013' not in a3979.name and 'diff' not in a3979.name:
            f3979s.append(a3979)
    return f3979s

# =============================================================================
# def _reproject_fabdems():
#     """Reprojecting fabdems to 3979 with default reprojection sampling (nearest)"""
#     fp = get_fabdem_path()
#     fd3979 = fp.joinpath(_fabdem3979_dir())
#     fd3979 = dce.checkOutpath(fd3979)
#     original_fabdems = get_original_fabdems()
#     #original_fabdems = [pathlib.Path('/gpfs/fs3/nrcan/nrcan_geobase/work/data/hem/ref/FAUDEM/N60W110-N70W100_FABDEM_V1-0/N64W105_FABDEM_V1-0.tif')]
#     for original_fabdem in original_fabdems:
#         src_file = str(original_fabdem.absolute())
#         dst_file = fd3979.joinpath(original_fabdem.name)
#         reproject_image(src_file,dst_file)
#         print(f'reprojected {src_file} to {dst_file}')
#     return
# =============================================================================


def reproject_fabdems(resample='bilinear',pixel_size=30,subdir_filter=None):
    """Reprojecting fabdems to 3979 with bilinear reprojection sampling"""
    start = dce.datetime.now()
    print(f'Start : {start}')
    fp = get_fabdem_path()
    # make a bilinear subdir
    fd3979 = fp.joinpath(_fabdem3979_dir(),resample)
    fd3979 = dce.checkOutpath(fd3979)
    original_fabdems = get_original_fabdems(subdir_filter)
    for original_fabdem in original_fabdems:
        src_file = str(original_fabdem.absolute())
        dst_file = fd3979.joinpath(original_fabdem.name)
        # reproject_image(src_file,dst_file,resample='bilinear')
        reproject_image_to_pixel_size(src_file,dst_file,
                                      resample='bilinear',
                                      pixel_size=pixel_size)
        print(f'reprojected {src_file} to {dst_file}')
    finish = dce.datetime.now()
    print(f'Finish : {finish}')
    return

def compare_fabdems():
    sames = []
    differents = []
    msgs = []
    compare_file = 'fabdem_deltas.json'

    fp3979 = get_fabdem_path().joinpath(_fabdem3979_dir())
    bilinears = fp3979.joinpath('bilinear').glob('**/*.tif')
    for bilinear in bilinears:
        if 'diff_b-n' not in bilinear.name:
            nearest = pathlib.Path(fp3979,bilinear.name)
            # update lists with delta info
            delta(nearest,bilinear,sames,differents,msgs)
    d = {
        'sames':sames,
        'differents':differents,
        'msgs':msgs}
    jp = pathlib.Path(compare_file)
    with jp.open('w') as fp:
        json.dump(d,fp)
    return

def compare_fabdem_mosaics():
    sames = []
    differents = []
    msgs = []
    compare_file = 'fabdem_mosiac_deltas.json'
    fpmosaics = get_fabdem_path().joinpath(get_fabdem_path(),'mosaics')
    bilinears = fpmosaics.joinpath('bilinear').glob('**/*.tif')
    for bilinear in bilinears:
        if 'diff_b-n' not in bilinear.name:
            nearest = pathlib.Path(fpmosaics,bilinear.parts[-2],bilinear.parts[-1])
            # update lists with delta info
            delta(nearest,bilinear,sames,differents,msgs,make_image=True)
    d = {
        'sames':sames,
        'differents':differents,
        'msgs':msgs}
    jp = pathlib.Path(compare_file)
    with jp.open('w') as fp:
        json.dump(d,fp)
    return

def delta(i1,i2,sames,differents,msgs,make_image=False):
    """Compares per pixel values for two images
    Optionally writes delta out as own image"""
    p1 = pathlib.Path(i1)
    p2 = pathlib.Path(i2)
    if p1.is_file() and p2.is_file():
        c1 = rasterio.open(p1)
        c2 = rasterio.open(p2)
        #compare shapes
        if c1.shape == c2.shape:
            a1 = c1.read(1)
            a2 = c2.read(1)
            d = a2-a1
            non_zeros = numpy.count_nonzero(d)
            percent = (non_zeros/(c1.width*c1.height))*100
            msg = f'{p2}-{p1} -> {percent} % pixels that are different'
            msgs.append(msg)
            if non_zeros > 0:
                differents.append(p2.name)
                if make_image:
                    kwargs = c2.meta.copy()
                    new_name = p2.with_name('diff_b-n.tif')
                    save_cog(new_name,d,kwargs)
            else:
                sames.append(p2.name)

        else:
            print('Images must be the same size')
            print(f'Image 1 {p1} is {c1.shape}')
            print(f'Image 2 {p2} is {c2.shape}')
        c1.close()
        c2.close
    else:
        print(f'at least one not a file {p1} {p2}')
    return

def save_cog(file_name,arr,kwargs,blocksize=512):
    """TODO read array or image per band per window
    # with rasterio.open(dst_file,'w+',**kwargs) as dst:
    #     for i in range(1,src_img.count+1):
    #         for rc, window in src_img.block_windows(i):
    #             dst.write(src_img.read(i,window=window),indexes=i,window=window)
    """
    # add image compression and tiles
    kwargs['compress'] = 'LZW'
    kwargs['tiled'] = True
    kwargs['blockxsize'] = blocksize
    kwargs['blockysize'] = blocksize

    with rasterio.open(file_name,'w',**kwargs) as new:
        new.write(arr,indexes=1)
    return

def create_local_copernicus(resample='bilinear'):
    print(f'Start : {dce.datetime.now()}')
    dst = get_fabdem_path().joinpath('copernicus3979',resample)
    dce.checkOutpath(dst)

    coperns = ['http://copernicus-dem-30m.s3.amazonaws.com/Copernicus_DSM_COG_10_N49_00_W098_00_DEM/Copernicus_DSM_COG_10_N49_00_W098_00_DEM.tif',
               'http://copernicus-dem-30m.s3.amazonaws.com/Copernicus_DSM_COG_10_N49_00_W099_00_DEM/Copernicus_DSM_COG_10_N49_00_W099_00_DEM.tif',
               'http://copernicus-dem-30m.s3.amazonaws.com/Copernicus_DSM_COG_10_N50_00_W098_00_DEM/Copernicus_DSM_COG_10_N50_00_W098_00_DEM.tif',
               'http://copernicus-dem-30m.s3.amazonaws.com/Copernicus_DSM_COG_10_N50_00_W099_00_DEM/Copernicus_DSM_COG_10_N50_00_W099_00_DEM.tif',
               'http://copernicus-dem-30m.s3.amazonaws.com/Copernicus_DSM_COG_10_N50_00_W100_00_DEM/Copernicus_DSM_COG_10_N50_00_W100_00_DEM.tif',
               'http://copernicus-dem-30m.s3.amazonaws.com/Copernicus_DSM_COG_10_N50_00_W097_00_DEM/Copernicus_DSM_COG_10_N50_00_W097_00_DEM.tif',
               'http://copernicus-dem-30m.s3.amazonaws.com/Copernicus_DSM_COG_10_N49_00_W100_00_DEM/Copernicus_DSM_COG_10_N49_00_W100_00_DEM.tif',
               'http://copernicus-dem-30m.s3.amazonaws.com/Copernicus_DSM_COG_10_N49_00_W097_00_DEM/Copernicus_DSM_COG_10_N49_00_W097_00_DEM.tif'
               ]
    # coperns = [coperns[0]]
    for copern in coperns:
        dst_file = dst.joinpath(copern.split('/')[-1])
        print(f'reprojectiong {copern}')
        print(f'to {dst}')
        reproject_image(copern,dst_file.absolute(),resample='bilinear')
    return
    print(f'End : {dce.datetime.now()}')

def build_overviews(file,
                    dec_factors=[2,4,8,16,32,64,128,256],
                    resampling='nearest'):
    print(f'Start : {dce.datetime.now()}')
    img = rasterio.open(file,'r+')
    img.build_overviews(dec_factors,dce.getResampleEnum(resampling))
    img.close
    print(f'End : {dce.datetime.now()}')
    return

def datacube_standard_transform(dst_w,dst_s,dst_e,dst_n,pixel_size):
    """Target Aligned Pixel image size calculated using datacube standard 3979
    NW pixel origin (-2600010,3914910)
    Assumes coords and pixel size
    in same EPSG:3979 crs
    """
    origin_pixel_north = 3914910.0
    origin_pixel_west = -2600010.0
    if dst_w < origin_pixel_west:
        msg = ('The destination images westerly edge is too west,'
               f' Please crop to datacube westerly edge {origin_pixel_west}')
        sys.exit(msg)
    if dst_n > origin_pixel_north:
        msg = ('The destination images northerly edge is too north,'
               f' Please crop to datacube northerly edge {origin_pixel_north}')
        sys.exit(msg)


    # Align output extent to datacube origin and TAP
    dst_w = origin_pixel_west
    dst_e = math.ceil(dst_e/pixel_size) * pixel_size
    dst_s = math.floor(dst_s/pixel_size) * pixel_size
    dst_n = origin_pixel_north

    # Output array shape.  Guaranteed to cover output bounds by rasterio.merge.merge
    dst_width = int(round((dst_e-dst_w)/pixel_size))
    dst_height = int(round((dst_n-dst_s)/pixel_size))

    # Calculate the Output transform affine matrix using rasterio.transform.Affine
    dst_transform = (Affine.translation(dst_w, dst_n)
                     * Affine.scale(pixel_size,-pixel_size))

    return dst_transform,dst_width,dst_height

if __name__ == '__main__':
    # parse the argument and pass the grid id to study_area_mosaics

    # script path used to verify location of required files
    script_path,script = os.path.split(sys.argv[0])
    # splilt out the path and file portion of geojson file passed as argument
    # naming convention <grid-id>.geojson
    p,f = os.path.split(sys.argv[1])
    grid_id = f.split('.')[0].upper()
    study_area_mosaics(resample='bilinear',fabdem=True,
                           national_grid=True,
                           grid_id=grid_id,
                           out_dir=script_path)