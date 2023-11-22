# -*- coding: utf-8 -*-
"""
Created on Thu Apr 20 14:53:44 2023

@author: ccrevier
Makes the extraction call for the subzones created by chn and performe a merge 

Steps 
1. load geopackges
1. iterate on geometry inside geopackage
3. make the extract cog call on each zones
4. get list of output files
5. performe the rasterio.merge.merge

Run this code inside HPC to know the evaluate the runtime
Compare with the collection hrdem-wcs and hrdem-lidar
"""
import argparse
import geopandas as gpd
import pathlib
import sys
import rasterio
import os

root = pathlib.Path(__file__).parents[2]
if str(root) not in sys.path:
    sys.path.insert(0,str(root))
    
import ccmeo_datacube.extract as dce    
import ccmeo_datacube.extract_cog as exc
import extract.devtests.extract_mosaic_by_window_wrapper as emw

def load_gpk():
    gdf = gpd.read_file('splitted.gpkg') 
    bbox_coords = gdf.geometry.bounds
    
    bbox_crs = gdf.crs
    return bbox_coords, bbox_crs.to_string()

#Alternative#1, qui appelle le extract_cog pour des extractions simple
def call_extract_cog(collections, folder):
    out_dir = folder + os.sep + collections
    bbox_df, bbox_crs = load_gpk()
    for coord in bbox_df.itertuples():
        bbox = f'{coord.minx},{coord.miny},{coord.maxx},{coord.maxy}'
        params = {'collections':collections,
                  'bbox':bbox,
        			'bbox_crs':bbox_crs,
        			'resolution': 4, 
        			'method': 'bilinear', 
        			'out_crs': 'EPSG:3979', 
        			'out_dir': out_dir, 
        			'suffix': str(coord.Index), 
        			'datetime_filter': None, 
        			'resolution_filter':'1',
        			'overviews': False, 
        			'debug': False, }
        			# 'mosaic': True, 
        			# 'orderby': 'date', 
        			# 'desc': True}
        
        result = exc.extract_cog(**params)
        
    return out_dir

#Alterantive #2 qui appelle le extract_cog avec la mosaic
def call_extract_cog_mosaic(collections, folder):
    out_dir = folder + os.sep + collections
    bbox_df, bbox_crs = load_gpk()
    for coord in bbox_df.itertuples():
        bbox = f'{coord.minx},{coord.miny},{coord.maxx},{coord.maxy}'
        params = {'collections':collections,
                  'bbox':bbox,
        			'bbox_crs':bbox_crs,
        			'resolution': 4, 
        			'method': 'bilinear', 
        			'out_crs': 'EPSG:3979', 
        			'out_dir': out_dir, 
        			'suffix': str(coord.Index), 
        			'datetime_filter': None, 
        			'resolution_filter':'1',
        			'overviews': False, 
        			'debug': False, 
        			 'mosaic': True, 
        			 'orderby': 'date', 
        			 'desc': True}
        
        result = exc.extract_cog(**params)
        
    return out_dir


def call_extract_cog_mosaic_by_window(collections, folder):
    out_dir = folder + os.sep + collections + '_by_window'
    # bbox_df, bbox_crs = load_gpk()
    # for coord in bbox_df.itertuples():
    bbox = '1775625.8392919037,-61408.370247168554,1896969.1451886918,86894.29517093688'#f'{coord.minx},{coord.miny},{coord.maxx},{coord.maxy}'
    #TODO : Le split de la boite devrait etre fait une fois que celle-ci est tap/cap/lcm
    xmin, ymin, xmax, ymax = bbox.split(',')
    xmin, ymin, xmax, ymax = float(xmin), float(ymin), float(xmax), float(ymax)
    dict_bbox = {'bbox1':f'{xmin},{ymin},{xmax-((xmax-xmin)/2)},{ymax-((ymax-ymin)/2)}',
                 'bbox2':f'{xmax-((xmax-xmin)/2)},{ymin},{xmax},{ymax-((ymax-ymin)/2)}',
                 'bbox3':f'{xmin},{ymax-((ymax-ymin)/2)},{xmax-((xmax-xmin)/2)},{ymax}',
                 'bbox4':f'{xmax-((xmax-xmin)/2)},{ymax-((ymax-ymin)/2)},{xmax},{ymax}'}
    # dict_bbox={'bbox1_4':'1800761, -20364,1803678,-14234'}
    bbox_crs = 'EPSG:3979'
    for key, bbox in dict_bbox.items():
        params = {'collections':collections,
                  'bbox':bbox,
        			'bbox_crs':bbox_crs,
        			'resolution': 4, 
        			'method': 'bilinear', 
        			'out_crs': 'EPSG:3979', 
        			'out_dir': out_dir, 
        			 'suffix': key, 
        			'datetime_filter': None, 
        			'resolution_filter':'1',
        			'overviews': False, 
        			'debug': False, 
        			 'mosaic': True, 
        			 'orderby': 'date', 
        			 'desc': True}
        
        result = emw.extract_mosaic_by_window(**params)
        
    return out_dir

@dce.print_time
def make_mosaic(collection, out_dir):
    list_extraction=[]
    for file in os.listdir(out_dir):
        if file.endswith('.tif'):
            list_extraction.append(out_dir+os.sep+file)
            
    opened_tifs = []
    for raster in list_extraction:
        tifs = rasterio.open(raster)
        opened_tifs.append(tifs)
        
    mosaic, out_trans = rasterio.merge.merge(datasets=opened_tifs,
                                      resampling=rasterio.enums.Resampling.bilinear,
                                      method='first')
    # Create the metadata for the mosaic based on one of the tif.
    out_meta = tifs.meta.copy()
    out_meta.update({"driver": "GTiff",
                     "height": mosaic.shape[1],
                     "width": mosaic.shape[2],
                     "transform": out_trans
                     })
    # Write the  mosaic to the project dir
    out_file =  out_dir + os.sep+f'test_mosaic_chn_{collection}.tif'
    print(f'Creation of mosaic : {out_file}')
    with rasterio.open(out_file, "w", **out_meta) as dest:
        dest.write(mosaic)

    return out_file

def _handle_it(collections, extract_type):
    # folder = r'C:\Users\ccrevier\Documents\Datacube\Temp\dev_dc_extract\CHN-issue\MemoryError\2023-04-20'
    folder = '/gpfs/fs5/nrcan/nrcan_geobase/work/transfer/work/datacube/DEV_CC/dev_dc_extract/Julien_big_bbox/MemoryError/by_window'
    if extract_type=='mosaic':
        out_dir = call_extract_cog_mosaic(collections, folder)
        mosaic_path = make_mosaic(collections, out_dir)
    elif extract_type =='by_window':
        out_dir = call_extract_cog_mosaic_by_window(collections, folder)
        mosaic_path = make_mosaic(collections, out_dir)
    elif extract_type == 'cogchip':
        out_dir = call_extract_cog(collections, folder)
        mosaic_path = make_mosaic(collections, out_dir)
        
    return mosaic_path
    

def _handle_cli():
    #Can define the collection
    parser = argparse.ArgumentParser()
    
    parser.add_argument('collections',
                        type=str,
                       )
    parser.add_argument('-type',
                        type=str,
                        default='cogchip',
                       )
    args=parser.parse_args()
    collections = args.collections
    extract_type = args.type
    
    
    _handle_it(collections, extract_type)
    return
    
    
if __name__ == '__main__':
    _handle_cli()
    # _handle_it('hrdem-lidar:dtm')
    

#Example of call with command line (or job file):
# 1. for the by_window
# python /gpfs/fs5/nrcan/nrcan_geobase/work/transfer/work/datacube/DEV_CC/git_files/dc_extract/extract/devtests/test_mosaic_chn_subzones.py hrdem-lidar:dtm -type 'by_window'   
# 2. for the extract_cog mosaic
# python /gpfs/fs5/nrcan/nrcan_geobase/work/transfer/work/datacube/DEV_CC/git_files/dc_extract/extract/devtests/test_mosaic_chn_subzones.py hrdem-lidar:dtm -type 'mosaic'
# 3. for the extract_cog cogchip
# python /gpfs/fs5/nrcan/nrcan_geobase/work/transfer/work/datacube/DEV_CC/git_files/dc_extract/extract/devtests/test_mosaic_chn_subzones.py hrdem-lidar:dtm 